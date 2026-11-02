import json
import math
import sqlite3
from datetime import datetime, timezone

from consumer.state_machine import MaquinaEstados
from messaging.config import caminho_dados


SCHEMA_VERSION = 1
TIPO_EVENTO = 'system.analysis.completed'


def _agora_utc():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace(
        '+00:00', 'Z'
    )


def _validar_evento(evento):
    if not isinstance(evento, dict):
        raise ValueError('O evento precisa ser um objeto JSON.')
    if evento.get('schema_version') != SCHEMA_VERSION:
        raise ValueError('Versão de schema não suportada.')
    if evento.get('event_type') != TIPO_EVENTO:
        raise ValueError('Tipo de evento não suportado.')
    if not evento.get('event_id') or not evento.get('host_id'):
        raise ValueError('event_id e host_id são obrigatórios.')

    metricas = evento.get('metrics')
    if not isinstance(metricas, dict):
        raise ValueError('metrics precisa ser um objeto.')

    for campo in ('cpu_percent', 'ram_percent'):
        valor = metricas.get(campo)
        if not isinstance(valor, (int, float)) or not math.isfinite(valor):
            raise ValueError(f'Métrica inválida: {campo}.')

    for campo in ('cpu_percent', 'ram_percent', 'gpu_percent', 'gpu_mem_percent'):
        valor = metricas.get(campo)
        if valor is not None:
            if not isinstance(valor, (int, float)) or not math.isfinite(valor):
                raise ValueError(f'Métrica inválida: {campo}.')
            if not 0 <= valor <= 100:
                raise ValueError(f'Métrica fora de faixa: {campo}.')

    temperatura = metricas.get('gpu_temp_celsius')
    if temperatura is not None:
        if not isinstance(temperatura, (int, float)) or not math.isfinite(temperatura):
            raise ValueError('Métrica inválida: gpu_temp_celsius.')
        if not -20 <= temperatura <= 200:
            raise ValueError('Temperatura da GPU fora de faixa.')


class StateStore:
    def __init__(self, caminho=None):
        self.caminho = caminho or caminho_dados(
            'SENTINEL_STATE_DB', 'state_machine.sqlite3'
        )
        self._inicializar()

    def _conectar(self):
        conexao = sqlite3.connect(self.caminho, timeout=10)
        conexao.row_factory = sqlite3.Row
        conexao.execute('PRAGMA busy_timeout=10000')
        conexao.execute('PRAGMA synchronous=FULL')
        return conexao

    def _inicializar(self):
        conexao = self._conectar()
        try:
            conexao.execute('PRAGMA journal_mode=WAL')
            conexao.executescript(
                '''CREATE TABLE IF NOT EXISTS processed_events (
                       event_id TEXT PRIMARY KEY,
                       host_id TEXT NOT NULL,
                       processed_at TEXT NOT NULL
                   );
                   CREATE TABLE IF NOT EXISTS machine_states (
                       host_id TEXT PRIMARY KEY,
                       current_state TEXT NOT NULL,
                       candidate_state TEXT,
                       candidate_count INTEGER NOT NULL,
                       updated_at TEXT NOT NULL
                   );
                   CREATE TABLE IF NOT EXISTS state_transitions (
                       id INTEGER PRIMARY KEY AUTOINCREMENT,
                       event_id TEXT NOT NULL UNIQUE,
                       host_id TEXT NOT NULL,
                       previous_state TEXT NOT NULL,
                       current_state TEXT NOT NULL,
                       occurred_at TEXT NOT NULL,
                       metrics_json TEXT NOT NULL,
                       diagnosis TEXT NOT NULL,
                       diagnosis_source TEXT NOT NULL,
                       notification_sent INTEGER NOT NULL DEFAULT 0
                   );
                   CREATE INDEX IF NOT EXISTS idx_transitions_host_time
                       ON state_transitions(host_id, occurred_at);'''
            )
            colunas = {
                linha['name']
                for linha in conexao.execute('PRAGMA table_info(state_transitions)')
            }
            if 'notification_sent' not in colunas:
                conexao.execute(
                    'ALTER TABLE state_transitions ADD COLUMN '
                    'notification_sent INTEGER NOT NULL DEFAULT 0'
                )
                conexao.execute(
                    'UPDATE state_transitions SET notification_sent = 1'
                )
            conexao.commit()
        finally:
            conexao.close()

    def processar(self, evento):
        _validar_evento(evento)
        event_id = evento['event_id']
        host_id = evento['host_id']
        metricas = evento['metrics']
        alvo = MaquinaEstados.classificar(metricas)
        agora = _agora_utc()

        conexao = self._conectar()
        try:
            conexao.execute('BEGIN IMMEDIATE')
            ja_processado = conexao.execute(
                'SELECT 1 FROM processed_events WHERE event_id = ?', (event_id,)
            ).fetchone()
            if ja_processado:
                conexao.commit()
                return {'duplicado': True, 'host_id': host_id, 'transicao': None}

            estado = conexao.execute(
                'SELECT current_state, candidate_state, candidate_count '
                'FROM machine_states WHERE host_id = ?',
                (host_id,),
            ).fetchone()
            if estado:
                atual = estado['current_state']
                candidato = estado['candidate_state']
                contagem = estado['candidate_count']
            else:
                atual = MaquinaEstados.NORMAL
                candidato = None
                contagem = 0

            novo_estado, novo_candidato, nova_contagem, transicao = (
                MaquinaEstados.avancar(atual, candidato, contagem, alvo)
            )

            conexao.execute(
                '''INSERT INTO processed_events(event_id, host_id, processed_at)
                   VALUES (?, ?, ?)''',
                (event_id, host_id, agora),
            )
            conexao.execute(
                '''INSERT INTO machine_states
                   (host_id, current_state, candidate_state, candidate_count, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(host_id) DO UPDATE SET
                       current_state = excluded.current_state,
                       candidate_state = excluded.candidate_state,
                       candidate_count = excluded.candidate_count,
                       updated_at = excluded.updated_at''',
                (host_id, novo_estado, novo_candidato, nova_contagem, agora),
            )

            if transicao:
                anterior, estado_atual = transicao
                conexao.execute(
                    '''INSERT INTO state_transitions
                       (event_id, host_id, previous_state, current_state,
                        occurred_at, metrics_json, diagnosis, diagnosis_source)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        event_id,
                        host_id,
                        anterior,
                        estado_atual,
                        evento.get('occurred_at', agora),
                        json.dumps(metricas, ensure_ascii=False, allow_nan=False),
                        str(evento.get('diagnosis', '')),
                        str(evento.get('diagnosis_source', '')),
                    ),
                )

            conexao.commit()
            return {
                'duplicado': False,
                'host_id': host_id,
                'estado': novo_estado,
                'alvo': alvo,
                'transicao': transicao,
            }
        except Exception:
            conexao.rollback()
            raise
        finally:
            conexao.close()

    def estado_atual(self, host_id):
        conexao = self._conectar()
        try:
            linha = conexao.execute(
                'SELECT current_state FROM machine_states WHERE host_id = ?',
                (host_id,),
            ).fetchone()
            return linha['current_state'] if linha else MaquinaEstados.NORMAL
        finally:
            conexao.close()

    def listar_transicoes(self, limite=100):
        conexao = self._conectar()
        try:
            linhas = conexao.execute(
                '''SELECT event_id, host_id, previous_state, current_state,
                          occurred_at, metrics_json, diagnosis, diagnosis_source
                   FROM state_transitions ORDER BY id DESC LIMIT ?''',
                (limite,),
            ).fetchall()
            return [dict(linha) for linha in linhas]
        finally:
            conexao.close()

    def alertas_pendentes(self, limite=100):
        conexao = self._conectar()
        try:
            linhas = conexao.execute(
                '''SELECT event_id, host_id, previous_state, current_state,
                          occurred_at, metrics_json, diagnosis, diagnosis_source
                   FROM state_transitions
                   WHERE notification_sent = 0
                   ORDER BY id LIMIT ?''',
                (limite,),
            ).fetchall()
            return [dict(linha) for linha in linhas]
        finally:
            conexao.close()

    def marcar_alerta_enviado(self, alert_id):
        conexao = self._conectar()
        try:
            conexao.execute(
                'UPDATE state_transitions SET notification_sent = 1 WHERE event_id = ?',
                (alert_id,),
            )
            conexao.commit()
        finally:
            conexao.close()
