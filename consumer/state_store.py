import json
import math
import os
import sqlite3
from datetime import datetime, timezone

from consumer.state_machine import MaquinaEstados
from messaging import debug_log
from messaging.config import caminho_dados


SCHEMA_VERSION = 1
TIPO_EVENTO = 'system.analysis.completed'


def _limite_ambiente(nome, padrao):
    try:
        valor = float(os.getenv(nome, padrao))
    except (TypeError, ValueError):
        return float(padrao)
    return min(100.0, max(0.1, valor))


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
    session_id = evento.get('session_id')
    if session_id is not None and (
        not isinstance(session_id, str) or not session_id.strip()
    ):
        raise ValueError('session_id precisa ser um texto não vazio.')

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

    processos = evento.get('processes', [])
    if not isinstance(processos, list) or len(processos) > 50:
        raise ValueError('processes precisa ser uma lista com até 50 processos.')
    for processo in processos:
        if not isinstance(processo, dict):
            raise ValueError('Cada processo precisa ser um objeto.')
        pid = processo.get('pid')
        nome = processo.get('name')
        criado = processo.get('create_time')
        if not isinstance(pid, int) or pid <= 0:
            raise ValueError('PID de processo inválido.')
        if not isinstance(nome, str) or not nome.strip():
            raise ValueError('Nome de processo inválido.')
        if not isinstance(criado, (int, float)) or not math.isfinite(criado):
            raise ValueError('create_time de processo inválido.')
        for campo in ('cpu_percent', 'ram_percent'):
            valor = processo.get(campo)
            if (
                not isinstance(valor, (int, float))
                or not math.isfinite(valor)
                or not 0 <= valor <= 100
            ):
                raise ValueError(f'Métrica de processo inválida: {campo}.')
        rss = processo.get('rss_bytes')
        if not isinstance(rss, int) or rss < 0:
            raise ValueError('rss_bytes de processo inválido.')

class StateStore:
    def __init__(self, caminho=None, cpu_limit=None, ram_limit=None):
        self.caminho = caminho or caminho_dados(
            'SENTINEL_STATE_DB', 'state_machine.sqlite3'
        )
        self.cpu_limit = (
            _limite_ambiente('SENTINEL_CPU_LIMIT', 85.0)
            if cpu_limit is None else float(cpu_limit)
        )
        self.ram_limit = (
            _limite_ambiente('SENTINEL_RAM_LIMIT', 85.0)
            if ram_limit is None else float(ram_limit)
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
                        process_json TEXT,
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
            if 'process_json' not in colunas:
                conexao.execute(
                    'ALTER TABLE state_transitions ADD COLUMN process_json TEXT'
                )
            conexao.commit()
        finally:
            conexao.close()

    def processar(self, evento):
        _validar_evento(evento)
        event_id = evento['event_id']
        host_id = evento['host_id']
        metricas = evento['metrics']
        alvo = MaquinaEstados.classificar(
            metricas, cpu_limit=self.cpu_limit, ram_limit=self.ram_limit,
        )
        agora = _agora_utc()
        transicao = None
        estado_anterior = None
        candidato_anterior = None
        contagem_anterior = 0

        conexao = self._conectar()
        try:
            conexao.execute('BEGIN IMMEDIATE')
            ja_processado = conexao.execute(
                'SELECT 1 FROM processed_events WHERE event_id = ?', (event_id,)
            ).fetchone()
            if ja_processado:
                conexao.commit()
                debug_log.registrar(
                    'state_store', 'evento_duplicado', event_id=event_id,
                    host_id=host_id,
                )
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
            estado_anterior = atual
            candidato_anterior = candidato
            contagem_anterior = contagem

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
                processo = None
                cpu_acima = metricas['cpu_percent'] >= self.cpu_limit
                ram_acima = metricas['ram_percent'] >= self.ram_limit
                processos = evento.get('processes', [])
                if (
                    estado_atual == MaquinaEstados.CRITICO
                    and (cpu_acima or ram_acima)
                    and processos
                ):
                    if cpu_acima and ram_acima:
                        pressao = lambda item: max(
                            item['cpu_percent'] / self.cpu_limit,
                            item['ram_percent'] / self.ram_limit,
                        )
                    elif cpu_acima:
                        pressao = lambda item: item['cpu_percent'] / self.cpu_limit
                    else:
                        pressao = lambda item: item['ram_percent'] / self.ram_limit
                    melhor = max(processos, key=pressao)
                    processo = {
                        'recommendation_id': event_id,
                        'pid': melhor['pid'],
                        'process_name': melhor['name'],
                        'create_time': melhor['create_time'],
                        'cpu_percent': melhor['cpu_percent'],
                        'ram_percent': melhor['ram_percent'],
                        'rss_bytes': melhor['rss_bytes'],
                    }
                conexao.execute(
                    '''INSERT INTO state_transitions
                       (event_id, host_id, previous_state, current_state,
                         occurred_at, metrics_json, diagnosis, diagnosis_source,
                         process_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        event_id,
                        host_id,
                        anterior,
                        estado_atual,
                        evento.get('occurred_at', agora),
                        json.dumps(metricas, ensure_ascii=False, allow_nan=False),
                        str(evento.get('diagnosis', '')),
                        str(evento.get('diagnosis_source', '')),
                        (
                            json.dumps(processo, ensure_ascii=False, allow_nan=False)
                            if processo else None
                        ),
                    ),
                )
                if processo:
                    debug_log.registrar(
                        'state_store', 'processo_sugerido_com_alerta_global',
                        event_id=event_id, host_id=host_id, process=processo,
                        cpu_limit=self.cpu_limit, ram_limit=self.ram_limit,
                    )

            conexao.commit()
            debug_log.registrar(
                'state_store', 'evento_processado', event_id=event_id,
                host_id=host_id, metrics=metricas, target_state=alvo,
                previous_state=estado_anterior, current_state=novo_estado,
                previous_candidate=candidato_anterior,
                previous_candidate_count=contagem_anterior,
                candidate_state=novo_candidato,
                candidate_count=nova_contagem,
                diagnosis=evento.get('diagnosis', ''),
                diagnosis_source=evento.get('diagnosis_source', ''),
                occurred_at=evento.get('occurred_at'),
                transition=transicao,
            )
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

    def evento_processado(self, event_id):
        conexao = self._conectar()
        try:
            return conexao.execute(
                'SELECT 1 FROM processed_events WHERE event_id = ?',
                (event_id,),
            ).fetchone() is not None
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

    def alertas_pendentes(self):
        conexao = self._conectar()
        try:
            transicoes = conexao.execute(
                '''SELECT event_id, host_id, previous_state, current_state,
                          occurred_at, metrics_json, diagnosis, diagnosis_source,
                          process_json
                   FROM state_transitions
                   WHERE notification_sent = 0
                   ORDER BY id'''
            ).fetchall()
            resultado = []
            for linha in transicoes:
                alerta = dict(linha)
                process_json = alerta.pop('process_json')
                alerta['alert_id'] = alerta['event_id']
                alerta['alert_type'] = 'state_transition'
                alerta['process'] = json.loads(process_json) if process_json else None
                resultado.append(alerta)
            return resultado
        finally:
            conexao.close()

    def marcar_alerta_enviado(self, alert_id):
        conexao = self._conectar()
        try:
            transicoes = conexao.execute(
                'UPDATE state_transitions SET notification_sent = 1 WHERE event_id = ?',
                (alert_id,),
            ).rowcount
            conexao.commit()
            debug_log.registrar(
                'state_store', 'alerta_marcado_enviado', alert_id=alert_id,
                transition_rows=transicoes,
            )
        finally:
            conexao.close()
