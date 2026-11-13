import json
import math
import os
import re
import sys
import threading
from datetime import datetime, timezone


_LOCK = threading.RLock()
_ESTATISTICAS = {}
_ULTIMO_DIAGNOSTICO = {}
_EVENTOS = {
    ('app', 'sessao_iniciada'),
    ('app', 'sessao_encerrada'),
    ('app', 'alerta_recebido'),
    ('app', 'popup_exibido'),
    ('app', 'popup_fechado'),
    ('app', 'alerta_duplicado_na_interface'),
    ('grpc', 'sendalert_aceito'),
    ('grpc', 'sendalert_rejeitado'),
    ('grpc', 'sendalert_entrega_falhou'),
    ('rabbitmq', 'conexao_estabelecida'),
    ('rabbitmq', 'conexao_indisponivel'),
    ('rabbitmq', 'publicacao_retentativa'),
    ('rabbitmq', 'falha_ao_atualizar_outbox'),
    ('rabbitmq', 'evento_outbox_falhou'),
    ('rabbitmq', 'encerramento_com_pendencias'),
    ('consumer', 'evento_sessao_antiga_descartado'),
    ('consumer', 'transicao_estado'),
    ('consumer', 'sendalert_tentativa'),
    ('consumer', 'sendalert_ack'),
    ('consumer', 'sendalert_recusado'),
    ('consumer', 'sendalert_indisponivel'),
    ('consumer', 'notificacao_falhou'),
    ('consumer', 'evento_rejeitado'),
    ('consumer', 'processamento_falhou'),
    ('consumer', 'rabbitmq_indisponivel'),
    ('state_store', 'alerta_marcado_enviado'),
    ('state_store', 'processo_sugerido_com_alerta_global'),
    ('process_action', 'encerramento_cancelado'),
    ('process_action', 'encerramento_confirmado'),
    ('process_action', 'encerramento_iniciado'),
    ('process_action', 'decisao_registrada'),
    ('process_action', 'resultado_sem_interface'),
}
_METRICAS = (
    'cpu_percent',
    'ram_percent',
    'gpu_percent',
    'gpu_mem_percent',
    'gpu_temp_celsius',
)


def _paths(session_id):
    from messaging.debug_log import caminho_log

    if not re.fullmatch(r'[A-Za-z0-9_-]+', str(session_id or '')):
        raise ValueError('ID de sessão inválido.')
    pasta = os.path.join(os.path.dirname(caminho_log()), 'sessoes')
    os.makedirs(pasta, exist_ok=True)
    return pasta, os.path.join(pasta, f'{session_id}.md')


def _append(caminho, texto):
    from messaging.debug_log import _escrever

    _escrever(caminho, texto)


def _agora():
    local = datetime.now().astimezone()
    utc = local.astimezone(timezone.utc)
    return (
        local.isoformat(timespec='milliseconds'),
        utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
    )


def _marcar_interrompida(session_id):
    _, caminho = _paths(session_id)
    if not os.path.exists(caminho):
        return
    with open(caminho, 'rb') as arquivo:
        arquivo.seek(0, os.SEEK_END)
        tamanho = arquivo.tell()
        arquivo.seek(max(0, tamanho - 4096))
        final = arquivo.read().decode('utf-8', errors='replace')
    if '**Estado:** encerrada normalmente' in final:
        return
    local, utc = _agora()
    _append(
        caminho,
        '\n## Encerramento detectado na próxima execução\n'
        f'- **Estado:** interrompida sem registro de encerramento normal\n'
        f'- **Detectada em:** {local} (UTC {utc})\n',
    )


def iniciar_sessao(session_id):
    pasta, caminho = _paths(session_id)
    marcador = os.path.join(pasta, '.sessao_ativa')
    with _LOCK:
        anterior = None
        try:
            with open(marcador, encoding='utf-8') as arquivo:
                registro = json.load(arquivo)
                anterior = registro.get('session_id')
        except (OSError, ValueError, TypeError):
            anterior = None
        if anterior and anterior != session_id:
            _marcar_interrompida(anterior)

        local, utc = _agora()
        _append(
            caminho,
            f'# Relatório da sessão `{session_id}`\n\n'
            f'- **Início local:** {local}\n'
            f'- **Início UTC:** {utc}\n'
            '- **Estado:** em andamento\n'
            '- Métricas detalhadas por amostra e eventos completos: `../debug.txt`\n\n'
            '## Resumo de telemetria\n'
            'Será calculado ao encerrar normalmente; as amostras abaixo também '
            'permanecem registradas se houver interrupção.\n\n'
            '## Linha do tempo\n',
        )
        temporario = f'{marcador}.{os.getpid()}.tmp'
        with open(temporario, 'w', encoding='utf-8') as arquivo:
            json.dump({'session_id': session_id, 'pid': os.getpid()}, arquivo)
        os.replace(temporario, marcador)
        _ESTATISTICAS[session_id] = {
            'samples': 0,
            'metrics': {
                nome: {'count': 0, 'sum': 0.0, 'min': None, 'max': None}
                for nome in _METRICAS
            },
        }
        _ULTIMO_DIAGNOSTICO.pop(session_id, None)


def _registrar_linha(caminho, local, utc, componente, evento, campos):
    campos = {
        chave: valor
        for chave, valor in campos.items()
        if chave not in {'pid', 'thread', 'session_id'}
    }
    dados = json.dumps(
        campos,
        ensure_ascii=False,
        separators=(',', ':'),
        allow_nan=False,
        default=str,
    ).replace('`', '\\`')
    _append(
        caminho,
        f'- **{local}** (UTC {utc}) — **{componente}.{evento}** — `{dados}`\n',
    )


def registrar_evento(session_id, local, utc, nivel, componente, evento, campos):
    if not session_id:
        return
    _, caminho = _paths(session_id)
    with _LOCK:
        if componente == 'coletor' and evento == 'amostra':
            estatisticas = _ESTATISTICAS.setdefault(
                session_id,
                {
                    'samples': 0,
                    'metrics': {
                        nome: {'count': 0, 'sum': 0.0, 'min': None, 'max': None}
                        for nome in _METRICAS
                    },
                },
            )
            estatisticas['samples'] += 1
            for nome in _METRICAS:
                valor = campos.get(nome)
                if isinstance(valor, (int, float)) and math.isfinite(valor):
                    metrica = estatisticas['metrics'][nome]
                    metrica['count'] += 1
                    metrica['sum'] += float(valor)
                    metrica['min'] = (
                        float(valor) if metrica['min'] is None
                        else min(metrica['min'], float(valor))
                    )
                    metrica['max'] = (
                        float(valor) if metrica['max'] is None
                        else max(metrica['max'], float(valor))
                    )
            valores = []
            for nome, rotulo, unidade in (
                ('cpu_percent', 'CPU', '%'),
                ('ram_percent', 'RAM', '%'),
                ('gpu_percent', 'GPU', '%'),
                ('gpu_mem_percent', 'VRAM', '%'),
                ('gpu_temp_celsius', 'Temp. GPU', '°C'),
            ):
                valor = campos.get(nome)
                valores.append(
                    f'{rotulo} {valor:.1f}{unidade}'
                    if isinstance(valor, (int, float)) else f'{rotulo} —'
                )
            _append(
                caminho,
                f'- **{local}** (UTC {utc}) — Amostra: '
                f'{" · ".join(valores)}\n',
            )
            return

        if componente == 'diagnostico' and evento == 'resultado':
            diagnostico = str(campos.get('diagnosis') or '')
            assinatura = (campos.get('origem'), diagnostico)
            if _ULTIMO_DIAGNOSTICO.get(session_id) == assinatura:
                return
            _ULTIMO_DIAGNOSTICO[session_id] = assinatura
            campos = {
                'origem': campos.get('origem'),
                'diagnosis': diagnostico[:600],
            }
            _registrar_linha(caminho, local, utc, componente, evento, campos)
            return

        if (componente, evento) in _EVENTOS or nivel.upper() in {'WARNING', 'ERROR'}:
            _registrar_linha(caminho, local, utc, componente, evento, campos)


def finalizar_sessao(session_id, motivo):
    if not session_id:
        return
    pasta, caminho = _paths(session_id)
    marcador = os.path.join(pasta, '.sessao_ativa')
    local, utc = _agora()
    with _LOCK:
        _append(
            caminho,
            '\n## Encerramento\n'
            '- **Estado:** encerrada normalmente\n'
            f'- **Motivo:** {motivo}\n'
            f'- **Fim local:** {local}\n'
            f'- **Fim UTC:** {utc}\n',
        )
        estatisticas = _ESTATISTICAS.pop(session_id, None)
        _ULTIMO_DIAGNOSTICO.pop(session_id, None)
        if estatisticas and estatisticas['samples']:
            _append(
                caminho,
                f'- **Amostras coletadas:** {estatisticas["samples"]}\n\n'
                '| Métrica | Mínimo | Média | Máximo |\n'
                '|---|---:|---:|---:|\n',
            )
            for nome, rotulo, unidade in (
                ('cpu_percent', 'CPU', '%'),
                ('ram_percent', 'RAM', '%'),
                ('gpu_percent', 'GPU', '%'),
                ('gpu_mem_percent', 'VRAM', '%'),
                ('gpu_temp_celsius', 'Temperatura GPU', '°C'),
            ):
                metrica = estatisticas['metrics'][nome]
                if not metrica['count']:
                    continue
                media = metrica['sum'] / metrica['count']
                _append(
                    caminho,
                    f'| {rotulo} | {metrica["min"]:.1f}{unidade} | '
                    f'{media:.1f}{unidade} | {metrica["max"]:.1f}{unidade} |\n',
                )
        try:
            with open(marcador, encoding='utf-8') as arquivo:
                registro = json.load(arquivo)
            if registro.get('session_id') == session_id:
                os.remove(marcador)
        except (OSError, ValueError, TypeError):
            pass
