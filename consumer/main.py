import json
import os
import sys
import threading
import time

import grpc
import pika
import psutil

caminho_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if caminho_raiz not in sys.path:
    sys.path.insert(0, caminho_raiz)
caminho_proto = os.path.join(caminho_raiz, 'proto')
if caminho_proto not in sys.path:
    sys.path.insert(0, caminho_proto)

from consumer.state_store import StateStore
from messaging import debug_log
from messaging.config import (
    GRPC_TARGET,
    RABBITMQ_URL,
    QUEUE,
    caminho_dados,
    declarar_topologia,
)
import system_monitor_pb2 as pb2
import system_monitor_pb2_grpc as pb2_grpc


def _processar_mensagem(store, channel, metodo, propriedades, corpo):
    event_id = None
    try:
        evento = json.loads(corpo.decode('utf-8'))
        event_id = evento.get('event_id') if isinstance(evento, dict) else None
    except (UnicodeDecodeError, json.JSONDecodeError) as erro:
        debug_log.registrar(
            'consumer', 'evento_invalido', nivel='ERROR',
            delivery_tag=metodo.delivery_tag, erro=erro,
        )
        print(f'[Consumidor] Evento inválido enviado à DLQ: {erro}')
        channel.basic_nack(delivery_tag=metodo.delivery_tag, requeue=False)
        return
    if not isinstance(evento, dict):
        erro = 'O evento precisa ser um objeto JSON.'
        debug_log.registrar(
            'consumer', 'evento_invalido', nivel='ERROR',
            delivery_tag=metodo.delivery_tag, erro=erro,
        )
        channel.basic_nack(delivery_tag=metodo.delivery_tag, requeue=False)
        return

    session_id_atual = os.getenv('SENTINEL_SESSION_ID')
    session_id_evento = evento.get('session_id')
    if session_id_atual and session_id_evento != session_id_atual:
        channel.basic_ack(delivery_tag=metodo.delivery_tag)
        debug_log.registrar(
            'consumer', 'evento_sessao_antiga_descartado', nivel='WARNING',
            event_id=event_id, event_session_id=session_id_evento,
            active_session_id=session_id_atual,
            delivery_tag=metodo.delivery_tag,
        )
        return

    if evento.get('state_relevant') is False:
        channel.basic_ack(delivery_tag=metodo.delivery_tag)
        debug_log.registrar(
            'consumer', 'evento_analise_ignorado', event_id=event_id,
            delivery_tag=metodo.delivery_tag,
        )
        return

    debug_log.registrar(
        'consumer', 'evento_recebido', event_id=event_id,
        host_id=evento.get('host_id') if isinstance(evento, dict) else None,
        delivery_tag=metodo.delivery_tag,
    )
    resultado = None
    for tentativa in range(1, 4):
        try:
            resultado = store.processar(evento)
            break
        except ValueError as erro:
            debug_log.registrar(
                'consumer', 'evento_rejeitado', nivel='WARNING',
                event_id=event_id, tentativa=tentativa, erro=erro,
            )
            print(f'[Consumidor] Evento rejeitado e enviado à DLQ: {erro}')
            channel.basic_nack(delivery_tag=metodo.delivery_tag, requeue=False)
            return
        except Exception as erro:
            debug_log.registrar(
                'consumer', 'processamento_falhou', nivel='ERROR',
                event_id=event_id, tentativa=tentativa,
                erro_tipo=type(erro).__name__,
                erro=erro if tentativa == 3 else type(erro).__name__,
            )
            if tentativa == 3:
                print(f'[Consumidor] Falha após 3 tentativas; enviado à DLQ: {erro}')
                channel.basic_nack(delivery_tag=metodo.delivery_tag, requeue=False)
                return
            time.sleep(0.5 * (2 ** (tentativa - 1)))

    channel.basic_ack(delivery_tag=metodo.delivery_tag)
    debug_log.registrar(
        'consumer', 'evento_ack', event_id=event_id,
        duplicado=resultado['duplicado'], delivery_tag=metodo.delivery_tag,
    )
    if not resultado['duplicado'] and resultado['transicao']:
        anterior, novo = resultado['transicao']
        debug_log.registrar(
            'consumer', 'transicao_estado', event_id=event_id,
            host_id=resultado['host_id'], previous_state=anterior,
            current_state=novo, alvo=resultado['alvo'],
        )
        print(
            f"[DECISÃO] {resultado['host_id']}: {anterior} -> {novo} "
            f"(alvo atual: {resultado['alvo']})"
        )


def _texto_transicao(alerta):
    metricas = json.loads(alerta['metrics_json'])
    resumo = (
        f"CPU {metricas['cpu_percent']:.0f}% e "
        f"RAM {metricas['ram_percent']:.0f}%"
    )
    gpu = metricas.get('gpu_percent')
    if gpu is not None:
        resumo += f', GPU {gpu:.0f}%'
    temperatura = metricas.get('gpu_temp_celsius')
    if temperatura is not None:
        resumo += f', temperatura da GPU {temperatura:.0f} °C'
    texto = (
        f"{alerta['host_id']}: estado mudou de {alerta['previous_state']} "
        f"para {alerta['current_state']}. {resumo}. "
        f"{alerta['diagnosis']}"
    )
    processo = alerta.get('process')
    if processo:
        texto += (
            f" Possível processo relacionado: {processo['process_name']} "
            f"(PID {processo['pid']}, CPU {processo['cpu_percent']:.1f}%, "
            f"RAM {processo['ram_percent']:.1f}%)."
        )
    return texto


def _loop_notificacoes(store, encerrar):
    canal = grpc.insecure_channel(GRPC_TARGET)
    stub = pb2_grpc.SystemMonitorStub(canal)
    ultimo_erro = None

    try:
        while not encerrar.is_set():
            try:
                pendentes = store.alertas_pendentes()
                for alerta in pendentes:
                    if encerrar.is_set():
                        break
                    pedido = pb2.AlertRequest(
                        log_message=_texto_transicao(alerta),
                        alert_id=alerta['alert_id'],
                        host_id=alerta['host_id'],
                        source='maquina_estados',
                    )
                    processo = alerta.get('process')
                    if processo:
                        pedido.process.CopyFrom(
                            pb2.ProcessRecommendation(**processo)
                        )
                    nivel = {
                        'ATENÇÃO': 'WARNING',
                        'CRÍTICO': 'CRITICAL',
                        'NORMAL': 'INFO',
                    }.get(alerta['current_state'], 'INFO')
                    pedido.level = nivel
                    debug_log.registrar(
                        'consumer', 'sendalert_tentativa', alert_id=alerta['alert_id'],
                        alert_type=alerta['alert_type'], host_id=alerta['host_id'],
                        level=nivel, process=processo,
                    )
                    resposta = stub.SendAlert(pedido, timeout=3.0)
                    if not resposta.accepted:
                        debug_log.registrar(
                            'consumer', 'sendalert_recusado', nivel='WARNING',
                            alert_id=alerta['alert_id'],
                            error=resposta.error or 'Alerta recusado pelo servidor.',
                        )
                        raise RuntimeError(resposta.error or 'Alerta recusado pelo servidor.')
                    store.marcar_alerta_enviado(alerta['alert_id'])
                    debug_log.registrar(
                        'consumer', 'sendalert_ack', alert_id=alerta['alert_id'],
                        alert_type=alerta['alert_type'], accepted=resposta.accepted,
                    )
                    print(
                        f"[Consumidor] SendAlert entregue: "
                        f"{alerta['alert_id']} ({nivel})."
                    )
                    if ultimo_erro is not None:
                        print('[Consumidor] gRPC restabelecido; notificações enviadas.')
                        ultimo_erro = None
            except grpc.RpcError as erro:
                mensagem = erro.details() or str(erro)
                if mensagem != ultimo_erro:
                    debug_log.registrar(
                        'consumer', 'sendalert_indisponivel', nivel='WARNING',
                        erro_tipo=type(erro).__name__, erro=mensagem,
                    )
                    print(f'[Consumidor] SendAlert indisponível; notificação pendente: {mensagem}')
                    ultimo_erro = mensagem
            except Exception as erro:
                mensagem = str(erro)
                if mensagem != ultimo_erro:
                    debug_log.registrar(
                        'consumer', 'notificacao_falhou', nivel='ERROR',
                        erro_tipo=type(erro).__name__, erro=mensagem,
                    )
                    print(f'[Consumidor] Falha ao notificar: {mensagem}')
                    ultimo_erro = mensagem
            encerrar.wait(1.0)
    finally:
        canal.close()


def _pai_ativo(pid):
    return bool(pid and psutil.pid_exists(pid))


def _vigiar_processo_pai(pid_pai):
    while True:
        time.sleep(1.0)
        if not _pai_ativo(pid_pai):
            debug_log.registrar(
                'consumer', 'processo_pai_encerrado', nivel='WARNING',
                parent_pid=pid_pai,
            )
            os._exit(0)


def consumir():
    if sys.stdout is None or sys.stderr is None:
        caminho_log = caminho_dados('SENTINEL_CONSUMER_LOG', 'consumer.log')
        saida_log = open(caminho_log, 'a', encoding='utf-8', buffering=1)
        if sys.stdout is None:
            sys.stdout = saida_log
        if sys.stderr is None:
            sys.stderr = saida_log

    if os.name == 'nt':
        threading.Thread(
            target=_vigiar_processo_pai,
            args=(os.getppid(),),
            name='vigia-processo-pai',
            daemon=True,
        ).start()

    store = StateStore()
    espera = 1.0
    encerrar_notificacoes = threading.Event()
    thread_notificacoes = threading.Thread(
        target=_loop_notificacoes,
        args=(store, encerrar_notificacoes),
        name='grpc-alert-notifier',
        daemon=True,
    )
    thread_notificacoes.start()
    debug_log.registrar('consumer', 'iniciado', state_db=store.caminho)

    while True:
        conexao = None
        try:
            parametros = pika.URLParameters(RABBITMQ_URL)
            parametros.connection_attempts = 1
            parametros.socket_timeout = 3
            parametros.blocked_connection_timeout = 3
            parametros.heartbeat = 30
            conexao = pika.BlockingConnection(parametros)
            canal = conexao.channel()
            declarar_topologia(canal)
            debug_log.registrar('consumer', 'rabbitmq_conectado', queue=QUEUE)
            canal.basic_qos(prefetch_count=1)
            canal.basic_consume(
                queue=QUEUE,
                on_message_callback=lambda ch, metodo, props, corpo: (
                    _processar_mensagem(store, ch, metodo, props, corpo)
                ),
                auto_ack=False,
                exclusive=True,
            )
            print(f'[Consumidor] Aguardando eventos na fila {QUEUE}.')
            espera = 1.0
            canal.start_consuming()
        except KeyboardInterrupt:
            print('\n[Consumidor] Encerrado pelo usuário.')
            debug_log.registrar('consumer', 'interrompido_pelo_usuario')
            break
        except Exception as erro:
            debug_log.registrar(
                'consumer', 'rabbitmq_indisponivel', nivel='WARNING',
                erro_tipo=type(erro).__name__, espera_segundos=espera,
            )
            print(f'[Consumidor] RabbitMQ indisponível: {erro}')
            time.sleep(espera)
            espera = min(30.0, espera * 2)
        finally:
            if conexao is not None and conexao.is_open:
                try:
                    conexao.close()
                except Exception:
                    pass

    encerrar_notificacoes.set()
    thread_notificacoes.join(timeout=4.0)
    debug_log.registrar('consumer', 'encerrado')


if __name__ == '__main__':
    consumir()
