import json
import os
import sys
import threading
import time

import grpc
import pika

caminho_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if caminho_raiz not in sys.path:
    sys.path.insert(0, caminho_raiz)
caminho_proto = os.path.join(caminho_raiz, 'proto')
if caminho_proto not in sys.path:
    sys.path.insert(0, caminho_proto)

from consumer.state_store import StateStore
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
    try:
        evento = json.loads(corpo.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as erro:
        print(f'[Consumidor] Evento inválido enviado à DLQ: {erro}')
        channel.basic_nack(delivery_tag=metodo.delivery_tag, requeue=False)
        return

    resultado = None
    for tentativa in range(1, 4):
        try:
            resultado = store.processar(evento)
            break
        except ValueError as erro:
            print(f'[Consumidor] Evento rejeitado e enviado à DLQ: {erro}')
            channel.basic_nack(delivery_tag=metodo.delivery_tag, requeue=False)
            return
        except Exception as erro:
            if tentativa == 3:
                print(f'[Consumidor] Falha após 3 tentativas; enviado à DLQ: {erro}')
                channel.basic_nack(delivery_tag=metodo.delivery_tag, requeue=False)
                return
            time.sleep(0.5 * (2 ** (tentativa - 1)))

    channel.basic_ack(delivery_tag=metodo.delivery_tag)
    if not resultado['duplicado'] and resultado['transicao']:
        anterior, novo = resultado['transicao']
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
    return (
        f"{alerta['host_id']}: estado mudou de {alerta['previous_state']} "
        f"para {alerta['current_state']}. {resumo}. "
        f"{alerta['diagnosis']}"
    )


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
                    nivel = {
                        'ATENÇÃO': 'WARNING',
                        'CRÍTICO': 'CRITICAL',
                        'NORMAL': 'INFO',
                    }.get(alerta['current_state'], 'INFO')
                    resposta = stub.SendAlert(
                        pb2.AlertRequest(
                            log_message=_texto_transicao(alerta),
                            level=nivel,
                            alert_id=alerta['event_id'],
                            host_id=alerta['host_id'],
                            source='maquina_estados',
                        ),
                        timeout=3.0,
                    )
                    if not resposta.accepted:
                        raise RuntimeError(resposta.error or 'Alerta recusado pelo servidor.')
                    store.marcar_alerta_enviado(alerta['event_id'])
                    print(
                        f"[Consumidor] SendAlert entregue: "
                        f"{alerta['event_id']} ({nivel})."
                    )
                    if ultimo_erro is not None:
                        print('[Consumidor] gRPC restabelecido; notificações enviadas.')
                        ultimo_erro = None
            except grpc.RpcError as erro:
                mensagem = erro.details() or str(erro)
                if mensagem != ultimo_erro:
                    print(f'[Consumidor] SendAlert indisponível; notificação pendente: {mensagem}')
                    ultimo_erro = mensagem
            except Exception as erro:
                mensagem = str(erro)
                if mensagem != ultimo_erro:
                    print(f'[Consumidor] Falha ao notificar: {mensagem}')
                    ultimo_erro = mensagem
            encerrar.wait(1.0)
    finally:
        canal.close()


def consumir():
    if sys.stdout is None or sys.stderr is None:
        caminho_log = caminho_dados('SENTINEL_CONSUMER_LOG', 'consumer.log')
        saida_log = open(caminho_log, 'a', encoding='utf-8', buffering=1)
        if sys.stdout is None:
            sys.stdout = saida_log
        if sys.stderr is None:
            sys.stderr = saida_log

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
            break
        except Exception as erro:
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


if __name__ == '__main__':
    consumir()
