import json
import os
import threading
import time

import pika

from messaging.config import (
    EXCHANGE,
    RABBITMQ_URL,
    ROUTING_KEY,
    declarar_topologia,
)
from messaging import debug_log
from server.outbox import OutboxSQLite


class PublicadorRabbitMQ:
    def __init__(self, outbox=None, url=None, ao_log=None):
        self.outbox = outbox or OutboxSQLite()
        self.url = url or RABBITMQ_URL
        self.ao_log = ao_log
        self._acordar = threading.Event()
        self._lock = threading.Lock()
        self._thread = None
        self._fechando = False
        self._limite_fechamento = None
        self._ultimo_erro = None

    def iniciar(self):
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._fechando = False
            self._limite_fechamento = None
            self._thread = threading.Thread(
                target=self._loop, name='rabbitmq-outbox', daemon=True
            )
            self._thread.start()

    def publicar(self, evento):
        payload = json.dumps(
            evento, ensure_ascii=False, separators=(',', ':'), allow_nan=False
        )
        self.outbox.adicionar(
            evento['event_id'], ROUTING_KEY, payload
        )
        debug_log.registrar(
            'rabbitmq', 'outbox_enfileirado',
            event_id=evento['event_id'], event_type=evento.get('event_type'),
        )
        self.iniciar()
        self._acordar.set()

    def fechar(self, timeout=3.0):
        with self._lock:
            thread = self._thread
            if thread is None or not thread.is_alive():
                return
            self._fechando = True
            self._limite_fechamento = time.monotonic() + max(0.0, timeout)
            self._acordar.set()
        thread.join(timeout=max(0.0, timeout) + 0.5)
        pendentes = self.outbox.quantidade_pendente()
        if pendentes:
            debug_log.registrar(
                'rabbitmq', 'encerramento_com_pendencias', nivel='WARNING',
                pending_count=pendentes,
            )
            if os.getenv('SENTINEL_SESSION_ID'):
                mensagem = (
                    f'[RabbitMQ] {pendentes} evento(s) da sessão atual '
                    'permanecem no outbox; não serão reenviados fora da sessão.'
                )
            else:
                mensagem = (
                    f'[RabbitMQ] {pendentes} evento(s) permanecem no outbox '
                    'para envio na próxima execução.'
                )
            self._log(mensagem)

    def _log(self, mensagem):
        if self.ao_log:
            self.ao_log(mensagem)
        else:
            print(mensagem)

    def _conectar(self):
        parametros = pika.URLParameters(self.url)
        parametros.connection_attempts = 1
        parametros.socket_timeout = 3
        parametros.blocked_connection_timeout = 3
        parametros.heartbeat = 30
        conexao = pika.BlockingConnection(parametros)
        try:
            canal = conexao.channel()
            declarar_topologia(canal)
            canal.confirm_delivery()
            debug_log.registrar('rabbitmq', 'conexao_estabelecida')
            return conexao, canal
        except Exception:
            conexao.close()
            raise

    def _esperar_retentativa(self, tentativas):
        atraso = min(30.0, 2.0 ** min(tentativas, 5))
        if self._fechando and self._limite_fechamento is not None:
            restante = self._limite_fechamento - time.monotonic()
            atraso = min(atraso, max(0.0, restante))
        self._acordar.clear()
        if atraso > 0:
            self._acordar.wait(atraso)

    def _registrar_erro(self, erro):
        mensagem = str(erro)
        if mensagem != self._ultimo_erro:
            debug_log.registrar(
                'rabbitmq', 'conexao_indisponivel', nivel='WARNING', erro=mensagem,
            )
            self._log(
                f'[RabbitMQ] Broker indisponível; eventos ficam no outbox: '
                f'{mensagem}'
            )
            self._ultimo_erro = mensagem

    def _loop(self):
        conexao = None
        canal = None
        tentativas = 0

        try:
            while True:
                if (
                    self._fechando
                    and self._limite_fechamento is not None
                    and time.monotonic() >= self._limite_fechamento
                ):
                    break

                try:
                    item = self.outbox.proximo()
                except Exception as erro:
                    tentativas += 1
                    self._registrar_erro(erro)
                    self._esperar_retentativa(tentativas)
                    continue
                if item is None:
                    if self._fechando:
                        break
                    self._acordar.wait(1.0)
                    self._acordar.clear()
                    continue

                try:
                    if conexao is None or not conexao.is_open:
                        conexao, canal = self._conectar()

                    confirmado = canal.basic_publish(
                        exchange=EXCHANGE,
                        routing_key=item['routing_key'],
                        body=item['payload'].encode('utf-8'),
                        properties=pika.BasicProperties(
                            content_type='application/json',
                            content_encoding='utf-8',
                            delivery_mode=pika.DeliveryMode.Persistent,
                            message_id=item['event_id'],
                            type='system.analysis.completed',
                            headers={'schema_version': 1},
                        ),
                        mandatory=True,
                    )
                    if confirmado is False:
                        raise RuntimeError('RabbitMQ não confirmou a publicação')

                    self.outbox.marcar_enviado(item['id'])
                    debug_log.registrar(
                        'rabbitmq', 'evento_confirmado',
                        event_id=item['event_id'], attempts=item['attempts'],
                    )
                    if self._ultimo_erro is not None:
                        self._log('[RabbitMQ] Conexão restabelecida; outbox drenando.')
                        self._ultimo_erro = None
                    tentativas = 0
                except Exception as erro:
                    tentativas += 1
                    self._registrar_erro(erro)
                    try:
                        self.outbox.registrar_falha(item['id'], erro)
                        debug_log.registrar(
                            'rabbitmq', 'publicacao_retentativa', nivel='WARNING',
                            event_id=item['event_id'], attempts=item['attempts'] + 1,
                            erro_tipo=type(erro).__name__, erro=erro,
                        )
                    except Exception as erro_db:
                        debug_log.registrar(
                            'rabbitmq', 'falha_ao_atualizar_outbox', nivel='ERROR',
                            event_id=item['event_id'],
                            erro_tipo=type(erro_db).__name__, erro=erro_db,
                        )
                        self._registrar_erro(erro_db)
                    if conexao is not None:
                        try:
                            conexao.close()
                        except Exception:
                            pass
                    conexao = None
                    canal = None
                    self._esperar_retentativa(tentativas)
        finally:
            if conexao is not None and conexao.is_open:
                try:
                    conexao.close()
                except Exception:
                    pass
