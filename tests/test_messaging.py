import json
import os
import queue
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import consumer.main as consumer_main
from consumer.main import _processar_mensagem
from consumer.state_machine import MaquinaEstados
from consumer.state_store import StateStore
from app import rabbitmq_service
from app.historico import HistoricoSessao
from app.main import App
from messaging.eventos import criar_evento
from server.outbox import OutboxSQLite
from server.rabbit_publisher import PublicadorRabbitMQ
from server.main import ServidorMonitorizacao
import system_monitor_pb2 as pb


def _evento(event_id, cpu=20, ram=30, gpu=20, vram=10, temperatura=50):
    return {
        'schema_version': 1,
        'event_type': 'system.analysis.completed',
        'event_id': event_id,
        'host_id': 'host-test',
        'occurred_at': '2026-11-02T12:00:00Z',
        'sample_timestamp': '12:00:00',
        'metrics': {
            'cpu_percent': cpu,
            'ram_percent': ram,
            'gpu_percent': gpu,
            'gpu_mem_percent': vram,
            'gpu_temp_celsius': temperatura,
        },
        'diagnosis': 'diagnóstico de teste',
        'diagnosis_source': 'regra',
    }


class FakeTelemetry:
    cpu_percent = 40.0
    ram_percent = 60.0
    timestamp = '12:00:00'
    gpu_percent = 25.0
    gpu_mem_percent = 30.0
    temp_celsius = 62.0

    def HasField(self, field):
        return field != 'gpu_mem_percent'


class StateStoreTests(unittest.TestCase):
    def test_escalada_recuperacao_e_idempotencia(self):
        with tempfile.TemporaryDirectory() as pasta:
            caminho = os.path.join(pasta, 'states.sqlite3')
            store = StateStore(caminho)

            self.assertEqual(
                store.processar(_evento('normal'))['estado'], MaquinaEstados.NORMAL
            )

            resultados = [
                store.processar(_evento(f'atencao-{i}', cpu=75))
                for i in range(3)
            ]
            self.assertEqual(resultados[-1]['transicao'], ('NORMAL', 'ATENÇÃO'))

            resultados = [
                store.processar(_evento(f'critico-{i}', temperatura=92))
                for i in range(3)
            ]
            self.assertEqual(resultados[-1]['transicao'], ('ATENÇÃO', 'CRÍTICO'))

            recuperacao = [
                store.processar(_evento(f'recuperacao-{i}')) for i in range(5)
            ]
            self.assertEqual(
                recuperacao[-1]['transicao'], ('CRÍTICO', 'NORMAL')
            )
            self.assertTrue(store.processar(_evento('recuperacao-4'))['duplicado'])
            pendentes = store.alertas_pendentes()
            self.assertEqual(len(pendentes), 3)
            store.marcar_alerta_enviado(pendentes[0]['event_id'])
            self.assertEqual(len(store.alertas_pendentes()), 2)

            store_reaberto = StateStore(caminho)
            self.assertEqual(store_reaberto.estado_atual('host-test'), 'NORMAL')
            self.assertEqual(len(store_reaberto.listar_transicoes()), 3)

    def test_classifica_limites_e_sensor_ausente(self):
        self.assertEqual(
            MaquinaEstados.classificar({
                'cpu_percent': 70,
                'ram_percent': 20,
                'gpu_percent': None,
                'gpu_mem_percent': None,
                'gpu_temp_celsius': None,
            }),
            MaquinaEstados.ATENCAO,
        )
        self.assertEqual(
            MaquinaEstados.classificar({
                'cpu_percent': 20,
                'ram_percent': 20,
                'gpu_percent': None,
                'gpu_mem_percent': None,
                'gpu_temp_celsius': 90,
            }),
            MaquinaEstados.CRITICO,
        )


class EventTests(unittest.TestCase):
    def test_evento_inclui_origem_e_metricas_opcionais(self):
        evento = criar_evento(FakeTelemetry(), 'Sistema estável', 'regra')
        self.assertEqual(evento['event_type'], 'system.analysis.completed')
        self.assertEqual(evento['diagnosis_source'], 'regra')
        self.assertEqual(evento['metrics']['gpu_temp_celsius'], 62.0)
        self.assertIsNone(evento['metrics']['gpu_mem_percent'])
        self.assertTrue(evento['event_id'])

    def test_send_alert_entrega_notificacao_e_deduplica_id(self):
        class PublicadorFake:
            pass

        recebidos = []
        servidor = ServidorMonitorizacao(
            publicador=PublicadorFake(),
            ao_alerta=recebidos.append,
        )
        pedido = pb.AlertRequest(
            log_message='Temperatura crítica da GPU',
            level='CRÍTICO',
            alert_id='alerta-1',
            host_id='host-test',
            source='maquina_estados',
        )

        resposta = servidor.SendAlert(pedido, None)
        duplicada = servidor.SendAlert(pedido, None)

        self.assertTrue(resposta.accepted)
        self.assertEqual(resposta.alert_id, 'alerta-1')
        self.assertTrue(duplicada.accepted)
        self.assertEqual(len(recebidos), 1)
        self.assertEqual(recebidos[0]['level'], 'CRITICAL')
        self.assertEqual(recebidos[0]['host_id'], 'host-test')


class SessionHistoryTests(unittest.TestCase):
    def test_filtros_paginacao_e_historico_apenas_em_memoria(self):
        historico = HistoricoSessao()
        historico.registrar('Diagnósticos', 'CPU em nível crítico', nivel='WARNING')
        historico.registrar('Decisões', 'NORMAL para ATENÇÃO', nivel='WARNING')
        historico.registrar('Notificações', 'Temperatura GPU crítica', nivel='CRITICAL')

        pagina, total = historico.buscar(categoria='Decisões', limite=10)
        self.assertEqual(total, 1)
        self.assertEqual(pagina[0]['mensagem'], 'NORMAL para ATENÇÃO')

        pagina, total = historico.buscar(busca='GPU', limite=1)
        self.assertEqual(total, 1)
        self.assertEqual(pagina[0]['categoria'], 'Notificações')

        outra_sessao = HistoricoSessao()
        self.assertEqual(outra_sessao.buscar()[1], 0)
        historico.fechar()
        outra_sessao.fechar()


class RabbitMQServiceTests(unittest.TestCase):
    def test_inicia_servico_parado_e_aguarda_porta(self):
        with patch.object(rabbitmq_service.sys, 'platform', 'win32'), \
                patch.object(
                    rabbitmq_service, '_consultar_servico',
                    side_effect=[(1, None), (4, None)],
                ), patch.object(
                    rabbitmq_service, '_iniciar_servico', return_value=(True, None)
                ) as iniciar, patch.object(
                    rabbitmq_service, '_porta_aberta', return_value=True
                ):
            ativo, detalhe = rabbitmq_service.garantir_rabbitmq(timeout=1)

        self.assertTrue(ativo)
        self.assertIn('Serviço RabbitMQ ativo', detalhe)
        iniciar.assert_called_once_with('RabbitMQ')

    def test_broker_remoto_nao_tenta_iniciar_servico_windows(self):
        with patch.object(rabbitmq_service.sys, 'platform', 'win32'), \
                patch.object(rabbitmq_service, '_aguardar_endpoint', return_value=True), \
                patch.object(rabbitmq_service, '_consultar_servico') as consultar:
            ativo, detalhe = rabbitmq_service.garantir_rabbitmq(
                'amqp://user:pass@broker.example:5672/%2F', timeout=1
            )

        self.assertTrue(ativo)
        self.assertIn('broker.example:5672', detalhe)
        consultar.assert_not_called()

    def test_para_servico_local_ativo(self):
        with patch.object(rabbitmq_service.sys, 'platform', 'win32'), \
                patch.object(
                    rabbitmq_service, '_consultar_servico',
                    side_effect=[(4, None), (1, None)],
                ), patch.object(
                    rabbitmq_service.subprocess, 'run',
                    return_value=SimpleNamespace(returncode=0, stdout='', stderr=''),
                ) as executar:
            parado, detalhe = rabbitmq_service.parar_rabbitmq(timeout=1)

        self.assertTrue(parado)
        self.assertIn('RabbitMQ parado', detalhe)
        self.assertEqual(executar.call_args.args[0], ['sc.exe', 'stop', 'RabbitMQ'])

    def test_nao_para_broker_remoto(self):
        with patch.object(rabbitmq_service.sys, 'platform', 'win32'), \
                patch.object(rabbitmq_service.subprocess, 'run') as executar:
            parado, detalhe = rabbitmq_service.parar_rabbitmq(
                'amqp://user:pass@broker.example:5672/%2F'
            )

        self.assertFalse(parado)
        self.assertIn('remoto', detalhe)
        executar.assert_not_called()


class AppLifecycleTests(unittest.TestCase):
    def test_parar_drena_servidor_antes_de_parar_rabbitmq(self):
        ordem = []

        class ColetorFake:
            def parar(self):
                ordem.append('coletor')

        class BotaoFake:
            def set_enabled(self, enabled):
                return None

        app = App.__new__(App)
        app.coletor = ColetorFake()
        app.servidor = object()
        app._rabbitmq_lock = threading.Lock()
        app._parar_rabbitmq_ao_encerrar = True
        app.processo_consumer = None
        app.pill_servidor = object()
        app._set_pill = lambda *args: None
        app._log = lambda mensagem: None
        app.btn_iniciar = BotaoFake()
        app.btn_parar = BotaoFake()

        def parar_grpc(servidor, grace):
            ordem.append('grpc-e-outbox')

        def parar_broker():
            ordem.append('rabbitmq')
            return True, 'parado'

        with patch('app.main.parar_servidor', side_effect=parar_grpc), \
                patch('app.main.parar_rabbitmq', side_effect=parar_broker):
            app._parar()

        self.assertEqual(ordem, ['coletor', 'grpc-e-outbox', 'rabbitmq'])
        self.assertFalse(app._parar_rabbitmq_ao_encerrar)

    def test_app_inicia_e_para_processo_consumidor(self):
        criados = []

        class ProcessoFake:
            pid = 1234
            stdout = None

            def __init__(self, **opcoes):
                self.opcoes = opcoes
                self.returncode = None
                criados.append(self)

            def poll(self):
                return self.returncode

            def terminate(self):
                self.returncode = 0

            def wait(self, timeout=None):
                return self.returncode

        app = App.__new__(App)
        app.processo_consumer = None
        app._log = lambda mensagem: None
        with patch('app.main.subprocess.Popen', ProcessoFake):
            app._iniciar_consumidor(50123)
            self.assertEqual(app.processo_consumer.pid, 1234)
            self.assertEqual(
                criados[0].opcoes['env']['GRPC_TARGET'], '127.0.0.1:50123'
            )
            app._parar_consumidor()

        self.assertEqual(criados[0].returncode, 0)
        self.assertIsNone(app.processo_consumer)

    def test_sino_recebe_alerta_e_nao_duplica_id(self):
        class SinoFake:
            def __init__(self):
                self.quantidade = 0

            def set_quantidade(self, quantidade):
                self.quantidade = quantidade

        class RootFake:
            def after(self, intervalo, callback):
                return None

        app = App.__new__(App)
        app.fila_alertas = queue.Queue()
        app.notificacoes = []
        app._alertas_ids = set()
        app.sino_notificacoes = SinoFake()
        app.historico_sessao = HistoricoSessao()
        app._janela_notificacoes = None
        app.root = RootFake()
        toasts = []
        app._mostrar_toast = toasts.append
        app._renderizar_notificacoes = lambda: None
        alerta = {
            'alert_id': 'alerta-ui-1',
            'level': 'CRITICAL',
            'host_id': 'host-test',
            'source': 'maquina_estados',
            'message': 'Temperatura crítica da GPU',
            'received_at': '2026-11-02T12:00:00Z',
        }
        app._receber_alerta(alerta)
        app._receber_alerta(alerta)
        app._atualizar_alertas()

        self.assertEqual(len(app.notificacoes), 1)
        self.assertEqual(app.sino_notificacoes.quantidade, 1)
        self.assertEqual(len(toasts), 1)
        app.historico_sessao.fechar()


class ConsumerTests(unittest.TestCase):
    def test_ack_apos_persistir_e_nack_de_evento_invalido(self):
        class Canal:
            def __init__(self):
                self.acks = []
                self.nacks = []

            def basic_ack(self, delivery_tag):
                self.acks.append(delivery_tag)

            def basic_nack(self, delivery_tag, requeue):
                self.nacks.append((delivery_tag, requeue))

        with tempfile.TemporaryDirectory() as pasta:
            store = StateStore(os.path.join(pasta, 'states.sqlite3'))
            canal = Canal()
            for indice in range(3):
                evento = _evento(f'consumer-{indice}', cpu=75)
                _processar_mensagem(
                    store,
                    canal,
                    SimpleNamespace(delivery_tag=indice),
                    None,
                    json.dumps(evento).encode('utf-8'),
                )

            _processar_mensagem(
                store,
                canal,
                SimpleNamespace(delivery_tag=3),
                None,
                b'{invalido',
            )

            self.assertEqual(canal.acks, [0, 1, 2])
            self.assertEqual(canal.nacks, [(3, False)])
            self.assertEqual(len(store.listar_transicoes()), 1)

    def test_transicao_pendente_vira_send_alert_e_so_entao_e_marcada(self):
        class CanalFake:
            def close(self):
                return None

        enviados = []
        encerrar = threading.Event()

        class StubFake:
            def __init__(self, canal):
                return None

            def SendAlert(self, pedido, timeout):
                enviados.append(pedido)
                encerrar.set()
                return pb.AlertAck(accepted=True, alert_id=pedido.alert_id)

        with tempfile.TemporaryDirectory() as pasta:
            store = StateStore(os.path.join(pasta, 'states.sqlite3'))
            for indice in range(3):
                store.processar(_evento(f'critical-{indice}', cpu=95))

            with patch('consumer.main.grpc.insecure_channel', return_value=CanalFake()), \
                    patch('consumer.main.pb2_grpc.SystemMonitorStub', StubFake):
                consumer_main._loop_notificacoes(store, encerrar)

            self.assertEqual(len(enviados), 1)
            self.assertEqual(enviados[0].level, 'CRITICAL')
            self.assertEqual(enviados[0].alert_id, 'critical-2')
            self.assertEqual(enviados[0].host_id, 'host-test')
            self.assertEqual(store.alertas_pendentes(), [])


class PublisherTests(unittest.TestCase):
    def test_publica_com_confirmacao_e_remove_do_outbox(self):
        publicado = threading.Event()
        mensagens = []

        class Canal:
            def exchange_declare(self, **kwargs):
                return None

            def queue_declare(self, **kwargs):
                return None

            def queue_bind(self, **kwargs):
                return None

            def confirm_delivery(self):
                return None

            def basic_publish(self, **kwargs):
                mensagens.append(kwargs)
                publicado.set()
                return True

        class Conexao:
            def __init__(self, parametros):
                self.is_open = True
                self._canal = Canal()

            def channel(self):
                return self._canal

            def close(self):
                self.is_open = False

        with tempfile.TemporaryDirectory() as pasta:
            outbox = OutboxSQLite(os.path.join(pasta, 'outbox.sqlite3'))
            publisher = PublicadorRabbitMQ(outbox, 'amqp://guest:guest@localhost:5672/%2F')
            try:
                with patch('server.rabbit_publisher.pika.BlockingConnection', Conexao):
                    publisher.publicar(_evento('publish-test'))
                    self.assertTrue(publicado.wait(3))
                    limite = time.monotonic() + 2
                    while outbox.quantidade_pendente() and time.monotonic() < limite:
                        time.sleep(0.02)
            finally:
                publisher.fechar(timeout=1)

            self.assertEqual(outbox.quantidade_pendente(), 0)
            self.assertEqual(len(mensagens), 1)
            self.assertTrue(mensagens[0]['mandatory'])
            self.assertEqual(mensagens[0]['properties'].message_id, 'publish-test')
            self.assertEqual(
                json.loads(mensagens[0]['body'])['event_id'], 'publish-test'
            )


if __name__ == '__main__':
    unittest.main()
