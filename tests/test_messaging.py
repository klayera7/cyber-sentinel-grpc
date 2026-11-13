import json
import os
import queue
import subprocess
import tempfile
import threading
import time
import unittest
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch

import psutil
import collector.client as collector_client
import consumer.main as consumer_main
from consumer.main import _processar_mensagem
from consumer.state_machine import MaquinaEstados
from consumer.state_store import StateStore
from app import config_limites, rabbitmq_service
from app.historico import HistoricoSessao
from app.main import App
from app.processos import (
    nome_protegido,
    preparar_grupo_processo,
    terminar_grupo_processo,
    terminar_processo,
)
from messaging.eventos import criar_evento
from messaging import debug_log, session_report
from messaging.config import caminho_dados_sessao
from server.outbox import OutboxSQLite
from server.rabbit_publisher import PublicadorRabbitMQ
from server.main import ServidorMonitorizacao
from server.diagnostico import diagnostico_rapido, estado_interessante
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
                store.processar(_evento(f'critico-{i}', cpu=90))
                for i in range(2)
            ]
            self.assertEqual(resultados[-1]['transicao'], ('NORMAL', 'CRÍTICO'))

            recuperacao = [
                store.processar(_evento(f'recuperacao-{i}')) for i in range(5)
            ]
            self.assertEqual(
                recuperacao[-1]['transicao'], ('CRÍTICO', 'NORMAL')
            )
            resultados = [
                store.processar(_evento(f'temperatura-{i}', temperatura=92))
                for i in range(2)
            ]
            self.assertEqual(resultados[-1]['transicao'], ('NORMAL', 'CRÍTICO'))

            self.assertTrue(store.processar(_evento('temperatura-1'))['duplicado'])
            pendentes = store.alertas_pendentes()
            self.assertEqual(len(pendentes), 3)
            store.marcar_alerta_enviado(pendentes[0]['event_id'])
            self.assertEqual(len(store.alertas_pendentes()), 2)

            store_reaberto = StateStore(caminho)
            self.assertEqual(store_reaberto.estado_atual('host-test'), 'CRÍTICO')
            self.assertEqual(len(store_reaberto.listar_transicoes()), 3)

    def test_limites_globais_configuraveis(self):
        with tempfile.TemporaryDirectory() as pasta:
            store = StateStore(
                os.path.join(pasta, 'states.sqlite3'),
                cpu_limit=55, ram_limit=95,
            )
            self.assertIsNone(
                store.processar(_evento('limite-1', cpu=60, ram=20))['transicao']
            )
            resultado = store.processar(_evento('limite-2', cpu=60, ram=20))
            self.assertEqual(resultado['transicao'], ('NORMAL', 'CRÍTICO'))

            store_ram = StateStore(
                os.path.join(pasta, 'states-ram.sqlite3'),
                cpu_limit=95, ram_limit=40,
            )
            store_ram.processar(_evento('ram-1', cpu=10, ram=45))
            resultado = store_ram.processar(_evento('ram-2', cpu=10, ram=45))
            self.assertEqual(resultado['transicao'], ('NORMAL', 'CRÍTICO'))

    def test_alerta_global_anexa_processo_sem_limite_individual(self):
        with tempfile.TemporaryDirectory() as pasta:
            store = StateStore(
                os.path.join(pasta, 'states.sqlite3'),
                cpu_limit=60, ram_limit=70,
            )
            for indice in range(2):
                evento = _evento(f'process-hint-{indice}', cpu=65, ram=40)
                evento['processes'] = [{
                    'pid': 4321,
                    'name': 'game.exe',
                    'create_time': 1234.5,
                    'cpu_percent': 8,
                    'ram_percent': 10,
                    'rss_bytes': 400000000,
                }]
                store.processar(evento)

            alerta = store.alertas_pendentes()[0]

        self.assertEqual(alerta['process']['pid'], 4321)
        self.assertEqual(alerta['process']['cpu_percent'], 8)

    def test_sugestao_de_ram_ignora_processo_com_cpu_maior(self):
        with tempfile.TemporaryDirectory() as pasta:
            store = StateStore(
                os.path.join(pasta, 'states.sqlite3'),
                cpu_limit=95, ram_limit=40,
            )
            for indice in range(2):
                evento = _evento(f'ram-hint-{indice}', cpu=10, ram=45)
                evento['processes'] = [
                    {
                        'pid': 100,
                        'name': 'cpu-alta.exe',
                        'create_time': 1.0,
                        'cpu_percent': 80,
                        'ram_percent': 5,
                        'rss_bytes': 400000000,
                    },
                    {
                        'pid': 200,
                        'name': 'ram-alta.exe',
                        'create_time': 2.0,
                        'cpu_percent': 5,
                        'ram_percent': 30,
                        'rss_bytes': 900000000,
                    },
                ]
                store.processar(evento)

            alerta = store.alertas_pendentes()[0]

        self.assertEqual(alerta['process']['pid'], 200)

    def test_alerta_gpu_nao_anexa_sugestao_de_processo(self):
        with tempfile.TemporaryDirectory() as pasta:
            store = StateStore(os.path.join(pasta, 'states.sqlite3'))
            for indice in range(2):
                evento = _evento(f'gpu-only-{indice}', temperatura=95)
                evento['processes'] = [{
                    'pid': 4321,
                    'name': 'game.exe',
                    'create_time': 1234.5,
                    'cpu_percent': 8,
                    'ram_percent': 10,
                    'rss_bytes': 400000000,
                }]
                store.processar(evento)

            alerta = store.alertas_pendentes()[0]

        self.assertIsNone(alerta['process'])

    def test_classifica_limites_e_sensor_ausente(self):
        self.assertEqual(
            MaquinaEstados.classificar({
                'cpu_percent': 70,
                'ram_percent': 20,
                'gpu_percent': None,
                'gpu_mem_percent': None,
                'gpu_temp_celsius': None,
            }),
            MaquinaEstados.NORMAL,
        )
        self.assertEqual(
            MaquinaEstados.classificar({
                'cpu_percent': 20,
                'ram_percent': 20,
                'gpu_percent': None,
                'gpu_mem_percent': None,
                'gpu_temp_celsius': 80,
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
        self.assertEqual(
            MaquinaEstados.classificar({
                'cpu_percent': 85,
                'ram_percent': 20,
                'gpu_percent': None,
                'gpu_mem_percent': None,
                'gpu_temp_celsius': None,
            }),
            MaquinaEstados.CRITICO,
        )
        self.assertEqual(
            MaquinaEstados.classificar({
                'cpu_percent': 84.9,
                'ram_percent': 84.9,
                'gpu_percent': None,
                'gpu_mem_percent': None,
                'gpu_temp_celsius': None,
            }),
            MaquinaEstados.NORMAL,
        )

    def test_classifica_com_limites_customizados(self):
        self.assertEqual(
            MaquinaEstados.classificar(
                {'cpu_percent': 55, 'ram_percent': 30},
                cpu_limit=55, ram_limit=90,
            ),
            MaquinaEstados.CRITICO,
        )
        self.assertEqual(
            MaquinaEstados.classificar(
                {'cpu_percent': 40, 'ram_percent': 30},
                cpu_limit=55, ram_limit=90,
            ),
            MaquinaEstados.NORMAL,
        )

    def test_diagnostico_rapido_usa_85_porcento_para_cpu_ram_criticas(self):
        dados = pb.TelemetryData(cpu_percent=85, ram_percent=85, timestamp='x')
        texto = diagnostico_rapido(dados)
        self.assertIn('CPU em nível crítico', texto)
        self.assertIn('RAM quase esgotada', texto)


class EventTests(unittest.TestCase):
    def test_evento_inclui_origem_e_metricas_opcionais(self):
        evento = criar_evento(FakeTelemetry(), 'Sistema estável', 'regra')
        self.assertEqual(evento['event_type'], 'system.analysis.completed')
        self.assertEqual(evento['diagnosis_source'], 'regra')
        self.assertEqual(evento['metrics']['gpu_temp_celsius'], 62.0)
        self.assertIsNone(evento['metrics']['gpu_mem_percent'])
        self.assertTrue(evento['event_id'])

    def test_evento_carrega_id_da_sessao_ativa(self):
        with patch.dict(os.environ, {'SENTINEL_SESSION_ID': 'sessao-atual'}):
            evento = criar_evento(FakeTelemetry(), 'Sistema estável', 'regra')

        self.assertEqual(evento['session_id'], 'sessao-atual')

    def test_configuracao_invalida_nao_impede_iniciar_com_padroes(self):
        for conteudo in ('[]', 'null', '"texto"'):
            with tempfile.TemporaryDirectory() as pasta, patch.dict(
                os.environ,
                {'SENTINEL_SETTINGS': os.path.join(pasta, 'settings.json')},
            ):
                with open(
                    os.environ['SENTINEL_SETTINGS'], 'w', encoding='utf-8',
                ) as arquivo:
                    arquivo.write(conteudo)
                self.assertEqual(
                    config_limites.carregar(), config_limites.PADRAO,
                )

    def test_processos_do_sistema_e_ollama_sao_protegidos(self):
        for nome in (
            'MemCompression', 'MsMpEng.exe', 'llama-server.exe', 'ollama.exe',
            'ollama app.exe', 'epmd.exe', 'cyber-sentinel.exe',
        ):
            self.assertTrue(nome_protegido(nome), nome)

    def test_diagnostico_rapido_respeita_limite_configurado(self):
        with patch.dict(
            os.environ,
            {'SENTINEL_CPU_LIMIT': '60', 'SENTINEL_RAM_LIMIT': '60'},
        ):
            texto = diagnostico_rapido(
                pb.TelemetryData(
                    cpu_percent=62, ram_percent=10, timestamp='x',
                )
            )
            interessante = estado_interessante(
                pb.TelemetryData(
                    cpu_percent=62, ram_percent=10, timestamp='x',
                )
            )

        self.assertIn('CPU em nível crítico', texto)
        self.assertTrue(interessante)

    def test_limites_do_painel_sao_persistidos(self):
        with tempfile.TemporaryDirectory() as pasta, patch.dict(
            os.environ,
            {'SENTINEL_SETTINGS': os.path.join(pasta, 'settings.json')},
        ):
            config_limites.salvar({
                'cpu_global': 64,
                'ram_global': 72,
            })
            limites = config_limites.carregar()

        self.assertEqual(
            limites,
            {
                'cpu_global': 64.0,
                'ram_global': 72.0,
            },
        )

    def test_caminhos_de_dados_sao_unicos_por_sessao(self):
        with tempfile.TemporaryDirectory() as pasta, patch.dict(
            os.environ,
            {'SENTINEL_STATE_DB': os.path.join(pasta, 'state_machine.sqlite3')},
        ):
            primeiro = caminho_dados_sessao(
                'SENTINEL_STATE_DB', 'state_machine.sqlite3', 'sessao-1',
            )
            segundo = caminho_dados_sessao(
                'SENTINEL_STATE_DB', 'state_machine.sqlite3', 'sessao-2',
            )

        self.assertNotEqual(primeiro, segundo)
        self.assertIn(os.path.join('sessions', 'sessao-1'), primeiro)
        self.assertIn(os.path.join('sessions', 'sessao-2'), segundo)

    def test_debug_log_inclui_horarios_e_redige_credenciais(self):
        with tempfile.TemporaryDirectory() as pasta:
            caminho = os.path.join(pasta, 'debug.txt')
            with patch.dict(os.environ, {'SENTINEL_DEBUG_LOG': caminho}):
                debug_log.registrar(
                    'teste', 'credenciais',
                    endpoint='amqp://usuario:segredo@broker.local:5672/%2F',
                    password='segredo',
                )

            with open(caminho, encoding='utf-8') as arquivo:
                linha = arquivo.read()

        self.assertRegex(
            linha,
            r'^\d{4}-\d{2}-\d{2}T.*[+-]\d{2}:\d{2} \| UTC '
            r'\d{4}-\d{2}-\d{2}T.*Z \| INFO \| teste \| credenciais \| ',
        )
        self.assertNotIn('usuario:segredo@', linha)
        self.assertNotIn('"password":"segredo"', linha)
        self.assertIn('[REDACTED]', linha)

    def test_coletor_standalone_nao_registra_caminho_do_executavel(self):
        processos = [{
            'pid': 42,
            'name': 'game.exe',
            'create_time': 1.0,
            'exe_path': r'C:\Games\game.exe',
            'cpu_percent': 10.0,
            'ram_percent': 5.0,
            'rss_bytes': 100,
        }]
        with patch('collector.client.psutil.cpu_percent', return_value=20), \
                patch(
                    'collector.client.psutil.virtual_memory',
                    return_value=SimpleNamespace(percent=30),
                ), patch('collector.client.coletar_processos', return_value=processos), \
                patch('collector.client.debug_log.registrar') as registrar:
            dados = next(collector_client.generate_telemetry())

        self.assertNotIn('exe_path', registrar.call_args.kwargs['top_processes'][0])
        self.assertEqual(dados.top_processes[0].pid, 42)

    def test_relatorio_markdown_registra_sessao_e_sumario(self):
        session_id = 'sessao-teste-1'
        with tempfile.TemporaryDirectory() as pasta, patch.dict(
            os.environ,
            {
                'SENTINEL_DEBUG_LOG': os.path.join(pasta, 'debug.txt'),
                'SENTINEL_SESSION_ID': session_id,
            },
        ):
            session_report.iniciar_sessao(session_id)
            debug_log.registrar(
                'coletor', 'amostra', cpu_percent=60, ram_percent=70,
                gpu_percent=20, gpu_mem_percent=15, gpu_temp_celsius=55,
            )
            debug_log.registrar(
                'app', 'popup_exibido', alert_id='alerta-teste',
            )
            session_report.finalizar_sessao(session_id, 'parada_pelo_usuario')

            caminho = os.path.join(pasta, 'sessoes', f'{session_id}.md')
            with open(caminho, encoding='utf-8') as arquivo:
                relatorio = arquivo.read()

        self.assertIn(f'# Relatório da sessão `{session_id}`', relatorio)
        self.assertIn('Amostra: CPU 60.0%', relatorio)
        self.assertIn('app.popup_exibido', relatorio)
        self.assertIn('Amostras coletadas:** 1', relatorio)
        self.assertIn('| CPU | 60.0% | 60.0% | 60.0% |', relatorio)
        self.assertIn('parada_pelo_usuario', relatorio)

    def test_proxima_sessao_marca_a_anterior_sem_fechamento(self):
        with tempfile.TemporaryDirectory() as pasta, patch.dict(
            os.environ,
            {'SENTINEL_DEBUG_LOG': os.path.join(pasta, 'debug.txt')},
        ):
            session_report.iniciar_sessao('sessao-interrompida')
            session_report.iniciar_sessao('sessao-seguinte')
            session_report.finalizar_sessao('sessao-seguinte', 'teste')

            caminho = os.path.join(
                pasta, 'sessoes', 'sessao-interrompida.md',
            )
            with open(caminho, encoding='utf-8') as arquivo:
                relatorio = arquivo.read()

        self.assertIn('interrompida sem registro de encerramento normal', relatorio)

    def test_amostra_publicada_para_estado_sem_analise_lenta(self):
        eventos = []
        servidor = ServidorMonitorizacao(
            publicador=SimpleNamespace(publicar=eventos.append),
        )
        dados = pb.TelemetryData(
            cpu_percent=20, ram_percent=30, timestamp='12:00:00',
        )
        dados.top_processes.add(
            pid=4321, name='game.exe', create_time=1000.0,
            cpu_percent=10, ram_percent=5, rss_bytes=400000000,
        )
        servidor.publicar_amostra_estado(dados)

        self.assertEqual(len(eventos), 1)
        self.assertTrue(eventos[0]['state_relevant'])
        self.assertEqual(eventos[0]['diagnosis_source'], 'regra')
        self.assertEqual(eventos[0]['processes'][0]['pid'], 4321)
        self.assertEqual(servidor.ultimo_evento_estado, eventos[0]['event_id'])

        with patch.dict(
            os.environ,
            {'SENTINEL_CPU_LIMIT': '15', 'SENTINEL_RAM_LIMIT': '85'},
        ):
            servidor.publicar_amostra_estado(
                pb.TelemetryData(
                    cpu_percent=20, ram_percent=30, timestamp='12:00:01',
                )
            )
        self.assertIn('limite 15%', eventos[1]['diagnosis'])

    def test_evento_direto_chega_ao_state_store_com_processo(self):
        eventos = []
        servidor = ServidorMonitorizacao(
            publicador=SimpleNamespace(publicar=eventos.append),
        )
        for _ in range(2):
            dados = pb.TelemetryData(
                cpu_percent=90, ram_percent=20, timestamp='12:00:00',
            )
            dados.top_processes.add(
                pid=4321, name='game.exe', create_time=1000.0,
                cpu_percent=40, ram_percent=10, rss_bytes=400000000,
            )
            servidor.publicar_amostra_estado(dados)

        with tempfile.TemporaryDirectory() as pasta:
            store = StateStore(
                os.path.join(pasta, 'states.sqlite3'),
                cpu_limit=85, ram_limit=85,
            )
            for evento in eventos:
                store.processar(evento)
            alerta = store.alertas_pendentes()[0]

        self.assertEqual(alerta['process']['pid'], 4321)

    def test_analise_nao_dirige_estado_quando_ha_amostras_diretas(self):
        eventos = []
        servidor = ServidorMonitorizacao(
            publicador=SimpleNamespace(publicar=eventos.append),
        )
        with patch.dict(os.environ, {'OLLAMA_MODEL': 'teste'}):
            list(servidor.StreamTelemetry([FakeTelemetry()], None))
            self.assertTrue(eventos[0]['state_relevant'])

            eventos.clear()
            servidor.ativar_amostras_diretas()
            list(servidor.StreamTelemetry([FakeTelemetry()], None))

        self.assertFalse(eventos[0]['state_relevant'])

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


class AppLifecycleTests(unittest.TestCase):
    def test_popup_de_sugestao_fica_visivel_por_dez_segundos(self):
        class WidgetFake:
            def __init__(self):
                self.timeout = None

            def __getattr__(self, _nome):
                return lambda *args, **kwargs: None

            def after(self, timeout, callback):
                self.timeout = timeout

        class RootFake:
            def update_idletasks(self):
                return None

            def winfo_rootx(self):
                return 0

            def winfo_rooty(self):
                return 0

            def winfo_width(self):
                return 1160

        popup = WidgetFake()
        app = App.__new__(App)
        app.root = RootFake()
        app._popup_alerta = None
        app._popup_alerta_id = None
        alerta = {
            'alert_id': 'popup-1',
            'host_id': 'host-test',
            'level': 'CRITICAL',
            'message': 'CPU global acima do limite.',
            'process': {
                'pid': 1234,
                'name': 'game.exe',
                'exe_path': r'C:\Users\person\Apps\game.exe',
                'create_time': 1000.0,
                'cpu_percent': 40,
                'ram_percent': 20,
            },
        }
        with tempfile.TemporaryDirectory() as pasta, patch.dict(
            os.environ,
            {'SENTINEL_DEBUG_LOG': os.path.join(pasta, 'debug.txt')},
        ), patch('app.main.tk.Toplevel', return_value=popup), \
                patch('app.main.tk.Frame', side_effect=lambda *a, **k: WidgetFake()), \
                patch('app.main.tk.Label', side_effect=lambda *a, **k: WidgetFake()), \
                patch('app.main.tk.Button', side_effect=lambda *a, **k: WidgetFake()):
            app._mostrar_toast(alerta)
            with open(os.environ['SENTINEL_DEBUG_LOG'], encoding='utf-8') as arquivo:
                log = arquivo.read()

        self.assertEqual(popup.timeout, 10000)
        self.assertNotIn('C:\\Users\\person\\Apps\\game.exe', log)

    def test_encerramento_de_processo_cancela_se_estado_global_recuperou(self):
        app = App.__new__(App)
        app.notificacoes = [{
            'alert_id': 'alerta-processo',
            'host_id': 'host-test',
            'action_status': 'PENDENTE',
            'process': {
                'pid': 4321,
                'name': 'game.exe',
                'create_time': 1234.5,
                'cpu_percent': 40,
                'ram_percent': 8,
            },
        }]
        app.state_store = SimpleNamespace(estado_atual=lambda _: 'NORMAL')
        app.coletor = SimpleNamespace(
            ultima=lambda: SimpleNamespace(cpu=40, ram=50),
        )
        app.limites = {'cpu_global': 65, 'ram_global': 65}
        finalizacoes = []
        app._finalizar_sugestao = lambda *args: finalizacoes.append(args)

        with patch('app.main.preparar_grupo_processo') as preparar:
            app._resolver_sugestao('alerta-processo', 'ENCERRAR')

        self.assertEqual(len(finalizacoes), 1)
        self.assertEqual(finalizacoes[0][1], 'NAO_MAIS_ALTO')
        preparar.assert_not_called()

    def test_reavaliacao_sugere_outro_processo_e_para_apos_normalizar(self):
        class RootFake:
            def __init__(self):
                self.callbacks = []

            def after(self, intervalo, callback, *args):
                if callback.__name__ == '_confirmar_grupo_preparado':
                    return callback(*args)
                self.callbacks.append((callback, args))

        app = App.__new__(App)
        app.root = RootFake()
        app._app_fechando = False
        app.limites = {'cpu_global': 85, 'ram_global': 85}
        app.state_store = SimpleNamespace(estado_atual=lambda _: 'CRÍTICO')
        amostra_atual = SimpleNamespace(cpu=95, ram=20)
        app.coletor = SimpleNamespace(ultima=lambda: amostra_atual)
        app._renderizar_notificacoes = lambda: None
        app._renderizar_acoes_pendentes = lambda: None
        app._popup_alerta_id = None
        app._popup_alerta = None
        alerts = []
        app._receber_alerta = alerts.append
        amostras = [
            SimpleNamespace(
                cpu=95,
                ram=20,
                processos=[
                    {
                        'pid': 201, 'name': 'restarted.exe',
                        'create_time': 2.1, 'exe_path': r'c:\games\root-heavy.exe',
                        'cpu_percent': 100, 'ram_percent': 5, 'rss_bytes': 100,
                    },
                    {
                        'pid': 200, 'name': 'cpu-heavy.exe',
                        'create_time': 2.0, 'exe_path': r'c:\games\cpu-heavy.exe',
                        'cpu_percent': 70,
                        'ram_percent': 5, 'rss_bytes': 100,
                    },
                    {
                        'pid': 300, 'name': 'ram-heavy.exe',
                        'create_time': 3.0, 'exe_path': r'c:\games\ram-heavy.exe',
                        'cpu_percent': 10,
                        'ram_percent': 80, 'rss_bytes': 200,
                    },
                ],
            ),
            SimpleNamespace(cpu=50, ram=40, processos=[]),
        ]
        app._aguardar_nova_amostra = lambda _: amostras.pop(0)
        primeiro = {
            'alert_id': 'primeiro',
            'host_id': 'host-test',
            'action_status': 'PENDENTE',
            'process': {
                'pid': 100, 'name': 'root-heavy.exe',
                'create_time': 1.0, 'cpu_percent': 90,
                'ram_percent': 10, 'rss_bytes': 300,
            },
        }

        class ThreadInline:
            def __init__(self, target, args, daemon):
                self.target = target
                self.args = args

            def start(self):
                self.target(*self.args)

        app.notificacoes = [primeiro]
        grupos = [
            {
                'exe_path': r'c:\games\root-heavy.exe',
                'anchor': {'pid': 100, 'name': 'root-heavy.exe', 'create_time': 1.0},
                'processes': [
                    {
                        'pid': 100, 'ppid': 1, 'name': 'root-heavy.exe',
                        'create_time': 1.0, 'exe_path': r'c:\games\root-heavy.exe',
                    },
                    {
                        'pid': 101, 'ppid': 100, 'name': 'worker.exe',
                        'create_time': 1.1, 'exe_path': r'c:\games\worker.exe',
                    },
                    {
                        'pid': 102, 'ppid': 101, 'name': 'renderer.exe',
                        'create_time': 1.2, 'exe_path': r'c:\games\renderer.exe',
                    },
                    *[
                        {
                            'pid': pid, 'ppid': 100, 'name': f'worker-{pid}.exe',
                            'create_time': 1.0 + pid / 1000,
                            'exe_path': r'c:\games\worker.exe',
                        }
                        for pid in range(103, 110)
                    ],
                ],
                'roots': [100],
            },
            {
                'exe_path': r'C:\Games\cpu-heavy.exe',
                'anchor': {'pid': 200, 'name': 'cpu-heavy.exe', 'create_time': 2.0},
                'processes': [{
                    'pid': 200, 'ppid': 1, 'name': 'cpu-heavy.exe',
                    'create_time': 2.0, 'exe_path': r'c:\games\cpu-heavy.exe',
                }],
                'roots': [200],
            },
        ]
        with patch('app.main.messagebox.askyesno', return_value=True) as confirmar, \
                patch('app.main.threading.Thread', ThreadInline), \
                patch(
                    'app.main.preparar_grupo_processo',
                    side_effect=lambda *_: ('OK', '', grupos.pop(0)),
                ), patch(
                    'app.main.terminar_grupo_processo',
                    return_value=('ENCERRADO', 'Encerrado.'),
                ):
            app._resolver_sugestao('primeiro', 'ENCERRAR')
            self.assertEqual(len(alerts), 1)
            segundo = alerts[0]
            self.assertEqual(segundo['process']['pid'], 200)
            self.assertNotEqual(segundo['alert_id'], primeiro['alert_id'])
            self.assertIn(r'c:\games\root-heavy.exe', confirmar.call_args.args[1])
            self.assertIn('10 processo(s)', confirmar.call_args.args[1])
            self.assertIn('PID 101', confirmar.call_args.args[1])
            self.assertIn('PID 109', confirmar.call_args.args[1])

            segundo['action_status'] = 'PENDENTE'
            app.notificacoes.append(segundo)
            app._resolver_sugestao(segundo['alert_id'], 'ENCERRAR')

        self.assertEqual(confirmar.call_count, 2)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(len(app.root.callbacks), 2)

    def test_reavaliacao_ignora_amostra_iniciada_antes_do_encerramento(self):
        app = App.__new__(App)
        app._app_fechando = False
        amostra_antiga = SimpleNamespace(inicio_monotonic=9.0)
        amostra_nova = SimpleNamespace(inicio_monotonic=11.0)
        leituras = iter((amostra_antiga, amostra_nova))
        app.coletor = SimpleNamespace(ultima=lambda: next(leituras))

        with patch('app.main.time.sleep'):
            resultado = app._aguardar_nova_amostra(10.0)

        self.assertIs(resultado, amostra_nova)

    def test_grupo_alterado_reabre_confirmacao_com_pids_atualizados(self):
        class RootFake:
            def after(self, intervalo, callback, *args):
                return callback(*args)

        app = App.__new__(App)
        app.root = RootFake()
        app._app_fechando = False
        app._renderizar_notificacoes = lambda: None
        app._renderizar_acoes_pendentes = lambda: None
        confirmacoes = []
        app._confirmar_grupo_preparado = lambda *args, **kwargs: confirmacoes.append(
            (args, kwargs),
        )
        alerta = {
            'alert_id': 'grupo-mutado',
            'action_status': 'ENCERRANDO',
            'process': {'pid': 10, 'name': 'game.exe', 'create_time': 1.0},
        }
        grupo = {
            'exe_path': r'c:\games\game.exe',
            'processes': [
                {'pid': 10, 'name': 'game.exe', 'create_time': 1.0},
                {'pid': 11, 'name': 'worker.exe', 'create_time': 2.0},
            ],
        }

        app._reconfirmar_grupo_alterado(
            alerta, grupo, 'O grupo ganhou processos desde a confirmação.',
        )

        self.assertEqual(alerta['action_status'], 'PREPARANDO')
        self.assertEqual([item['pid'] for item in alerta['_grupo_processos']], [10, 11])
        self.assertEqual(confirmacoes[0][0][1:3], ('OK', ''))
        self.assertIn('grupo ganhou processos', confirmacoes[0][1]['aviso'])

    def test_cancelar_confirmacao_registra_sugestao_como_ignorada(self):
        class RootFake:
            def after(self, intervalo, callback, *args):
                return callback(*args)

        class ThreadInline:
            def __init__(self, target, args, daemon):
                self.target = target
                self.args = args

            def start(self):
                self.target(*self.args)

        app = App.__new__(App)
        alerta = {
            'alert_id': 'alerta-cancelado',
            'host_id': 'host-test',
            'action_status': 'PENDENTE',
            'process': {
                'pid': 1234, 'name': 'game.exe',
                'create_time': 1.0, 'cpu_percent': 50, 'ram_percent': 5,
            },
        }
        app.notificacoes = [alerta]
        app.root = RootFake()
        app.state_store = SimpleNamespace(estado_atual=lambda _: 'CRÍTICO')
        app.coletor = SimpleNamespace(ultima=lambda: SimpleNamespace(cpu=90, ram=20))
        app.limites = {'cpu_global': 85, 'ram_global': 85}
        app._renderizar_notificacoes = lambda: None
        app._renderizar_acoes_pendentes = lambda: None
        finalizacoes = []
        app._finalizar_sugestao = lambda *args: finalizacoes.append(args)

        grupo = {
            'exe_path': r'C:\Games\game.exe',
            'processes': [{
                'pid': 1234, 'name': 'game.exe', 'create_time': 1.0,
            }],
        }
        with patch('app.main.messagebox.askyesno', return_value=False), \
                patch('app.main.debug_log.registrar'), \
                patch('app.main.threading.Thread', ThreadInline), \
                patch(
                    'app.main.preparar_grupo_processo',
                    return_value=('OK', '', grupo),
                ), patch('app.main.terminar_grupo_processo') as encerrar:
            app._resolver_sugestao('alerta-cancelado', 'ENCERRAR')

        self.assertEqual(finalizacoes[0][1], 'IGNORADO')
        encerrar.assert_not_called()

    def test_sessao_do_painel_usa_paths_proprios_e_restaura_ambiente(self):
        app = App.__new__(App)
        app.session_id = None
        app._env_sessao_original = None
        app.state_store = None

        with tempfile.TemporaryDirectory() as pasta, patch.dict(
            os.environ,
            {
                'SENTINEL_DEBUG_LOG': os.path.join(pasta, 'logs', 'debug.txt'),
                'SENTINEL_OUTBOX_DB': os.path.join(pasta, 'base', 'outbox.sqlite3'),
                'SENTINEL_STATE_DB': os.path.join(pasta, 'base', 'state.sqlite3'),
            },
        ):
            original_outbox = os.environ['SENTINEL_OUTBOX_DB']
            original_state = os.environ['SENTINEL_STATE_DB']
            original_session = os.environ.get('SENTINEL_SESSION_ID')
            app._iniciar_nova_sessao()
            session_id = app.session_id
            self.assertEqual(os.environ['SENTINEL_SESSION_ID'], session_id)
            self.assertIn(session_id, os.environ['SENTINEL_OUTBOX_DB'])
            self.assertIn(session_id, os.environ['SENTINEL_STATE_DB'])

            app._finalizar_sessao('teste')

            self.assertEqual(os.environ['SENTINEL_OUTBOX_DB'], original_outbox)
            self.assertEqual(os.environ['SENTINEL_STATE_DB'], original_state)
            self.assertEqual(os.environ.get('SENTINEL_SESSION_ID'), original_session)
            self.assertTrue(
                os.path.isfile(
                    os.path.join(pasta, 'logs', 'sessoes', f'{session_id}.md')
                )
            )

    def test_parar_espera_ultimo_evento_e_notificacao(self):
        class BotaoFake:
            def set_enabled(self, enabled):
                return None

        class ConsumidorFake:
            def poll(self):
                return None

        class OutboxFake:
            def quantidade_pendente(self):
                return 0

        servicer = SimpleNamespace(
            ultimo_evento_estado='evento-final',
            publicador=SimpleNamespace(outbox=OutboxFake()),
        )
        app = App.__new__(App)
        app.coletor = SimpleNamespace(parar=lambda: None, ao_amostra=None)
        app.servidor = SimpleNamespace(_sentinel_servicer=servicer)
        app._log = lambda _mensagem: None
        app.processo_consumer = ConsumidorFake()
        app.btn_iniciar = BotaoFake()
        app.btn_parar = BotaoFake()
        app.pill_servidor = object()
        app._set_pill = lambda *args: None
        app._parar_consumidor = lambda: None
        app._encerrar_rabbitmq = lambda: None
        app._finalizar_sessao = lambda _motivo: None
        app._iniciando = False

        class StoreFake:
            def __init__(self):
                self.consultas = 0

            def evento_processado(self, event_id):
                self.consultas += 1
                return self.consultas >= 2

            def alertas_pendentes(self):
                return [] if self.consultas >= 2 else [{'alert_id': 'x'}]

        app.state_store = StoreFake()
        inicio = time.monotonic()
        with patch('app.main.parar_servidor'), \
                patch('app.main.TEMPO_DRENAGEM_SESSAO', 1.0):
            app._parar()

        self.assertGreaterEqual(app.state_store.consultas, 2)
        self.assertLess(time.monotonic() - inicio, 0.8)

    def test_parar_nao_espera_consumidor_se_outbox_esta_pendente(self):
        class BotaoFake:
            def set_enabled(self, enabled):
                return None

        class ConsumidorFake:
            def poll(self):
                return None

        class OutboxFake:
            def quantidade_pendente(self):
                return 1

        class StoreFake:
            def evento_processado(self, event_id):
                raise AssertionError('Não deve consultar drenagem com outbox pendente')

            def alertas_pendentes(self):
                raise AssertionError('Não deve consultar alertas com outbox pendente')

        servicer = SimpleNamespace(
            ultimo_evento_estado='evento-final',
            publicador=SimpleNamespace(outbox=OutboxFake()),
        )
        app = App.__new__(App)
        app.coletor = SimpleNamespace(parar=lambda: None, ao_amostra=None)
        app.servidor = SimpleNamespace(_sentinel_servicer=servicer)
        app._log = lambda _mensagem: None
        app.processo_consumer = ConsumidorFake()
        app.btn_iniciar = BotaoFake()
        app.btn_parar = BotaoFake()
        app.pill_servidor = object()
        app._set_pill = lambda *args: None
        app._parar_consumidor = lambda: None
        app._encerrar_rabbitmq = lambda: None
        app._finalizar_sessao = lambda _motivo: None
        app._iniciando = False
        app.state_store = StoreFake()

        with patch('app.main.parar_servidor'):
            app._parar()

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


class ProcessoFake:
    def __init__(self, pid, nome, exe, criado, ppid=1):
        self.pid = pid
        self.nome = nome
        self.caminho = exe
        self.criado = criado
        self._ppid = ppid
        self.filhos = []

    def oneshot(self):
        return nullcontext()

    def name(self):
        return self.nome

    def exe(self):
        return self.caminho

    def create_time(self):
        return self.criado

    def ppid(self):
        return self._ppid

    def children(self, recursive=False):
        return self.filhos


class ProcessActionSafetyTests(unittest.TestCase):
    def test_bloqueia_encerrar_o_proprio_processo(self):
        status, _ = terminar_processo(os.getpid(), 1.0, 'python.exe')
        self.assertEqual(status, 'BLOQUEADO')

    def test_nao_encerra_pid_reutilizado(self):
        processo = ProcessoFake(4321, 'game.exe', r'C:\Games\game.exe', 2000.0)
        with patch('app.processos._pids_do_aplicativo', return_value=set()), \
                patch('app.processos.psutil.Process', return_value=processo):
            status, _ = terminar_processo(4321, 1000.0, 'game.exe')

        self.assertEqual(status, 'BLOQUEADO')

    def test_agrupa_mesmo_caminho_mais_filhos_e_killa_todas_as_raizes(self):
        raiz_a = ProcessoFake(4321, 'slowroads.exe', r'C:\Games\SlowRoads.exe', 1000.0)
        filho_a = ProcessoFake(4322, 'worker.exe', r'C:\Games\worker.exe', 1001.0, 4321)
        neto_a = ProcessoFake(4323, 'renderer.exe', r'C:\Games\renderer.exe', 1002.0, 4322)
        raiz_b = ProcessoFake(4331, 'slowroads.exe', r'c:\games\slowroads.exe', 1003.0)
        filho_b = ProcessoFake(4332, 'worker.exe', r'C:\Games\worker.exe', 1004.0, 4331)
        outro_diretorio = ProcessoFake(
            4400, 'slowroads.exe', r'C:\Other\slowroads.exe', 1005.0,
        )
        raiz_a.filhos = [filho_a, neto_a]
        raiz_b.filhos = [filho_b]
        processos = [raiz_a, filho_a, neto_a, raiz_b, filho_b, outro_diretorio]
        por_pid = {processo.pid: processo for processo in processos}
        with patch('app.processos._pids_do_aplicativo', return_value=set()), \
                patch('app.processos.psutil.Process', side_effect=por_pid.__getitem__), \
                patch('app.processos.psutil.process_iter', return_value=processos), \
                patch(
                    'app.processos.subprocess.run',
                    return_value=SimpleNamespace(returncode=0, stdout='', stderr=''),
                ) as taskkill, patch(
                    'app.processos.psutil.wait_procs',
                    side_effect=lambda processos, timeout: (processos, []),
                ):
            status, mensagem, grupo = preparar_grupo_processo(
                4321, 1000.0, 'slowroads.exe',
            )
            self.assertEqual(status, 'OK')
            self.assertEqual(
                {membro['pid'] for membro in grupo['processes']},
                {4321, 4322, 4323, 4331, 4332},
            )
            status, mensagem = terminar_grupo_processo(grupo)

        self.assertEqual(status, 'ENCERRADO')
        self.assertEqual(
            [chamada.args[0][-1] for chamada in taskkill.call_args_list],
            ['4321', '4331'],
        )

    def test_ppid_reutilizado_nao_classifica_processo_antigo_como_filho(self):
        raiz = ProcessoFake(4321, 'game.exe', r'C:\Games\game.exe', 1000.0)
        filho_antigo = ProcessoFake(
            4322, 'other.exe', r'C:\Other\other.exe', 999.995, 4321,
        )
        with patch('app.processos._pids_do_aplicativo', return_value=set()), \
                patch('app.processos.psutil.Process', return_value=raiz), \
                patch(
                    'app.processos.psutil.process_iter',
                    return_value=[raiz, filho_antigo],
                ), patch('app.processos.subprocess.run') as taskkill:
            status, _, _ = preparar_grupo_processo(4321, 1000.0, 'game.exe')

        self.assertEqual(status, 'FALHA')
        taskkill.assert_not_called()

    def test_nova_instancia_apos_confirmacao_exige_reconfirmar_grupo(self):
        raiz = ProcessoFake(4321, 'game.exe', r'C:\Games\game.exe', 1000.0)
        nova_raiz = ProcessoFake(4322, 'game.exe', r'C:\Games\game.exe', 1001.0)
        processos = [raiz]
        por_pid = {raiz.pid: raiz, nova_raiz.pid: nova_raiz}
        with patch('app.processos._pids_do_aplicativo', return_value=set()), \
                patch('app.processos.psutil.Process', side_effect=por_pid.__getitem__), \
                patch('app.processos.psutil.process_iter', side_effect=lambda: processos), \
                patch('app.processos.subprocess.run') as taskkill:
            status, _, grupo = preparar_grupo_processo(4321, 1000.0, 'game.exe')
            processos.append(nova_raiz)
            status, mensagem = terminar_grupo_processo(grupo)

        self.assertEqual(status, 'GRUPO_ALTERADO')
        self.assertIn('revise a lista atualizada', mensagem)
        self.assertEqual({membro['pid'] for membro in grupo['processes']}, {4321, 4322})
        taskkill.assert_not_called()

    def test_novo_descendente_antes_do_kill_exige_reconfirmacao(self):
        raiz = ProcessoFake(4321, 'game.exe', r'C:\Games\game.exe', 1000.0)
        filho = ProcessoFake(4322, 'worker.exe', r'C:\Games\worker.exe', 1001.0, 4321)
        with patch('app.processos._pids_do_aplicativo', return_value=set()), \
                patch('app.processos.psutil.Process', return_value=raiz), \
                patch('app.processos.psutil.process_iter', return_value=[raiz]), \
                patch('app.processos.subprocess.run') as taskkill:
            status, _, grupo = preparar_grupo_processo(4321, 1000.0, 'game.exe')
            raiz.filhos = [filho]
            status, mensagem = terminar_grupo_processo(grupo)

        self.assertEqual(status, 'GRUPO_ALTERADO')
        self.assertIn("'pid': 4322", str(grupo['processes']))
        self.assertIn('revis', mensagem)
        taskkill.assert_not_called()

    def test_descendente_que_some_nao_esconde_protegido_seguinte(self):
        class ProcessoSumido:
            pid = 4322

            def oneshot(self):
                raise psutil.NoSuchProcess(self.pid)

        raiz = ProcessoFake(4321, 'game.exe', r'C:\Games\game.exe', 1000.0)
        protegido = ProcessoFake(
            4323, 'ollama.exe', r'C:\Ollama\ollama.exe', 1002.0, 4321,
        )
        with patch('app.processos._pids_do_aplicativo', return_value=set()), \
                patch('app.processos.psutil.Process', return_value=raiz), \
                patch('app.processos.psutil.process_iter', return_value=[raiz]), \
                patch('app.processos.subprocess.run') as taskkill:
            status, _, grupo = preparar_grupo_processo(4321, 1000.0, 'game.exe')
            raiz.filhos = [ProcessoSumido(), protegido]
            status, mensagem = terminar_grupo_processo(grupo)

        self.assertEqual(status, 'BLOQUEADO')
        self.assertIn('ollama.exe', mensagem)
        taskkill.assert_not_called()

    def test_revalida_identidade_imediatamente_antes_de_taskkill(self):
        processo = ProcessoFake(4321, 'game.exe', r'C:\Games\game.exe', 1000.0)
        processos = [processo]
        with patch('app.processos._pids_do_aplicativo', return_value=set()), \
                patch('app.processos.psutil.Process', return_value=processo), \
                patch('app.processos.psutil.process_iter', return_value=processos), \
                patch('app.processos.subprocess.run') as taskkill:
            status, _, grupo = preparar_grupo_processo(4321, 1000.0, 'game.exe')
            processo.criado = 2000.0
            status, _ = terminar_grupo_processo(grupo)

        self.assertEqual(status, 'BLOQUEADO')
        taskkill.assert_not_called()

    def test_taskkill_com_sobreviventes_nao_reporta_sucesso(self):
        processo = ProcessoFake(4321, 'game.exe', r'C:\Games\game.exe', 1000.0)
        with patch('app.processos._pids_do_aplicativo', return_value=set()), \
                patch('app.processos.psutil.Process', return_value=processo), \
                patch('app.processos.psutil.process_iter', return_value=[processo]), \
                patch(
                    'app.processos.subprocess.run',
                    return_value=SimpleNamespace(
                        returncode=1, stdout='', stderr='access denied',
                    ),
                ), patch(
                    'app.processos.psutil.wait_procs',
                    return_value=([], [processo]),
                ):
            status, _, grupo = preparar_grupo_processo(4321, 1000.0, 'game.exe')
            status, mensagem = terminar_grupo_processo(grupo)

        self.assertEqual(status, 'FALHA')
        self.assertIn('access denied', mensagem)

    def test_pids_reutilizados_em_raizes_diferentes_nao_ocultam_abate_parcial(self):
        raiz_a = ProcessoFake(4321, 'game.exe', r'C:\Games\game.exe', 1000.0)
        raiz_b = ProcessoFake(4322, 'game.exe', r'C:\Games\game.exe', 1001.0)
        processos = [raiz_a, raiz_b]
        por_pid = {4321: raiz_a, 4322: raiz_b}

        def taskkill(args, **_opcoes):
            raiz_b.criado = 2000.0
            return SimpleNamespace(returncode=0, stdout='', stderr='')

        with patch('app.processos._pids_do_aplicativo', return_value=set()), \
                patch('app.processos.psutil.Process', side_effect=por_pid.__getitem__), \
                patch('app.processos.psutil.process_iter', return_value=processos), \
                patch('app.processos.subprocess.run', side_effect=taskkill) as executar, \
                patch(
                    'app.processos.psutil.wait_procs',
                    return_value=([], [raiz_b]),
                ):
            status, mensagem, grupo = preparar_grupo_processo(4321, 1000.0, 'game.exe')
            status, mensagem = terminar_grupo_processo(grupo)

        self.assertEqual(status, 'FALHA')
        self.assertEqual(executar.call_count, 1)
        self.assertIn('4322', mensagem)

    def test_identidade_alterada_na_segunda_raiz_nao_vira_sucesso(self):
        raiz_a = ProcessoFake(4321, 'game.exe', r'C:\Games\game.exe', 1000.0)
        raiz_b = ProcessoFake(4322, 'game.exe', r'C:\Games\game.exe', 1001.0)
        processos = [raiz_a, raiz_b]
        por_pid = {4321: raiz_a, 4322: raiz_b}

        def taskkill(args, **_opcoes):
            raiz_b.criado = 2000.0
            return SimpleNamespace(returncode=0, stdout='', stderr='')

        with patch('app.processos._pids_do_aplicativo', return_value=set()), \
                patch('app.processos.psutil.Process', side_effect=por_pid.__getitem__), \
                patch('app.processos.psutil.process_iter', return_value=processos), \
                patch('app.processos.subprocess.run', side_effect=taskkill) as executar, \
                patch(
                    'app.processos.psutil.wait_procs',
                    side_effect=lambda vivos, timeout: (vivos, []),
                ):
            _, _, grupo = preparar_grupo_processo(4321, 1000.0, 'game.exe')
            status, mensagem = terminar_grupo_processo(grupo)

        self.assertEqual(status, 'FALHA')
        self.assertEqual(executar.call_count, 1)
        self.assertIn('identidade', mensagem)

    def test_timeout_de_taskkill_nao_reporta_sucesso(self):
        processo = ProcessoFake(4321, 'game.exe', r'C:\Games\game.exe', 1000.0)
        with patch('app.processos._pids_do_aplicativo', return_value=set()), \
                patch('app.processos.psutil.Process', return_value=processo), \
                patch('app.processos.psutil.process_iter', return_value=[processo]), \
                patch(
                    'app.processos.subprocess.run',
                    side_effect=subprocess.TimeoutExpired('taskkill', 10),
                ), patch(
                    'app.processos.psutil.wait_procs',
                    return_value=([], [processo]),
                ):
            status, _, grupo = preparar_grupo_processo(4321, 1000.0, 'game.exe')
            status, _ = terminar_grupo_processo(grupo)

        self.assertEqual(status, 'FALHA')

    def test_bloqueia_arvore_que_inclui_processo_protegido(self):
        raiz = ProcessoFake(4321, 'launcher.exe', r'C:\Games\launcher.exe', 1000.0)
        filho = ProcessoFake(4322, 'ollama.exe', r'C:\Ollama\ollama.exe', 1001.0, 4321)
        raiz.filhos = [filho]
        with patch('app.processos._pids_do_aplicativo', return_value=set()), \
                patch('app.processos.psutil.Process', return_value=raiz), \
                patch('app.processos.psutil.process_iter', return_value=[raiz, filho]), \
                patch('app.processos.subprocess.run') as taskkill:
            status, _, _ = preparar_grupo_processo(4321, 1000.0, 'launcher.exe')

        self.assertEqual(status, 'BLOQUEADO')
        taskkill.assert_not_called()

    def test_bloqueia_grupo_que_inclui_processo_do_proprio_app(self):
        raiz = ProcessoFake(4321, 'game.exe', r'C:\Games\game.exe', 1000.0)
        app_filho = ProcessoFake(4322, 'cyber-sentinel.exe', r'C:\App\cyber-sentinel.exe', 1001.0, 4321)
        raiz.filhos = [app_filho]
        with patch('app.processos._pids_do_aplicativo', return_value={4322}), \
                patch('app.processos.psutil.Process', return_value=raiz), \
                patch('app.processos.psutil.process_iter', return_value=[raiz, app_filho]), \
                patch('app.processos.subprocess.run') as taskkill:
            status, _, _ = preparar_grupo_processo(4321, 1000.0, 'game.exe')

        self.assertEqual(status, 'BLOQUEADO')
        taskkill.assert_not_called()

    def test_caminho_desconhecido_com_mesmo_nome_falha_fechado(self):
        raiz = ProcessoFake(4321, 'game.exe', r'C:\Games\game.exe', 1000.0)
        outro = ProcessoFake(4322, 'game.exe', None, 1001.0)
        with patch('app.processos._pids_do_aplicativo', return_value=set()), \
                patch('app.processos.psutil.Process', return_value=raiz), \
                patch('app.processos.psutil.process_iter', return_value=[raiz, outro]), \
                patch('app.processos.subprocess.run') as taskkill:
            status, _, _ = preparar_grupo_processo(4321, 1000.0, 'game.exe')

        self.assertEqual(status, 'FALHA')
        taskkill.assert_not_called()

    def test_falha_ao_validar_arvore_do_app_bloqueia_encerramento(self):
        with patch(
            'app.processos._pids_do_aplicativo',
            side_effect=psutil.AccessDenied(pid=4321),
        ), patch('app.processos.psutil.Process') as criar_processo:
            status, _, _ = preparar_grupo_processo(4321, 1000.0, 'game.exe')

        self.assertEqual(status, 'FALHA')
        criar_processo.assert_not_called()

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
        app._historico_acoes = None
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
    def test_json_valido_sem_objeto_vai_para_dlq(self):
        class Canal:
            def __init__(self):
                self.nacks = []

            def basic_nack(self, delivery_tag, requeue):
                self.nacks.append((delivery_tag, requeue))

        canal = Canal()
        with tempfile.TemporaryDirectory() as pasta, patch.dict(
            os.environ,
            {'SENTINEL_DEBUG_LOG': os.path.join(pasta, 'debug.txt')},
            clear=True,
        ):
            for tag, corpo in ((10, b'null'), (11, b'[]')):
                _processar_mensagem(
                    SimpleNamespace(),
                    canal,
                    SimpleNamespace(delivery_tag=tag),
                    None,
                    corpo,
                )

        self.assertEqual(canal.nacks, [(10, False), (11, False)])

    def test_consumidor_reconhece_pai_encerrado(self):
        with patch('consumer.main.psutil.pid_exists', return_value=False):
            self.assertFalse(consumer_main._pai_ativo(1234))
        with patch('consumer.main.psutil.pid_exists', return_value=True):
            self.assertTrue(consumer_main._pai_ativo(1234))

    def test_ignora_evento_de_analise_marcado_como_nao_relevante(self):
        class Canal:
            def __init__(self):
                self.acks = []

            def basic_ack(self, delivery_tag):
                self.acks.append(delivery_tag)

        evento = _evento('analise')
        evento['state_relevant'] = False
        canal = Canal()
        store = SimpleNamespace(
            processar=lambda _: self.fail('Análise não deve alterar o estado')
        )
        with tempfile.TemporaryDirectory() as pasta, patch.dict(
            os.environ,
            {'SENTINEL_DEBUG_LOG': os.path.join(pasta, 'debug.txt')},
        ):
            _processar_mensagem(
                store, canal, SimpleNamespace(delivery_tag=8), None,
                json.dumps(evento).encode('utf-8'),
            )

        self.assertEqual(canal.acks, [8])

    def test_descarta_evento_de_sessao_anterior(self):
        class Canal:
            def __init__(self):
                self.acks = []
                self.nacks = []

            def basic_ack(self, delivery_tag):
                self.acks.append(delivery_tag)

            def basic_nack(self, delivery_tag, requeue):
                self.nacks.append((delivery_tag, requeue))

        evento = _evento('sessao-antiga')
        evento['session_id'] = 'sessao-anterior'
        canal = Canal()
        store = SimpleNamespace(
            processar=lambda _: self.fail('Evento antigo não deve ser processado')
        )
        with tempfile.TemporaryDirectory() as pasta, patch.dict(
            os.environ,
            {
                'SENTINEL_SESSION_ID': 'sessao-atual',
                'SENTINEL_DEBUG_LOG': os.path.join(pasta, 'debug.txt'),
            },
        ):
            _processar_mensagem(
                store, canal, SimpleNamespace(delivery_tag=7), None,
                json.dumps(evento).encode('utf-8'),
            )

        self.assertEqual(canal.acks, [7])
        self.assertEqual(canal.nacks, [])

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
            for indice in range(2):
                evento = _evento(f'consumer-{indice}', cpu=90)
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

            self.assertEqual(canal.acks, [0, 1])
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
            for indice in range(2):
                evento = _evento(f'critical-{indice}', cpu=95)
                evento['processes'] = [{
                    'pid': 4321,
                    'name': 'game.exe',
                    'create_time': 1234.5,
                    'cpu_percent': 8,
                    'ram_percent': 10,
                    'rss_bytes': 400000000,
                }]
                store.processar(evento)

            with patch('consumer.main.grpc.insecure_channel', return_value=CanalFake()), \
                    patch('consumer.main.pb2_grpc.SystemMonitorStub', StubFake):
                consumer_main._loop_notificacoes(store, encerrar)

            self.assertEqual(len(enviados), 1)
            self.assertEqual(enviados[0].level, 'CRITICAL')
            self.assertEqual(enviados[0].alert_id, 'critical-1')
            self.assertTrue(enviados[0].HasField('process'))
            self.assertEqual(enviados[0].process.pid, 4321)
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
