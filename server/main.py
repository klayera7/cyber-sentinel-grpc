import os
import sys
import threading
import uuid
from collections import OrderedDict
from concurrent import futures
from datetime import datetime, timezone

import grpc
import ollama

caminho_proto = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'proto'))
if caminho_proto not in sys.path:
    sys.path.insert(0, caminho_proto)
caminho_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if caminho_raiz not in sys.path:
    sys.path.insert(0, caminho_raiz)

import system_monitor_pb2 as pb2
import system_monitor_pb2_grpc as pb2_grpc
from messaging.eventos import criar_evento
from server.diagnostico import (
    CacheDiagnostico,
    chave_estado,
    diagnostico_rapido,
    estado_interessante,
)
from server.rabbit_publisher import PublicadorRabbitMQ


def _host_ollama():
    host = os.getenv('OLLAMA_HOST', 'http://127.0.0.1:11434')
    if not host.startswith(('http://', 'https://')):
        host = 'http://' + host
    return host


def _extrair_texto(resposta):
    if isinstance(resposta, dict):
        return resposta.get('response', str(resposta))
    return getattr(resposta, 'response', str(resposta))


class ServidorMonitorizacao(pb2_grpc.SystemMonitorServicer):
    def __init__(self, publicador=None, ao_log=None, ao_alerta=None):
        self.cache_diagnosticos = CacheDiagnostico()
        self.ao_log = ao_log
        self.ao_alerta = ao_alerta
        self.publicador = publicador or PublicadorRabbitMQ(ao_log=ao_log)
        self._alertas_lock = threading.Lock()
        self._alertas_vistos = OrderedDict()

    def iniciar_publicador(self):
        self.publicador.iniciar()

    def fechar(self):
        self.publicador.fechar(timeout=3.0)

    def _log(self, mensagem):
        if self.ao_log:
            self.ao_log(mensagem)
        else:
            print(mensagem)

    def SendAlert(self, request, context):
        mensagem = (request.log_message or '').strip()
        nivel = (request.level or '').strip().upper()
        aliases = {
            'WARN': 'WARNING',
            'ATENCAO': 'WARNING',
            'ATENÇÃO': 'WARNING',
            'CRITICO': 'CRITICAL',
            'CRÍTICO': 'CRITICAL',
        }
        nivel = aliases.get(nivel, nivel)

        if not mensagem:
            erro = 'A mensagem do alerta não pode estar vazia.'
            if context is not None:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, erro)
            return pb2.AlertAck(accepted=False, error=erro)
        if nivel not in {'INFO', 'WARNING', 'CRITICAL'}:
            erro = 'level precisa ser INFO, WARNING ou CRITICAL.'
            if context is not None:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, erro)
            return pb2.AlertAck(accepted=False, error=erro)

        alert_id = (request.alert_id or '').strip() or str(uuid.uuid4())
        alerta = {
            'alert_id': alert_id,
            'level': nivel,
            'message': mensagem[:2000],
            'host_id': (request.host_id or '').strip() or 'desconhecido',
            'source': (request.source or '').strip() or 'externo',
            'received_at': datetime.now(timezone.utc).isoformat(
                timespec='seconds'
            ).replace('+00:00', 'Z'),
        }

        with self._alertas_lock:
            if alert_id in self._alertas_vistos:
                self._alertas_vistos.move_to_end(alert_id)
                return pb2.AlertAck(accepted=True, alert_id=alert_id)
            self._alertas_vistos[alert_id] = None
            while len(self._alertas_vistos) > 1000:
                self._alertas_vistos.popitem(last=False)
            try:
                if self.ao_alerta:
                    self.ao_alerta(alerta)
            except Exception as erro:
                self._alertas_vistos.pop(alert_id, None)
                self._log(f'[Notificações] Não foi possível entregar alerta: {erro}')
                return pb2.AlertAck(
                    accepted=False,
                    alert_id=alert_id,
                    error='A interface não recebeu a notificação.',
                )

        self._log(f"[Notificação] {nivel} de {alerta['host_id']}: {mensagem[:120]}")
        return pb2.AlertAck(accepted=True, alert_id=alert_id)

    def StreamTelemetry(self, request_iterator, context):
        modelo = os.getenv('OLLAMA_MODEL', 'llama3.1')
        cliente = None
        print('[Servidor] Ligação de streaming estabelecida com um Coletor!')

        for dados in request_iterator:
            chave = chave_estado(dados)
            interessante = estado_interessante(dados)
            diagnostico = (
                self.cache_diagnosticos.obter(chave) if interessante else None
            )
            origem = 'cache' if diagnostico is not None else 'regra'

            if diagnostico is None:
                chamar_ia = (
                    interessante
                    and self.cache_diagnosticos.iniciar_chamada_llm()
                )
                if chamar_ia:
                    extras = []
                    if dados.HasField('gpu_percent'):
                        texto_gpu = f'GPU em {dados.gpu_percent:.1f}% de uso'
                        if dados.HasField('gpu_mem_percent'):
                            texto_gpu += f' ({dados.gpu_mem_percent:.1f}% da VRAM)'
                        extras.append(texto_gpu)
                    if dados.HasField('temp_celsius'):
                        extras.append(
                            f'temperatura da GPU em {dados.temp_celsius:.1f} °C'
                        )

                    detalhe = ''
                    if extras:
                        detalhe = ', ' + ' e '.join(extras)
                    prompt = (
                        f'Analise de forma muito breve e direta o estado de um sistema '
                        f'com {dados.cpu_percent:.1f}% de CPU e '
                        f'{dados.ram_percent:.1f}% de RAM{detalhe}.'
                    )
                    if dados.HasField('temp_celsius'):
                        prompt += ' Comente o risco de throttling se a temperatura estiver alta.'

                    try:
                        if cliente is None:
                            cliente = ollama.Client(host=_host_ollama())
                        resposta_ia = cliente.generate(model=modelo, prompt=prompt)
                        texto_ia = str(_extrair_texto(resposta_ia)).strip()
                        if texto_ia:
                            diagnostico = texto_ia
                            self.cache_diagnosticos.armazenar(chave, diagnostico)
                            origem = 'IA'
                        else:
                            diagnostico = diagnostico_rapido(dados)
                    except Exception as e:
                        print(f'[Servidor] Falha na consulta à IA: {e}')
                        diagnostico = diagnostico_rapido(dados)
                    finally:
                        self.cache_diagnosticos.finalizar_chamada_llm()
                else:
                    diagnostico = diagnostico_rapido(dados)

            resumo = f'CPU {dados.cpu_percent:.0f}% | RAM {dados.ram_percent:.0f}%'
            if dados.HasField('gpu_percent'):
                resumo += f' | GPU {dados.gpu_percent:.0f}%'
            if dados.HasField('temp_celsius'):
                resumo += f' | GPU temp {dados.temp_celsius:.0f}°C'
            print(f'-> Recebido: {resumo}')
            print(f'<- [{origem}] {diagnostico}')
            print('-' * 40)

            try:
                self.publicador.publicar(criar_evento(dados, diagnostico, origem))
            except Exception as e:
                self._log(f'[RabbitMQ] Não foi possível gravar evento no outbox: {e}')

            yield pb2.AnalysisResponse(diagnosis=diagnostico)


def criar_servidor(porta: int = 50051, servicer=None) -> grpc.Server:
    servicer = servicer or ServidorMonitorizacao()
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_SystemMonitorServicer_to_server(servicer, servidor)
    servidor.add_insecure_port(f'[::]:{porta}')
    try:
        servidor.start()
        servidor._sentinel_servicer = servicer
        servicer.iniciar_publicador()
    except Exception:
        try:
            servidor.stop(grace=0)
        except Exception:
            pass
        servicer.fechar()
        raise
    print(f'Servidor gRPC a correr na porta {porta}...')
    return servidor


def parar_servidor(servidor, grace=1.0):
    finalizado = servidor.stop(grace)
    try:
        finalizado.wait(timeout=grace + 1.0)
    finally:
        servicer = getattr(servidor, '_sentinel_servicer', None)
        if servicer is not None:
            servicer.fechar()
    return finalizado


def iniciar_servidor():
    porta = int(os.getenv('GRPC_PORT', '50051'))
    servidor = criar_servidor(porta)
    print('A aguardar dados do Coletor...\n')
    try:
        servidor.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        parar_servidor(servidor)


if __name__ == '__main__':
    iniciar_servidor()
