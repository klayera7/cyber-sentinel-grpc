import os
import sys
from concurrent import futures

import grpc
import ollama

caminho_proto = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'proto'))
if caminho_proto not in sys.path:
    sys.path.insert(0, caminho_proto)

import system_monitor_pb2 as pb2
import system_monitor_pb2_grpc as pb2_grpc


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

    def StreamTelemetry(self, request_iterator, context):
        modelo = os.getenv('OLLAMA_MODEL', 'llama3.1')
        cliente = ollama.Client(host=_host_ollama())
        print('[Servidor] Ligação de streaming estabelecida com um Coletor!')

        for dados in request_iterator:
            print(f'-> Recebido: CPU {dados.cpu_percent}% | RAM {dados.ram_percent}%')

            prompt = (
                f'Analise de forma muito breve e direta o estado de um sistema '
                f'com {dados.cpu_percent}% de CPU e {dados.ram_percent}% de RAM.'
            )

            try:
                resposta_ia = cliente.generate(model=modelo, prompt=prompt)
                diagnostico = _extrair_texto(resposta_ia)
            except Exception as e:
                diagnostico = f'[Erro ao consultar a IA: {e}]'

            print(f'<- IA respondeu: {diagnostico}')
            print('-' * 40)

            yield pb2.AnalysisResponse(diagnosis=diagnostico)


def criar_servidor(porta: int = 50051) -> grpc.Server:
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_SystemMonitorServicer_to_server(ServidorMonitorizacao(), servidor)
    servidor.add_insecure_port(f'[::]:{porta}')
    servidor.start()
    print(f'Servidor gRPC a correr na porta {porta}...')
    return servidor


def iniciar_servidor():
    porta = int(os.getenv('GRPC_PORT', '50051'))
    servidor = criar_servidor(porta)
    print('A aguardar dados do Coletor...\n')
    servidor.wait_for_termination()


if __name__ == '__main__':
    iniciar_servidor()
