import grpc
from concurrent import futures
import sys
import os
import ollama

caminho_proto = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'proto'))
sys.path.append(caminho_proto)

import system_monitor_pb2 as pb2
import system_monitor_pb2_grpc as pb2_grpc

MODELO = os.getenv('OLLAMA_MODEL', 'llama3.1')

class ServidorMonitorizacao(pb2_grpc.SystemMonitorServicer):
    
    def StreamTelemetry(self, request_iterator, context):
        print("[Servidor] Ligação de streaming estabelecida com um Coletor!")
        
        for dados in request_iterator:
            print(f"-> Recebido: CPU {dados.cpu_percent}% | RAM {dados.ram_percent}%")
            
            prompt = f"Analise de forma muito breve e direta o estado de um sistema com {dados.cpu_percent}% de CPU e {dados.ram_percent}% de RAM."
            
            resposta_ia = ollama.generate(model=MODELO, prompt=prompt)
            diagnostico = resposta_ia['response']
            
            print(f"<- IA respondeu: {diagnostico}")
            print("-" * 40)
            
            yield pb2.AnalysisResponse(diagnosis=diagnostico)

def iniciar_servidor():
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    pb2_grpc.add_SystemMonitorServicer_to_server(ServidorMonitorizacao(), servidor)
    
    servidor.add_insecure_port('[::]:50051')
    servidor.start()
    
    print("Servidor gRPC a correr na porta 50051...")
    print("A aguardar dados do Coletor...\n")

    servidor.wait_for_termination()

if __name__ == '__main__':
    iniciar_servidor()