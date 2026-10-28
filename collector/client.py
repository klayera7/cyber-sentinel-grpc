import grpc
import time
import psutil
from datetime import datetime
import sys
import os

caminho_proto = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'proto'))
sys.path.append(caminho_proto)

import system_monitor_pb2 as pb2
import system_monitor_pb2_grpc as pb2_grpc

def generate_telemetry():
    while True:
        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory().percent
        agora = datetime.now().strftime("%H:%M:%S")

        print(f"[Coletor] A enviar -> CPU: {cpu}% | RAM: {ram}%")

        yield pb2.TelemetryData(cpu_percent=cpu, ram_percent=ram, timestamp=agora)

        time.sleep(3)

def run():
    print("A iniciar o Coletor...")

    with grpc.insecure_channel('localhost:50051') as channel:

        stub = pb2_grpc.SystemMonitorStub(channel)

        print("A iniciar envio do fluxo de telemetria...")
        try:
            respostas_do_servidor = stub.StreamTelemetry(generate_telemetry())

            for resposta in respostas_do_servidor:
                print(f"[Servidor/IA devolveu]: {resposta.diagnosis}")

        except KeyboardInterrupt:
            print("\nColetor desligado pelo utilizador.")
        except grpc.RpcError as e:
            print(f"\n[Erro] Não foi possível ligar ao Servidor. Detalhe: {e.details()}")

if __name__ == '__main__':
    run()
