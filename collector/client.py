import grpc
import time
import psutil
from datetime import datetime
import sys
import os

caminho_proto = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'proto'))
sys.path.append(caminho_proto)
caminho_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, caminho_raiz)

import system_monitor_pb2 as pb2
import system_monitor_pb2_grpc as pb2_grpc
from app.processos import coletar_processos
from messaging import debug_log

def generate_telemetry():
    while True:
        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory().percent
        agora = datetime.now().strftime("%H:%M:%S")
        processos = coletar_processos()

        debug_log.registrar(
            'collector_standalone', 'amostra', timestamp=agora,
            cpu_percent=cpu,
            ram_percent=ram,
            top_processes=[
                {
                    chave: valor for chave, valor in processo.items()
                    if chave != 'exe_path'
                }
                for processo in processos
            ],
        )

        print(f"[Coletor] A enviar -> CPU: {cpu}% | RAM: {ram}%")

        dados = pb2.TelemetryData(cpu_percent=cpu, ram_percent=ram, timestamp=agora)
        for processo in processos:
            dados.top_processes.add(
                pid=processo['pid'],
                name=processo['name'],
                create_time=processo['create_time'],
                cpu_percent=processo['cpu_percent'],
                ram_percent=processo['ram_percent'],
                rss_bytes=processo['rss_bytes'],
            )
        yield dados

        time.sleep(3)

def run():
    print("A iniciar o Coletor...")

    with grpc.insecure_channel('127.0.0.1:50051') as channel:

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
