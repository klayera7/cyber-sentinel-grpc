import queue
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime

import grpc
import psutil

import system_monitor_pb2 as pb2
import system_monitor_pb2_grpc as pb2_grpc

from app.sensores import Sensores, temperatura_sistema


@dataclass
class Amostra:
    ts: str
    cpu: float
    ram: float
    gpu: float = None
    gpu_mem: float = None
    temp: float = None


class Coletor:
    def __init__(self, intervalo=2.0, on_erro=None):
        self.intervalo = intervalo
        self.on_erro = on_erro

        self.historico = deque(maxlen=600)
        self.fila_diag = queue.Queue()
        self._fila_envio = queue.Queue(maxsize=1)
        self._sensores = Sensores()

        self._ativo = False
        self._channel = None
        self._thread_metricas = None
        self._thread_grpc = None

    def _loop_metricas(self):
        psutil.cpu_percent(None)
        while self._ativo:
            cpu = psutil.cpu_percent(interval=1)
            ram = psutil.virtual_memory().percent
            agora = datetime.now().strftime('%H:%M:%S')

            leitura = self._sensores.ler_gpu()
            amostra = Amostra(
                ts=agora,
                cpu=cpu,
                ram=ram,
                gpu=leitura.uso,
                gpu_mem=leitura.memoria,
                temp=temperatura_sistema(leitura.temperatura),
            )

            self.historico.append(amostra)

            try:
                self._fila_envio.get_nowait()
            except queue.Empty:
                pass
            self._fila_envio.put(amostra)

            time.sleep(max(0.0, self.intervalo - 1.0))

    def _gerar_telemetria(self):
        while self._ativo:
            try:
                amostra = self._fila_envio.get(timeout=1.0)
            except queue.Empty:
                continue

            dados = pb2.TelemetryData(
                cpu_percent=amostra.cpu,
                ram_percent=amostra.ram,
                timestamp=amostra.ts,
            )
            if amostra.temp is not None:
                dados.temp_celsius = amostra.temp
            if amostra.gpu is not None:
                dados.gpu_percent = amostra.gpu
            if amostra.gpu_mem is not None:
                dados.gpu_mem_percent = amostra.gpu_mem

            yield dados

    def _loop_grpc(self, porta):
        while self._ativo:
            canal = grpc.insecure_channel(f'localhost:{porta}')
            self._channel = canal
            try:
                stub = pb2_grpc.SystemMonitorStub(canal)
                respostas = stub.StreamTelemetry(self._gerar_telemetria())
                for resposta in respostas:
                    if not self._ativo:
                        break
                    agora = datetime.now().strftime('%H:%M:%S')
                    self.fila_diag.put((agora, resposta.diagnosis))
            except grpc.RpcError as e:
                self._reportar_erro(f'Erro gRPC: {e.details() or e}')
            except Exception as e:
                self._reportar_erro(f'Erro no coletor: {e}')
            finally:
                try:
                    canal.close()
                except Exception:
                    pass
                self._channel = None
            time.sleep(1.0)

    def _reportar_erro(self, msg):
        if self.on_erro:
            self.on_erro(msg)

    def iniciar(self, porta=50051):
        if self._ativo:
            return
        self._ativo = True
        self._thread_metricas = threading.Thread(target=self._loop_metricas, daemon=True)
        self._thread_grpc = threading.Thread(target=self._loop_grpc, args=(porta,), daemon=True)
        self._thread_metricas.start()
        self._thread_grpc.start()

    def parar(self):
        self._ativo = False
        if self._channel is not None:
            try:
                self._channel.close()
            except Exception:
                pass
            self._channel = None

    def ultima(self):
        return self.historico[-1] if self.historico else None

    def consumir_diagnosticos(self):
        itens = []
        while True:
            try:
                itens.append(self.fila_diag.get_nowait())
            except queue.Empty:
                break
        return itens
