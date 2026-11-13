import queue
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime

import grpc
import psutil

import system_monitor_pb2 as pb2
import system_monitor_pb2_grpc as pb2_grpc

from app.sensores import Sensores, temperatura_sistema
from app.processos import coletar_processos
from messaging import debug_log


@dataclass
class Amostra:
    ts: str
    cpu: float
    ram: float
    gpu: float = None
    gpu_mem: float = None
    temp: float = None
    processos: list = field(default_factory=list)
    inicio_monotonic: float = None


class Coletor:
    def __init__(self, intervalo=2.0, on_erro=None, ao_amostra=None):
        self.intervalo = intervalo
        self.on_erro = on_erro
        self.ao_amostra = ao_amostra

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
            inicio_amostra = time.monotonic()
            cpu = psutil.cpu_percent(interval=1)
            if not self._ativo:
                break
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
                processos=coletar_processos(),
                inicio_monotonic=inicio_amostra,
            )
            debug_log.registrar(
                'coletor',
                'amostra',
                timestamp=agora,
                cpu_percent=cpu,
                ram_percent=ram,
                gpu_percent=amostra.gpu,
                gpu_mem_percent=amostra.gpu_mem,
                gpu_temp_celsius=amostra.temp,
                top_processes=[
                    {
                        chave: valor for chave, valor in processo.items()
                        if chave != 'exe_path'
                    }
                    for processo in amostra.processos
                ],
            )

            self.historico.append(amostra)

            if self.ao_amostra is not None:
                try:
                    self.ao_amostra(self._dados_telemetria(amostra))
                except Exception as erro:
                    self._reportar_erro(f'Erro ao avaliar limites: {erro}')

            try:
                self._fila_envio.get_nowait()
            except queue.Empty:
                pass
            self._fila_envio.put(amostra)

            time.sleep(max(0.0, self.intervalo - 1.0))

    @staticmethod
    def _dados_telemetria(amostra):
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
        for processo in amostra.processos:
            dados.top_processes.add(
                pid=processo['pid'],
                name=processo['name'],
                create_time=processo['create_time'],
                cpu_percent=processo['cpu_percent'],
                ram_percent=processo['ram_percent'],
                rss_bytes=processo['rss_bytes'],
            )
        return dados

    def _gerar_telemetria(self):
        while self._ativo:
            try:
                amostra = self._fila_envio.get(timeout=1.0)
            except queue.Empty:
                continue
            yield self._dados_telemetria(amostra)

    def _loop_grpc(self, porta):
        while self._ativo:
            canal = grpc.insecure_channel(f'127.0.0.1:{porta}')
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
        for thread in (self._thread_metricas, self._thread_grpc):
            if (
                thread is not None
                and thread.ident is not None
                and thread is not threading.current_thread()
            ):
                thread.join(timeout=5.0)

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
