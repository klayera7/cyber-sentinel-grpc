import os
import threading
import time
from collections import OrderedDict


def _opcional(dados, campo):
    return getattr(dados, campo) if dados.HasField(campo) else None


def _faixa(valor, tamanho):
    if valor is None:
        return None
    return int(valor // tamanho) * tamanho


def chave_estado(dados):
    temperatura = _opcional(dados, 'temp_celsius')
    if temperatura is not None:
        temperatura = int((temperatura + 2.5) // 5) * 5

    return (
        _faixa(dados.cpu_percent, 10),
        _faixa(dados.ram_percent, 10),
        _faixa(_opcional(dados, 'gpu_percent'), 10),
        _faixa(_opcional(dados, 'gpu_mem_percent'), 10),
        temperatura,
    )


def limites_configurados():
    try:
        cpu = float(os.getenv('SENTINEL_CPU_LIMIT', '85'))
        ram = float(os.getenv('SENTINEL_RAM_LIMIT', '85'))
    except (TypeError, ValueError):
        return 85.0, 85.0
    return cpu, ram


def estado_interessante(dados, cpu_limit=None, ram_limit=None):
    if cpu_limit is None or ram_limit is None:
        cpu_limit, ram_limit = limites_configurados()
    return (
        dados.cpu_percent >= max(0.0, cpu_limit - 15.0)
        or dados.ram_percent >= max(0.0, ram_limit - 5.0)
        or (_opcional(dados, 'gpu_percent') or 0) >= 70
        or (_opcional(dados, 'gpu_mem_percent') or 0) >= 80
        or (_opcional(dados, 'temp_celsius') or 0) >= 75
    )


def diagnostico_rapido(dados, cpu_limit=None, ram_limit=None):
    if cpu_limit is None or ram_limit is None:
        cpu_limit, ram_limit = limites_configurados()
    alertas = []
    temperatura = _opcional(dados, 'temp_celsius')
    gpu = _opcional(dados, 'gpu_percent')
    memoria_gpu = _opcional(dados, 'gpu_mem_percent')

    if temperatura is not None:
        if temperatura >= 90:
            alertas.append(
                f'Temperatura da GPU crítica ({temperatura:.0f} °C), com risco elevado de throttling.'
            )
        elif temperatura >= 75:
            alertas.append(
                f'Temperatura da GPU alta ({temperatura:.0f} °C); monitore a refrigeração.'
            )

    if dados.cpu_percent >= cpu_limit:
        alertas.append(f'CPU em nível crítico ({dados.cpu_percent:.0f}%).')
    elif dados.cpu_percent >= max(0.0, cpu_limit - 15.0):
        alertas.append(f'CPU com carga elevada ({dados.cpu_percent:.0f}%).')

    if dados.ram_percent >= ram_limit:
        alertas.append(f'RAM quase esgotada ({dados.ram_percent:.0f}%).')
    elif dados.ram_percent >= max(0.0, ram_limit - 5.0):
        alertas.append(f'Uso de RAM elevado ({dados.ram_percent:.0f}%).')

    if gpu is not None:
        if gpu >= 90:
            alertas.append(f'GPU em nível crítico de uso ({gpu:.0f}%).')
        elif gpu >= 70:
            alertas.append(f'GPU com carga elevada ({gpu:.0f}%).')

    if memoria_gpu is not None:
        if memoria_gpu >= 90:
            alertas.append(f'VRAM quase esgotada ({memoria_gpu:.0f}%).')
        elif memoria_gpu >= 80:
            alertas.append(f'Uso de VRAM elevado ({memoria_gpu:.0f}%).')

    if alertas:
        return ' '.join(alertas)
    if gpu is None:
        return 'Sistema estável: CPU e RAM em níveis normais; telemetria da GPU indisponível.'
    return 'Sistema estável: uso de CPU, RAM e GPU dentro dos níveis normais.'


class CacheDiagnostico:
    def __init__(self, max_entradas=100, ttl=600.0, cooldown=4.0, relogio=None):
        self.max_entradas = max_entradas
        self.ttl = ttl
        self.cooldown = cooldown
        self._relogio = relogio or time.monotonic
        self._itens = OrderedDict()
        self._lock = threading.Lock()
        self._ultima_chamada = None
        self._llm_em_andamento = False

    def _remover_expirados(self, agora):
        expiradas = [
            chave for chave, (criado, _) in self._itens.items()
            if agora - criado >= self.ttl
        ]
        for chave in expiradas:
            del self._itens[chave]

    def obter(self, chave):
        agora = self._relogio()
        with self._lock:
            self._remover_expirados(agora)
            item = self._itens.get(chave)
            if item is None:
                return None
            self._itens.move_to_end(chave)
            return item[1]

    def armazenar(self, chave, texto):
        if self.max_entradas <= 0 or self.ttl <= 0:
            return
        agora = self._relogio()
        with self._lock:
            self._remover_expirados(agora)
            self._itens[chave] = (agora, texto)
            self._itens.move_to_end(chave)
            while len(self._itens) > self.max_entradas:
                self._itens.popitem(last=False)

    def iniciar_chamada_llm(self):
        agora = self._relogio()
        with self._lock:
            if self._llm_em_andamento:
                return False
            if (
                self._ultima_chamada is not None
                and agora - self._ultima_chamada < self.cooldown
            ):
                return False
            self._ultima_chamada = agora
            self._llm_em_andamento = True
            return True

    def finalizar_chamada_llm(self):
        with self._lock:
            self._llm_em_andamento = False
