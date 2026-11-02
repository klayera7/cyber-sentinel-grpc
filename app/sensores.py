import shutil
import subprocess

_SEM_JANELA = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
_CAMPOS = 'temperature.gpu,utilization.gpu,memory.used,memory.total'


class LeituraGPU:
    __slots__ = ('uso', 'memoria', 'temperatura')

    def __init__(self, uso=None, memoria=None, temperatura=None):
        self.uso = uso
        self.memoria = memoria
        self.temperatura = temperatura


def _numero(texto):
    partes = (texto or '').strip().split()
    if not partes:
        return None
    bruto = partes[0]
    if bruto.upper().startswith('[N/A') or bruto.upper() == 'N/A':
        return None
    try:
        return float(bruto)
    except ValueError:
        return None


def _normalizar_percentual(valor):
    if valor is None:
        return None
    return max(0.0, min(100.0, valor))


class Sensores:
    def __init__(self):
        self._exe = shutil.which('nvidia-smi')

    def ler_gpu(self):
        if not self._exe:
            return LeituraGPU()
        try:
            resultado = subprocess.run(
                [self._exe, f'--query-gpu={_CAMPOS}', '--format=csv,noheader,nounits'],
                capture_output=True, text=True, timeout=3, creationflags=_SEM_JANELA,
            )
        except Exception:
            self._exe = None
            return LeituraGPU()
        if resultado.returncode != 0:
            return LeituraGPU()

        linhas = [linha for linha in (resultado.stdout or '').splitlines() if linha.strip()]
        if not linhas:
            return LeituraGPU()

        campos = [campo.strip() for campo in linhas[0].split(',')]
        if len(campos) < 4:
            return LeituraGPU()

        temperatura, uso, usado, total = (_numero(campo) for campo in campos[:4])
        memoria = None
        if usado is not None and total:
            memoria = _normalizar_percentual(usado / total * 100.0)

        return LeituraGPU(_normalizar_percentual(uso), memoria, temperatura)


def temperatura_sistema(*leituras):
    valores = [valor for valor in leituras if valor is not None]
    if not valores:
        return None
    return max(valores)
