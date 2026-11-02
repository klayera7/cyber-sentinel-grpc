import os
import re
import socket
import subprocess
import sys
import time
from urllib.parse import urlsplit

from messaging.config import RABBITMQ_URL


_SEM_JANELA = getattr(subprocess, 'CREATE_NO_WINDOW', 0)


def _endpoint(url):
    analisada = urlsplit(url)
    if analisada.scheme not in ('amqp', 'amqps') or not analisada.hostname:
        raise ValueError('RABBITMQ_URL precisa ser uma URL amqp:// ou amqps://.')
    porta = analisada.port or (5671 if analisada.scheme == 'amqps' else 5672)
    return analisada.hostname, porta


def _porta_aberta(host, porta):
    try:
        with socket.create_connection((host, porta), timeout=0.5):
            return True
    except OSError:
        return False


def _consultar_servico(nome):
    try:
        resultado = subprocess.run(
            ['sc.exe', 'query', nome],
            capture_output=True,
            text=True,
            timeout=5,
            creationflags=_SEM_JANELA,
        )
    except Exception as erro:
        return None, str(erro)

    if resultado.returncode != 0:
        detalhe = (resultado.stderr or resultado.stdout or '').strip()
        return None, detalhe or f'Não foi possível consultar o serviço {nome}.'

    estado = re.search(r'\bSTATE\s*:\s*(\d+)', resultado.stdout, re.IGNORECASE)
    if not estado:
        return None, f'Não foi possível ler o estado do serviço {nome}.'
    return int(estado.group(1)), None


def _iniciar_servico(nome):
    try:
        resultado = subprocess.run(
            ['sc.exe', 'start', nome],
            capture_output=True,
            text=True,
            timeout=10,
            creationflags=_SEM_JANELA,
        )
    except Exception as erro:
        return False, str(erro)

    if resultado.returncode == 0:
        return True, None

    texto = f'{resultado.stdout or ""} {resultado.stderr or ""}'.strip()
    if re.search(r'\b1056\b', texto):
        return True, None
    return False, texto or f'O Windows não iniciou o serviço {nome}.'


def _aguardar_endpoint(host, porta, timeout):
    limite = time.monotonic() + timeout
    while time.monotonic() < limite:
        if _porta_aberta(host, porta):
            return True
        time.sleep(0.5)
    return _porta_aberta(host, porta)


def garantir_rabbitmq(url=None, timeout=25.0):
    url = url or RABBITMQ_URL
    try:
        host, porta = _endpoint(url)
    except ValueError as erro:
        return False, str(erro)

    locais = {'localhost', '127.0.0.1', '::1'}
    if host.lower() not in locais or sys.platform != 'win32':
        if _aguardar_endpoint(host, porta, timeout):
            return True, f'Broker RabbitMQ acessível em {host}:{porta}.'
        return False, f'RabbitMQ não respondeu em {host}:{porta}.'

    nome_servico = os.getenv('RABBITMQ_SERVICE_NAME', 'RabbitMQ')
    estado, erro = _consultar_servico(nome_servico)
    if estado is None:
        return False, (
            f'Serviço Windows {nome_servico} não encontrado ou indisponível. '
            f'Instale Erlang e RabbitMQ. {erro}'
        )

    iniciou = False
    if estado == 1:
        iniciou, erro = _iniciar_servico(nome_servico)
        if not iniciou:
            return False, (
                f'Não foi possível iniciar {nome_servico}; verifique as '
                f'permissões do Windows. {erro}'
            )
    elif estado not in {2, 3, 4, 5, 6}:
        return False, f'O serviço {nome_servico} está em estado não inicializável ({estado}).'

    limite = time.monotonic() + timeout
    parado_desde = None
    while time.monotonic() < limite:
        estado, erro = _consultar_servico(nome_servico)
        if estado is None:
            return False, erro
        if estado == 4 and _porta_aberta(host, porta):
            return True, f'Serviço RabbitMQ ativo em {host}:{porta}.'
        if estado == 1:
            if parado_desde is None:
                parado_desde = time.monotonic()
            elif time.monotonic() - parado_desde >= 2.0:
                break
        else:
            parado_desde = None
        time.sleep(0.5)

    if not iniciou and _porta_aberta(host, porta):
        return True, f'Broker RabbitMQ acessível em {host}:{porta}.'
    return False, (
        f'O serviço {nome_servico} não abriu a porta AMQP {host}:{porta} '
        f'em até {timeout:.0f} segundos.'
    )


def parar_rabbitmq(url=None, timeout=15.0):
    if sys.platform != 'win32':
        return False, 'Parada do serviço RabbitMQ só está disponível no Windows.'

    url = url or RABBITMQ_URL
    try:
        host, _ = _endpoint(url)
    except ValueError as erro:
        return False, str(erro)

    if host.lower() not in {'localhost', '127.0.0.1', '::1'}:
        return False, 'O broker configurado é remoto; o painel não o encerra.'

    nome_servico = os.getenv('RABBITMQ_SERVICE_NAME', 'RabbitMQ')
    estado, erro = _consultar_servico(nome_servico)
    if estado is None:
        return False, erro
    if estado == 1:
        return True, f'Serviço {nome_servico} já estava parado.'

    try:
        resultado = subprocess.run(
            ['sc.exe', 'stop', nome_servico],
            capture_output=True,
            text=True,
            timeout=10,
            creationflags=_SEM_JANELA,
        )
    except Exception as erro:
        return False, str(erro)

    texto = f'{resultado.stdout or ""} {resultado.stderr or ""}'.strip()
    if resultado.returncode != 0 and not re.search(r'\b1062\b', texto):
        return False, texto or f'O Windows não parou o serviço {nome_servico}.'

    limite = time.monotonic() + timeout
    while time.monotonic() < limite:
        estado, erro = _consultar_servico(nome_servico)
        if estado is None:
            return False, erro
        if estado == 1:
            return True, f'Serviço {nome_servico} parado.'
        time.sleep(0.5)

    return False, f'O serviço {nome_servico} não parou em até {timeout:.0f} segundos.'
