import json
import os
import re
import sys
import threading
from datetime import datetime, timezone


_LOCK = threading.Lock()
_SENSITIVE_KEYS = ('password', 'passwd', 'secret', 'token', 'credential')
_URL_CREDENTIALS = re.compile(
    r'(?i)\b([a-z][a-z0-9+.-]*://)[^/@\s]+:[^/@\s]*@'
)


def _sanitizar(valor, chave=''):
    if any(palavra in str(chave).lower() for palavra in _SENSITIVE_KEYS):
        return '[REDACTED]'
    if isinstance(valor, dict):
        return {k: _sanitizar(v, k) for k, v in valor.items()}
    if isinstance(valor, (list, tuple)):
        return [_sanitizar(item) for item in valor]
    if isinstance(valor, str):
        return _URL_CREDENTIALS.sub(r'\1[REDACTED]@', valor)
    return valor


def caminho_log():
    configurado = os.getenv('SENTINEL_DEBUG_LOG')
    if configurado:
        caminho = os.path.abspath(os.path.expanduser(configurado))
    else:
        raiz = (
            os.path.dirname(sys.executable)
            if getattr(sys, 'frozen', False)
            else os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        caminho = os.path.join(raiz, 'logs', 'debug.txt')
    os.makedirs(os.path.dirname(caminho) or os.curdir, exist_ok=True)
    return caminho


def _escrever(caminho, linha):
    flags = os.O_CREAT | os.O_APPEND | os.O_WRONLY | getattr(os, 'O_BINARY', 0)
    descritor = os.open(caminho, flags, 0o600)
    bloqueado = False
    try:
        with _LOCK:
            if os.name == 'nt':
                import msvcrt
                os.lseek(descritor, 0, os.SEEK_SET)
                msvcrt.locking(descritor, msvcrt.LK_LOCK, 1)
                bloqueado = True
            else:
                import fcntl
                fcntl.flock(descritor, fcntl.LOCK_EX)
                bloqueado = True

            dados = linha.encode('utf-8', errors='replace')
            restante = memoryview(dados)
            while restante:
                escrito = os.write(descritor, restante)
                restante = restante[escrito:]
    finally:
        if bloqueado:
            try:
                if os.name == 'nt':
                    import msvcrt
                    os.lseek(descritor, 0, os.SEEK_SET)
                    msvcrt.locking(descritor, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(descritor, fcntl.LOCK_UN)
            except OSError:
                pass
        os.close(descritor)


def registrar(componente, evento, nivel='INFO', **campos):
    agora_local = datetime.now().astimezone()
    agora_utc = agora_local.astimezone(timezone.utc)
    local = agora_local.isoformat(timespec='milliseconds')
    utc = agora_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    session_id = os.getenv('SENTINEL_SESSION_ID')
    contexto = {
        'pid': os.getpid(),
        'thread': threading.current_thread().name,
        **campos,
    }
    if session_id:
        contexto['session_id'] = session_id
    try:
        contexto = _sanitizar(contexto)
        dados = json.dumps(
            contexto,
            ensure_ascii=False,
            separators=(',', ':'),
            allow_nan=False,
            default=str,
        )
        _escrever(
            caminho_log(),
            f'{local} | UTC {utc} | {nivel.upper()} | '
            f'{componente} | {evento} | {dados}\n',
        )
    except Exception:
        return False
    if session_id:
        try:
            from messaging.session_report import registrar_evento

            registrar_evento(
                session_id, local, utc, nivel, componente, evento, contexto,
            )
        except Exception:
            pass
    return True
