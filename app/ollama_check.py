from ollama import Client


def normalizar_host(host):
    host = (host or '').strip()
    if not host:
        host = 'http://127.0.0.1:11434'
    if not host.startswith(('http://', 'https://')):
        host = 'http://' + host
    return host


def _extrair_nomes(resposta):
    if isinstance(resposta, dict):
        modelos = resposta.get('models', [])
    else:
        modelos = getattr(resposta, 'models', None) or []

    nomes = []
    for m in modelos:
        if isinstance(m, dict):
            nomes.append(m.get('name') or m.get('model') or '')
        else:
            nomes.append(getattr(m, 'name', None) or getattr(m, 'model', None) or '')
    return nomes


def verificar(host):
    try:
        cliente = Client(host=normalizar_host(host))
        resposta = cliente.list()
        return True, _extrair_nomes(resposta), None
    except Exception as e:
        return False, [], str(e)


def modelo_presente(modelo, nomes):
    for nome in nomes:
        if nome == modelo or nome.startswith(modelo + ':'):
            return True
    return False


def baixar_modelo(modelo, host):
    cliente = Client(host=normalizar_host(host))
    cliente.pull(modelo)
