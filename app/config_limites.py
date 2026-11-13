import json
import os

from messaging.config import caminho_dados


PADRAO = {
    'cpu_global': 85.0,
    'ram_global': 85.0,
}


def caminho_configuracao():
    return caminho_dados('SENTINEL_SETTINGS', 'settings.json')


def carregar():
    limites = dict(PADRAO)
    try:
        with open(caminho_configuracao(), encoding='utf-8') as arquivo:
            salvos = json.load(arquivo)
        if not isinstance(salvos, dict):
            return limites
        for chave in limites:
            valor = salvos.get(chave)
            if isinstance(valor, (int, float)) and 0 < valor <= 100:
                limites[chave] = float(valor)
    except (OSError, ValueError, TypeError):
        pass
    return limites


def salvar(limites):
    caminho = caminho_configuracao()
    temporario = f'{caminho}.{os.getpid()}.tmp'
    with open(temporario, 'w', encoding='utf-8') as arquivo:
        json.dump(limites, arquivo, ensure_ascii=False, indent=2)
    os.replace(temporario, caminho)


def validar_texto(texto):
    try:
        valor = float(str(texto).strip().replace(',', '.'))
    except (TypeError, ValueError):
        return None
    if not 0 < valor <= 100:
        return None
    return valor
