import os
import subprocess

import psutil


PROCESSOS_POR_AMOSTRA = 10

_PROCESSOS_PROTEGIDOS = {
    'system',
    'system idle process',
    'registry',
    'smss.exe',
    'csrss.exe',
    'wininit.exe',
    'services.exe',
    'lsass.exe',
    'winlogon.exe',
    'svchost.exe',
    'dwm.exe',
    'fontdrvhost.exe',
    'memcompression',
    'msmpeng.exe',
    'beam.smp.exe',
    'epmd.exe',
    'erl.exe',
    'ollama.exe',
    'ollama app.exe',
    'ollama_llama_server.exe',
    'llama-server.exe',
    'cyber-sentinel.exe',
}


def _pids_do_aplicativo(estrito=False):
    protegidos = {os.getpid()}
    try:
        protegidos.update(
            processo.pid
            for processo in psutil.Process(os.getpid()).children(recursive=True)
        )
    except psutil.Error:
        if estrito:
            raise
    return protegidos


def nome_protegido(nome):
    return (nome or '').strip().lower() in _PROCESSOS_PROTEGIDOS


def coletar_processos(limite=PROCESSOS_POR_AMOSTRA):
    cores = max(psutil.cpu_count(logical=True) or 1, 1)
    memoria_total = max(psutil.virtual_memory().total, 1)
    pids_protegidos = _pids_do_aplicativo()
    amostras = []

    for processo in psutil.process_iter():
        try:
            if processo.pid in pids_protegidos:
                continue
            with processo.oneshot():
                nome = processo.name()
                if nome_protegido(nome):
                    continue
                criado = processo.create_time()
                cpu_bruto = processo.cpu_percent(interval=None)
                memoria_rss = processo.memory_info().rss
                try:
                    exe_path = _caminho_executavel(processo.exe())
                except psutil.AccessDenied:
                    exe_path = None
                cpu = max(0.0, min(100.0, cpu_bruto / cores))
                ram = max(0.0, min(100.0, memoria_rss / memoria_total * 100.0))
            amostras.append({
                'pid': processo.pid,
                'name': nome,
                'create_time': criado,
                'exe_path': exe_path,
                'cpu_percent': cpu,
                'ram_percent': ram,
                'rss_bytes': memoria_rss,
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, OSError):
            continue

    amostras.sort(
        key=lambda item: max(item['cpu_percent'], item['ram_percent']),
        reverse=True,
    )
    return amostras[:limite]


def _caminho_executavel(caminho):
    return os.path.normcase(os.path.normpath(os.path.abspath(caminho)))


def _snapshot_processos():
    processos = {}
    try:
        for processo in psutil.process_iter():
            try:
                with processo.oneshot():
                    pid = processo.pid
                    ppid = processo.ppid()
                    nome = processo.name()
                    criado = processo.create_time()
                    try:
                        exe = processo.exe()
                    except psutil.AccessDenied:
                        exe = None
                processos[pid] = {
                    'pid': pid,
                    'ppid': ppid,
                    'name': nome,
                    'create_time': criado,
                    'exe_path': _caminho_executavel(exe) if exe else None,
                }
            except psutil.NoSuchProcess:
                continue
    except psutil.Error as erro:
        return None, erro
    return processos, None


def preparar_grupo_processo(pid, create_time, nome_esperado):
    try:
        pids_aplicativo = _pids_do_aplicativo(estrito=True)
    except psutil.Error as erro:
        return 'FALHA', f'Não foi possível validar os processos do app: {erro}', None
    if not pid or pid in pids_aplicativo:
        return 'BLOQUEADO', 'O processo pertence ao próprio Cyber Sentinel.', None

    try:
        processo = psutil.Process(pid)
        with processo.oneshot():
            nome_atual = processo.name()
            criado_atual = processo.create_time()
            exe_atual = processo.exe()
        if abs(criado_atual - create_time) > 0.01:
            return (
                'BLOQUEADO',
                'O PID foi reutilizado; a ação foi cancelada por segurança.',
                None,
            )
        if nome_atual.casefold() != (nome_esperado or '').casefold():
            return (
                'BLOQUEADO',
                'O processo mudou desde a recomendação; a ação foi cancelada.',
                None,
            )
        if nome_protegido(nome_atual):
            return (
                'BLOQUEADO',
                f'O processo protegido {nome_atual} não pode ser encerrado.',
                None,
            )
        if not exe_atual:
            return 'FALHA', 'Não foi possível determinar o caminho do executável.', None

        caminho_exe = _caminho_executavel(exe_atual)
        snapshot, erro = _snapshot_processos()
        if erro is not None:
            return 'FALHA', f'Não foi possível enumerar os processos: {erro}', None

        desconhecidos = [
            item for item in snapshot.values()
            if item['exe_path'] is None
            and item['name'].casefold() == nome_atual.casefold()
        ]
        if desconhecidos:
            return (
                'FALHA',
                'Há processos com o mesmo nome cujo caminho não pôde ser validado.',
                None,
            )

        ids_grupo = {
            item['pid'] for item in snapshot.values()
            if item['exe_path'] == caminho_exe
        }
        registro_alvo = snapshot.get(pid)
        if registro_alvo is None:
            return 'JA_ENCERRADO', 'O processo recomendado já não está ativo.', None
        if (
            abs(registro_alvo['create_time'] - create_time) > 0.01
            or registro_alvo['exe_path'] != caminho_exe
        ):
            return (
                'BLOQUEADO',
                'A identidade do processo mudou durante a validação.',
                None,
            )

        filhos = {}
        for item in snapshot.values():
            filhos.setdefault(item['ppid'], []).append(item['pid'])
        pendentes = list(ids_grupo)
        while pendentes:
            pai = pendentes.pop()
            for filho in filhos.get(pai, ()):
                if filho not in ids_grupo:
                    if snapshot[filho]['create_time'] < snapshot[pai]['create_time']:
                        return (
                            'FALHA',
                            f'Não foi possível validar a relação pai-filho do PID {filho}.',
                            None,
                        )
                    ids_grupo.add(filho)
                    pendentes.append(filho)

        membros = [snapshot[pid_membro] for pid_membro in ids_grupo]
        membros.sort(key=lambda item: item['pid'])
        for membro in membros:
            if membro['pid'] in pids_aplicativo:
                return (
                    'BLOQUEADO',
                    'O grupo inclui o próprio Cyber Sentinel.',
                    None,
                )
            if nome_protegido(membro['name']):
                return (
                    'BLOQUEADO',
                    f"O grupo inclui o processo protegido {membro['name']}.",
                    None,
                )

        if pid not in ids_grupo:
            return 'JA_ENCERRADO', 'O processo recomendado já não está ativo.', None
        ids = {membro['pid'] for membro in membros}
        raizes = [membro['pid'] for membro in membros if membro['ppid'] not in ids]
        return 'OK', '', {
            'exe_path': caminho_exe,
            'anchor': {
                'pid': pid,
                'name': nome_atual,
                'create_time': criado_atual,
            },
            'processes': membros,
            'roots': raizes,
        }
    except psutil.NoSuchProcess:
        return 'JA_ENCERRADO', f'O processo {nome_esperado} (PID {pid}) já não estava ativo.', None
    except psutil.AccessDenied:
        return 'FALHA', f'O Windows negou acesso ao processo {nome_esperado} (PID {pid}).', None
    except psutil.Error as erro:
        return 'FALHA', f'Não foi possível validar o processo {nome_esperado} (PID {pid}): {erro}', None


def terminar_grupo_processo(grupo):
    pids_confirmados = {
        (membro['pid'], membro['create_time'])
        for membro in grupo['processes']
    }
    for membro in grupo['processes']:
        try:
            processo = psutil.Process(membro['pid'])
            with processo.oneshot():
                nome = processo.name()
                criado = processo.create_time()
                try:
                    exe = processo.exe()
                except psutil.AccessDenied:
                    exe = None
        except psutil.NoSuchProcess:
            continue
        except psutil.AccessDenied:
            return 'FALHA', f"O Windows negou acesso ao PID {membro['pid']} durante a confirmação."
        except psutil.Error as erro:
            return 'FALHA', f"Não foi possível revalidar o PID {membro['pid']}: {erro}"
        if (
            abs(criado - membro['create_time']) > 0.01
            or nome.casefold() != membro['name'].casefold()
            or (
                membro['exe_path'] is not None
                and (not exe or _caminho_executavel(exe) != membro['exe_path'])
            )
        ):
            return 'BLOQUEADO', (
                f"O PID {membro['pid']} mudou desde a confirmação; ação cancelada."
            )

    ancora = grupo['anchor']
    status, mensagem, grupo_atual = preparar_grupo_processo(
        ancora['pid'], ancora['create_time'], ancora['name'],
    )
    if status != 'OK':
        return status, mensagem
    novos = [
        membro for membro in grupo_atual['processes']
        if (membro['pid'], membro['create_time']) not in pids_confirmados
    ]
    if novos:
        grupo.clear()
        grupo.update(grupo_atual)
        return (
            'GRUPO_ALTERADO',
            'O grupo ganhou processos desde a confirmação; revise a lista atualizada.',
        )
    grupo.clear()
    grupo.update(grupo_atual)

    try:
        pids_aplicativo = _pids_do_aplicativo(estrito=True)
    except psutil.Error as erro:
        return 'FALHA', f'Não foi possível validar os processos do app: {erro}'

    ativos = []
    try:
        for membro in grupo['processes']:
            pid = membro['pid']
            if pid in pids_aplicativo:
                return 'BLOQUEADO', 'O grupo inclui o próprio Cyber Sentinel.'
            try:
                processo = psutil.Process(pid)
                with processo.oneshot():
                    nome = processo.name()
                    criado = processo.create_time()
                    try:
                        exe = processo.exe()
                    except psutil.AccessDenied:
                        exe = None
            except psutil.NoSuchProcess:
                continue
            if (
                abs(criado - membro['create_time']) > 0.01
                or nome.casefold() != membro['name'].casefold()
                or (
                    membro['exe_path'] is not None
                    and (not exe or _caminho_executavel(exe) != membro['exe_path'])
                )
            ):
                return 'BLOQUEADO', f'O PID {pid} mudou desde a confirmação; ação cancelada.'
            if nome_protegido(nome):
                return 'BLOQUEADO', f'O grupo inclui o processo protegido {nome}.'
            ativos.append((membro, processo))
    except psutil.AccessDenied:
        return 'FALHA', 'O Windows negou acesso ao revalidar o grupo.'
    except psutil.Error as erro:
        return 'FALHA', f'Não foi possível revalidar o grupo: {erro}'

    if not ativos:
        return 'JA_ENCERRADO', 'Os processos do grupo já não estavam ativos.'

    ids_ativos = {membro['pid'] for membro, _ in ativos}
    raizes = [
        (membro, processo) for membro, processo in ativos
        if membro['ppid'] not in ids_ativos
    ]

    membros_conhecidos = {membro['pid']: membro for membro, _ in ativos}
    novos_descendentes = {}
    for membro, processo in raizes:
        try:
            descendentes = processo.children(recursive=True)
        except psutil.NoSuchProcess:
            continue
        except psutil.Error as erro:
            return 'FALHA', f"Não foi possível enumerar filhos do PID {membro['pid']}: {erro}"
        for descendente in descendentes:
            try:
                if descendente.pid in pids_aplicativo:
                    return 'BLOQUEADO', 'A árvore inclui o próprio Cyber Sentinel.'
                with descendente.oneshot():
                    pid_filho = descendente.pid
                    nome_descendente = descendente.name()
                    criado_descendente = descendente.create_time()
                    ppid_descendente = descendente.ppid()
                    try:
                        exe_descendente = descendente.exe()
                    except psutil.AccessDenied:
                        exe_descendente = None
                if nome_protegido(nome_descendente):
                    return (
                        'BLOQUEADO',
                        f'A árvore inclui o processo protegido {nome_descendente}.',
                    )
                esperado = membros_conhecidos.get(pid_filho)
                if esperado is not None:
                    if abs(criado_descendente - esperado['create_time']) > 0.01:
                        return (
                            'BLOQUEADO',
                            f'O PID {pid_filho} mudou na árvore; ação cancelada.',
                        )
                    continue
                novos_descendentes[pid_filho] = {
                    'pid': pid_filho,
                    'ppid': ppid_descendente,
                    'name': nome_descendente,
                    'create_time': criado_descendente,
                    'exe_path': (
                        _caminho_executavel(exe_descendente)
                        if exe_descendente else None
                    ),
                }
            except psutil.NoSuchProcess:
                continue
            except psutil.Error as erro:
                return (
                    'FALHA',
                    f'Não foi possível validar um filho do PID {membro["pid"]}: {erro}',
                )
    if novos_descendentes:
        grupo['processes'].extend(novos_descendentes.values())
        grupo['processes'].sort(key=lambda item: item['pid'])
        return (
            'GRUPO_ALTERADO',
            'A árvore ganhou processos desde a confirmação; revise a lista atualizada.',
        )

    erros = []
    houve_sucesso = False
    identidade_alterada = False
    for membro, processo in raizes:
        try:
            with processo.oneshot():
                if (
                    abs(processo.create_time() - membro['create_time']) > 0.01
                    or processo.name().casefold() != membro['name'].casefold()
                    or (
                        membro['exe_path'] is not None
                        and _caminho_executavel(processo.exe()) != membro['exe_path']
                    )
                ):
                    erros.append(
                        f"O PID {membro['pid']} mudou antes do encerramento; não foi sinalizado."
                    )
                    identidade_alterada = True
                    continue
            # shortcut: taskkill usa PIDs e mantém uma pequena corrida; use Job Objects para eliminá-la.
            resultado = subprocess.run(
                ['taskkill', '/F', '/T', '/PID', str(membro['pid'])],
                capture_output=True,
                text=True,
                timeout=10.0,
                check=False,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
            )
            if resultado.returncode == 0:
                houve_sucesso = True
            else:
                erros.append((resultado.stderr or resultado.stdout).strip())
        except subprocess.TimeoutExpired:
            erros.append(f'taskkill excedeu o tempo para o PID {membro["pid"]}.')
        except OSError as erro:
            erros.append(f'Não foi possível executar taskkill: {erro}')
        except psutil.NoSuchProcess:
            continue
        except psutil.AccessDenied:
            erros.append(f'O Windows negou acesso ao PID {membro["pid"]}.')
        except psutil.Error as erro:
            erros.append(f'Não foi possível validar o PID {membro["pid"]}: {erro}')

    try:
        _, vivos = psutil.wait_procs(
            [processo for _, processo in ativos], timeout=5.0,
        )
    except psutil.Error as erro:
        return 'FALHA', f'Não foi possível confirmar o encerramento do grupo: {erro}'
    if identidade_alterada:
        detalhe = '; '.join(erro for erro in erros if erro)
        return 'FALHA', (
            'A identidade de uma raiz mudou durante a ação; o resultado do grupo '
            f'é parcial. {detalhe}'
        )
    if vivos:
        pids_vivos = ', '.join(str(processo.pid) for processo in vivos)
        detalhe = next((erro for erro in erros if erro), '')
        return 'FALHA', (
            f'Não foi possível encerrar todos os processos do grupo. PIDs ativos: '
            f'{pids_vivos}. {detalhe}'
        ).strip()
    if not houve_sucesso:
        return 'JA_ENCERRADO', 'Os processos do grupo encerraram antes da ação do Windows.'

    return 'ENCERRADO', (
        f"Grupo de {os.path.basename(grupo['exe_path'])} encerrado "
        f'({len(ativos)} processo(s)).'
    )


def terminar_processo(pid, create_time, nome_esperado):
    status, mensagem, grupo = preparar_grupo_processo(
        pid, create_time, nome_esperado,
    )
    if status != 'OK':
        return status, mensagem
    return terminar_grupo_processo(grupo)
