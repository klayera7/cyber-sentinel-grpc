import os
import queue
import sys
import subprocess
import threading
import time
import uuid
import tkinter as tk
import tkinter.font as tkfont
from datetime import datetime
from tkinter import messagebox, ttk

_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMPO_DRENAGEM_SESSAO = 5.0
TEMPO_REAVALIACAO_PROCESSO = 8.0
for _p in (_RAIZ, os.path.join(_RAIZ, 'proto')):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from server.main import ServidorMonitorizacao, criar_servidor, parar_servidor
from app import charts, config_limites, markdown, ollama_check, theme
from app.coletor import Coletor
from app.historico import HistoricoSessao
from app.processos import preparar_grupo_processo, terminar_grupo_processo
from app.rabbitmq_service import garantir_rabbitmq, parar_rabbitmq
from consumer.state_store import StateStore
from messaging import debug_log
from messaging import session_report
from messaging.config import caminho_dados_sessao


def _escolher_processo(amostra, limites, tentados):
    cpu_acima = amostra.cpu >= limites['cpu_global']
    ram_acima = amostra.ram >= limites['ram_global']
    if not cpu_acima and not ram_acima:
        return None

    processos = [
        processo for processo in amostra.processos
        if (processo['pid'], processo['create_time']) not in tentados
        and ('exe', processo.get('exe_path')) not in tentados
    ]
    if not processos:
        return None

    def pressao(processo):
        cpu = (
            processo['cpu_percent'] / limites['cpu_global']
            if cpu_acima else 0
        )
        ram = (
            processo['ram_percent'] / limites['ram_global']
            if ram_acima else 0
        )
        return max(cpu, ram)

    return max(processos, key=pressao)


def _round_rect(canvas, x1, y1, x2, y2, r, **kw):
    pontos = [
        x1 + r, y1, x2 - r, y1, x2, y1, x2, y1 + r,
        x2, y2 - r, x2, y2, x2 - r, y2, x1 + r, y2,
        x1, y2, x1, y2 - r, x1, y1 + r, x1, y1,
    ]
    return canvas.create_polygon(pontos, smooth=True, **kw)


class Pill(tk.Canvas):
    def __init__(self, parent, texto, cor, altura=26, largura=None):
        self._fonte = tkfont.Font(family='Segoe UI', size=9, weight='bold')
        self._texto = texto
        self._cor = cor
        self._altura = altura
        self._largura_min = largura or 0
        super().__init__(parent, height=altura, bg=theme.BG, highlightthickness=0)
        self._render()

    def _largura(self):
        return max(self._fonte.measure(self._texto) + 40, self._largura_min)

    def _render(self):
        w = self._largura()
        h = self._altura
        self.config(width=w)
        self.delete('all')
        _round_rect(self, 1, 1, w - 1, h - 1, h // 2, fill=theme.SURFACE, outline='')
        self.create_oval(12, h // 2 - 4, 12 + 8, h // 2 + 4, fill=self._cor, outline='')
        self.create_text(27, h // 2, text=self._texto, fill=theme.FG, font=self._fonte, anchor='w')

    def set(self, texto, cor):
        if texto == self._texto and cor == self._cor:
            return
        self._texto = texto
        self._cor = cor
        self._render()


class PillButton(tk.Canvas):
    def __init__(self, parent, texto, comando, primario=False, altura=34):
        self._fonte = tkfont.Font(family='Segoe UI', size=10, weight='bold')
        self._texto = texto
        self._comando = comando
        self._primario = primario
        self._altura = altura
        self._hover = False
        self._enabled = True
        super().__init__(
            parent, height=altura, bg=theme.BG, highlightthickness=0, cursor='hand2'
        )
        self._render()
        self.bind('<Enter>', self._on_enter)
        self.bind('<Leave>', self._on_leave)
        self.bind('<Button-1>', self._on_click)

    def _largura(self):
        return self._fonte.measure(self._texto) + 48

    def _cores(self):
        if not self._enabled:
            return theme.SURFACE, theme.FG_MUTED
        if self._primario:
            return (theme.ACCENT_HOVER if self._hover else theme.ACCENT), '#ffffff'
        return (theme.SURFACE_2 if self._hover else theme.SURFACE), theme.FG

    def _render(self):
        w = self._largura()
        h = self._altura
        self.config(width=w)
        self.delete('all')
        bg, fg = self._cores()
        _round_rect(self, 1, 1, w - 1, h - 1, h // 2, fill=bg, outline='')
        self.create_text(w // 2, h // 2, text=self._texto, fill=fg, font=self._fonte)

    def _on_enter(self, _e):
        if self._enabled:
            self._hover = True
            self._render()

    def _on_leave(self, _e):
        self._hover = False
        self._render()

    def _on_click(self, _e):
        if self._enabled and self._comando:
            self._comando()

    def set_enabled(self, enabled):
        self._enabled = enabled
        self._hover = False
        self.config(cursor='hand2' if enabled else '')
        self._render()


class CampoConfiguracao(tk.Frame):
    def __init__(self, parent, rotulo, valor_inicial, largura=180):
        super().__init__(parent, bg=theme.BG)
        tk.Label(
            self,
            text=rotulo,
            font=('Segoe UI', 8),
            fg=theme.FG_MUTED,
            bg=theme.BG,
        ).pack(anchor='w', pady=(0, 5))

        self.canvas = tk.Canvas(
            self, height=40, width=largura, bg=theme.BG, highlightthickness=0,
        )
        self.canvas.pack(fill='x', expand=True)
        self._focado = False
        self.entry = tk.Entry(
            self.canvas,
            bg=theme.SURFACE,
            fg=theme.FG,
            insertbackground=theme.ACCENT,
            selectbackground=theme.ACCENT,
            selectforeground='#ffffff',
            font=('Segoe UI', 10),
            relief='flat',
            borderwidth=0,
            highlightthickness=0,
        )
        self.entry.insert(0, valor_inicial)
        self._janela = self.canvas.create_window(
            13, 20, window=self.entry, anchor='w', width=max(40, largura - 26), height=24,
        )
        self.canvas.bind('<Configure>', self._ao_redimensionar)
        self.entry.bind('<FocusIn>', self._ao_focar)
        self.entry.bind('<FocusOut>', self._ao_desfocar)
        self._render()

    def _ao_redimensionar(self, evento):
        self.canvas.coords(self._janela, 13, evento.height / 2)
        self.canvas.itemconfigure(
            self._janela, width=max(40, evento.width - 26), height=24,
        )
        self._render()

    def _ao_focar(self, _evento):
        self._focado = True
        self._render()

    def _ao_desfocar(self, _evento):
        self._focado = False
        self._render()

    def _render(self):
        largura = max(2, self.canvas.winfo_width())
        altura = max(2, self.canvas.winfo_height())
        self.canvas.delete('moldura')
        _round_rect(
            self.canvas,
            1,
            1,
            largura - 1,
            altura - 1,
            10,
            fill=theme.SURFACE,
            outline=theme.ACCENT if self._focado else theme.SURFACE_2,
            width=1.5 if self._focado else 1,
            tags='moldura',
        )
        self.canvas.tag_lower('moldura')


class SinoNotificacoes(tk.Canvas):
    def __init__(self, parent, comando):
        super().__init__(
            parent, width=46, height=38, bg=theme.BG,
            highlightthickness=0, cursor='hand2',
        )
        self._comando = comando
        self._quantidade = 0
        self._hover = False
        self.bind('<Button-1>', self._clicar)
        self.bind('<Enter>', self._entrar)
        self.bind('<Leave>', self._sair)
        self._render()

    def set_quantidade(self, quantidade):
        quantidade = max(0, int(quantidade))
        if quantidade != self._quantidade:
            self._quantidade = quantidade
            self._render()

    def _clicar(self, _evento):
        if self._comando:
            self._comando()

    def _entrar(self, _evento):
        self._hover = True
        self._render()

    def _sair(self, _evento):
        self._hover = False
        self._render()

    def _render(self):
        self.delete('all')
        cor = theme.ACCENT if self._hover else theme.FG
        self.create_arc(
            13, 7, 29, 24, start=0, extent=180, style='arc',
            outline=cor, width=2,
        )
        self.create_line(13, 16, 13, 24, fill=cor, width=2)
        self.create_line(29, 16, 29, 24, fill=cor, width=2)
        self.create_line(13, 24, 11, 27, 31, 27, 29, 24, fill=cor, width=2, smooth=True)
        self.create_oval(19, 28, 23, 32, fill=cor, outline='')

        if self._quantidade:
            texto = str(self._quantidade) if self._quantidade < 100 else '99+'
            largura = 20 if len(texto) < 3 else 25
            x2 = 45
            x1 = x2 - largura
            self.create_oval(x1, 0, x2, 19, fill=theme.ERR, outline=theme.BG, width=1)
            self.create_text(
                (x1 + x2) / 2, 9.5, text=texto, fill='#ffffff',
                font=('Segoe UI', 7, 'bold'),
            )


class App:
    def __init__(self, root):
        self.root = root
        self.root.title('Cyber Sentinel')
        self.root.geometry('1160x820')
        self.root.minsize(980, 700)
        self.root.configure(bg=theme.BG)

        self.fila_log = queue.Queue()
        self.servidor = None
        self.coletor = Coletor(intervalo=2.0, on_erro=self._log)
        self._avisou_gpu = False
        self._avisou_temp = False
        self._ultimo_diag = None
        self._iniciando = False
        self._app_fechando = False
        self._thread_preparacao = None
        self.session_id = None
        self._env_sessao_original = None
        self._rabbitmq_lock = threading.Lock()
        self._parar_rabbitmq_ao_encerrar = False
        self.processo_consumer = None
        self.state_store = None
        self.fila_alertas = queue.Queue()
        self.notificacoes = []
        self._alertas_ids = set()
        self._popup_alerta = None
        self._popup_alerta_id = None
        self._janela_notificacoes = None
        self._texto_notificacoes = None
        self._historico_acoes = None
        self.historico_sessao = HistoricoSessao()
        self._historico_offset = 0
        self._historico_filtro = None
        self._historico_busca = None
        self._historico_status = None
        self._historico_btn_novos = None
        self._historico_btn_antigos = None
        self.limites = config_limites.carregar()
        self._limites_sessao = None

        self._aplicar_tema()
        self._construir_ui()
        self._agendar()
        self.root.protocol('WM_DELETE_WINDOW', self._ao_fechar)

        self._log('Pronto. Configure os campos e clique em "Iniciar".')
        self._verificar_ollama_async()

    def _aplicar_tema(self):
        style = ttk.Style(self.root)
        style.theme_use('clam')
        style.configure(
            'TEntry',
            fieldbackground=theme.SURFACE,
            foreground=theme.FG,
            bordercolor=theme.SURFACE,
        )
        style.configure(
            'Historico.TCombobox',
            fieldbackground=theme.SURFACE,
            background=theme.SURFACE,
            foreground=theme.FG,
            arrowcolor=theme.FG_MUTED,
            bordercolor=theme.SURFACE_2,
        )
        style.map(
            'Historico.TCombobox',
            fieldbackground=[('readonly', theme.SURFACE)],
            foreground=[('readonly', theme.FG)],
            selectbackground=[('readonly', theme.SURFACE_2)],
            selectforeground=[('readonly', theme.FG)],
        )

    def _construir_ui(self):
        header = tk.Frame(self.root, bg=theme.BG)
        header.pack(fill='x', padx=24, pady=(20, 10))

        titulo = tk.Frame(header, bg=theme.BG)
        titulo.pack(side='left')
        tk.Label(
            titulo, text='Cyber Sentinel', font=('Segoe UI Semibold', 16),
            fg=theme.FG, bg=theme.BG,
        ).pack(anchor='w')
        tk.Label(
            titulo, text='Monitoramento de sistema com IA', font=('Segoe UI', 9),
            fg=theme.FG_MUTED, bg=theme.BG,
        ).pack(anchor='w')

        self.sino_notificacoes = SinoNotificacoes(
            header, self._abrir_notificacoes,
        )
        self.sino_notificacoes.pack(side='left', padx=(16, 0))

        status = tk.Frame(header, bg=theme.BG)
        status.pack(side='right')

        self.pill_servidor = Pill(status, 'Servidor · parado', theme.ERR)
        self.pill_servidor.pack(side='left', padx=(0, 8))
        self.pill_ollama = Pill(status, 'Ollama · —', theme.GRAY)
        self.pill_ollama.pack(side='left', padx=(0, 8))
        self.pill_modelo = Pill(status, 'Modelo · —', theme.GRAY)
        self.pill_modelo.pack(side='left')

        principal = tk.Frame(self.root, bg=theme.BG)
        principal.pack(fill='both', expand=True, padx=24, pady=(0, 8))

        self.graficos = charts.Graficos(principal)
        self.graficos.widget.pack(side='left', fill='both', expand=True, padx=(0, 18))

        painel = tk.Frame(principal, bg=theme.BG, width=360)
        painel.pack(side='right', fill='both', expand=False)
        painel.pack_propagate(False)

        metricas = tk.Frame(painel, bg=theme.BG)
        metricas.pack(fill='x', pady=(0, 12))

        self.pill_gpu = Pill(metricas, 'GPU · —', theme.GRAY, largura=140)
        self.pill_gpu.pack(side='left', padx=(0, 8))
        self.pill_temp = Pill(metricas, 'GPU Temp · —', theme.GRAY, largura=190)
        self.pill_temp.pack(side='left')

        tk.Label(
            painel, text='Diagnósticos', font=('Segoe UI Semibold', 11),
            fg=theme.FG_MUTED, bg=theme.BG,
        ).pack(anchor='w', pady=(0, 6))

        self.feed = tk.Text(
            painel, wrap='word', bg=theme.BG, fg=theme.FG, font=('Segoe UI', 9),
            relief='flat', borderwidth=0, state='disabled', cursor='arrow',
            insertbackground=theme.FG, selectbackground=theme.ACCENT,
        )
        self.feed.pack(fill='both', expand=True)
        self.feed.tag_configure(
            'card', background=theme.SURFACE,
            lmargin1=14, lmargin2=14, rmargin=14, spacing1=6, spacing3=6,
        )
        self.feed.tag_configure('hora', foreground=theme.FG_MUTED, font=('Segoe UI', 8))
        self.feed.tag_configure('h1', foreground=theme.ACCENT, font=('Segoe UI Semibold', 12))
        self.feed.tag_configure('h2', foreground=theme.ACCENT, font=('Segoe UI Semibold', 11))
        self.feed.tag_configure('h3', foreground=theme.FG, font=('Segoe UI Semibold', 10))
        self.feed.tag_configure('bold', foreground=theme.FG, font=('Segoe UI', 9, 'bold'))
        self.feed.tag_configure('italic', foreground=theme.FG_MUTED, font=('Segoe UI', 9, 'italic'))
        self.feed.tag_configure('code', foreground=theme.ACCENT, font=('Consolas', 9))
        self.feed.tag_configure('li', lmargin1=32, lmargin2=32)

        self.log = tk.Text(
            self.root, height=3, wrap='word', bg=theme.BG, fg=theme.FG_MUTED,
            font=('Consolas', 8), relief='flat', borderwidth=0, state='disabled',
            cursor='arrow', insertbackground=theme.FG, selectbackground=theme.ACCENT,
        )
        self.log.pack(fill='x', padx=24, pady=(0, 4))

        rodape = tk.Frame(self.root, bg=theme.BG)
        rodape.pack(fill='x', padx=24, pady=(4, 18))

        cfg = tk.Frame(rodape, bg=theme.BG)
        cfg.pack(fill='x')

        cfg.grid_columnconfigure(0, weight=0, minsize=180)
        cfg.grid_columnconfigure(1, weight=1, minsize=300)
        cfg.grid_columnconfigure(2, weight=0, minsize=130)

        campo_modelo = CampoConfiguracao(
            cfg, 'MODELO', os.getenv('OLLAMA_MODEL', 'llama3.1'), largura=180,
        )
        campo_modelo.grid(row=0, column=0, sticky='ew', padx=(0, 14))
        self.ent_modelo = campo_modelo.entry

        campo_host = CampoConfiguracao(
            cfg, 'HOST OLLAMA', os.getenv('OLLAMA_HOST', 'http://127.0.0.1:11434'),
            largura=300,
        )
        campo_host.grid(row=0, column=1, sticky='ew', padx=(0, 14))
        self.ent_host = campo_host.entry

        campo_porta = CampoConfiguracao(
            cfg, 'PORTA gRPC', os.getenv('GRPC_PORT', '50051'), largura=130,
        )
        campo_porta.grid(row=0, column=2, sticky='ew')
        self.ent_porta = campo_porta.entry

        cfg_limites = tk.Frame(rodape, bg=theme.BG)
        cfg_limites.pack(fill='x', pady=(10, 0))
        for coluna in range(2):
            cfg_limites.grid_columnconfigure(coluna, weight=1)
        self.ent_limites = {}
        for coluna, (chave, rotulo) in enumerate((
            ('cpu_global', 'LIMITE CPU SISTEMA (%)'),
            ('ram_global', 'LIMITE RAM SISTEMA (%)'),
        )):
            campo = CampoConfiguracao(
                cfg_limites, rotulo, f'{self.limites[chave]:g}', largura=150,
            )
            campo.grid(
                row=0, column=coluna, sticky='ew',
                padx=(0, 14) if coluna == 0 else 0,
            )
            self.ent_limites[chave] = campo.entry

        botoes = tk.Frame(rodape, bg=theme.BG)
        botoes.pack(fill='x', pady=(14, 0))

        self.btn_iniciar = PillButton(botoes, 'Iniciar', self._iniciar, primario=True)
        self.btn_iniciar.pack(side='left', padx=(0, 8))
        self.btn_parar = PillButton(botoes, 'Parar', self._parar)
        self.btn_parar.pack(side='left', padx=(0, 8))
        self.btn_parar.set_enabled(False)
        self.btn_verificar = PillButton(botoes, 'Verificar Ollama', self._verificar_ollama_async)
        self.btn_verificar.pack(side='left', padx=(0, 8))
        self.btn_baixar = PillButton(botoes, 'Baixar modelo', self._baixar_modelo)
        self.btn_baixar.pack(side='left')
        self.btn_baixar.set_enabled(False)

    def _set_pill(self, pill, categoria, estado, cor):
        pill.set(f'{categoria} · {estado}', cor)

    def _iniciar(self):
        if self.servidor is not None or self._iniciando:
            self._log('O servidor já está em execução.')
            return
        try:
            porta = self._porta()
            modelo = self._modelo()
            host = self._host()
            limites = self._ler_limites_interface()
            if limites is None:
                return

            os.environ['OLLAMA_MODEL'] = modelo
            os.environ['OLLAMA_HOST'] = host
            os.environ['SENTINEL_CPU_LIMIT'] = str(limites['cpu_global'])
            os.environ['SENTINEL_RAM_LIMIT'] = str(limites['ram_global'])
            self.limites = limites
            self._limites_sessao = dict(limites)
            config_limites.salvar(limites)
            self._iniciar_nova_sessao()

            self._iniciando = True
            with self._rabbitmq_lock:
                self._parar_rabbitmq_ao_encerrar = True
            self.btn_iniciar.set_enabled(False)
            self._log('Verificando/iniciando o serviço RabbitMQ...')
            self._thread_preparacao = threading.Thread(
                target=self._preparar_rabbitmq,
                args=(porta,),
                daemon=True,
            )
            self._thread_preparacao.start()
        except Exception as e:
            self._iniciando = False
            with self._rabbitmq_lock:
                self._parar_rabbitmq_ao_encerrar = False
            self.btn_iniciar.set_enabled(True)
            self._log(f'Falha ao preparar inicialização: {e}')
            self._finalizar_sessao('falha_ao_preparar')

    def _iniciar_nova_sessao(self):
        session_id = str(uuid.uuid4())
        variaveis = (
            'SENTINEL_SESSION_ID',
            'SENTINEL_OUTBOX_DB',
            'SENTINEL_STATE_DB',
        )
        valores_originais = {nome: os.environ.get(nome) for nome in variaveis}
        caminho_outbox = caminho_dados_sessao(
            'SENTINEL_OUTBOX_DB', 'rabbit_outbox.sqlite3', session_id,
        )
        caminho_estado = caminho_dados_sessao(
            'SENTINEL_STATE_DB', 'state_machine.sqlite3', session_id,
        )
        os.environ['SENTINEL_SESSION_ID'] = session_id
        os.environ['SENTINEL_OUTBOX_DB'] = caminho_outbox
        os.environ['SENTINEL_STATE_DB'] = caminho_estado
        self.session_id = session_id
        self._env_sessao_original = valores_originais
        try:
            session_report.iniciar_sessao(session_id)
            debug_log.registrar(
                'app', 'sessao_iniciada', session_id=session_id,
                outbox_db=caminho_outbox, state_db=caminho_estado,
                limits=getattr(self, '_limites_sessao', None),
            )
        except Exception:
            self._restaurar_ambiente_sessao()
            self.session_id = None
            raise

    def _restaurar_ambiente_sessao(self):
        valores = getattr(self, '_env_sessao_original', None)
        if valores is None:
            return
        for nome, valor in valores.items():
            if valor is None:
                os.environ.pop(nome, None)
            else:
                os.environ[nome] = valor
        self._env_sessao_original = None

    def _finalizar_sessao(self, motivo):
        session_id = getattr(self, 'session_id', None)
        if not session_id:
            return
        debug_log.registrar(
            'app', 'sessao_encerrada', session_id=session_id, reason=motivo,
        )
        try:
            session_report.finalizar_sessao(session_id, motivo)
        except Exception as erro:
            debug_log.registrar(
                'app', 'relatorio_sessao_falhou', nivel='ERROR',
                session_id=session_id, erro=erro,
            )
        finally:
            self.session_id = None
            self.state_store = None
            self._restaurar_ambiente_sessao()

    def _preparar_rabbitmq(self, porta):
        try:
            ativo, detalhe = garantir_rabbitmq()
        except Exception as erro:
            ativo, detalhe = False, f'Falha ao verificar o RabbitMQ: {erro}'
        if self._app_fechando:
            self._encerrar_rabbitmq()
            return
        try:
            self.root.after(0, self._continuar_inicio, porta, ativo, detalhe)
        except tk.TclError:
            self._encerrar_rabbitmq()

    def _continuar_inicio(self, porta, rabbitmq_ativo, detalhe):
        if self._app_fechando:
            self._encerrar_rabbitmq()
            return
        self._iniciando = False
        if rabbitmq_ativo:
            self._log(detalhe)
        else:
            self._log(
                f'Aviso: {detalhe} A coleta continuará e os eventos serão '
                'mantidos no outbox até o broker ficar disponível.'
            )

        try:
            servicer = ServidorMonitorizacao(
                ao_log=self._log,
                ao_alerta=self._receber_alerta,
            )
            self.servidor = criar_servidor(porta, servicer=servicer)
            servicer.ativar_amostras_diretas()
            self.coletor.ao_amostra = servicer.publicar_amostra_estado
            self._log(f'Servidor gRPC iniciado na porta {porta}.')

            self.state_store = StateStore()
            self._iniciar_consumidor(porta)
            self.coletor.iniciar(porta=porta)
            self._log('Coleta de telemetria iniciada.')

            self._set_pill(self.pill_servidor, 'Servidor', 'ativo', theme.OK)
            self.btn_iniciar.set_enabled(False)
            self.btn_parar.set_enabled(True)

            self._verificar_ollama_async()
        except Exception as e:
            self._log(f'Falha ao iniciar: {e}')
            self.coletor.ao_amostra = None
            if self.servidor is not None:
                try:
                    parar_servidor(self.servidor, grace=0)
                except Exception:
                    pass
                self.servidor = None
            self._parar_consumidor()
            self._encerrar_rabbitmq()
            self.btn_iniciar.set_enabled(True)
            self._finalizar_sessao('falha_na_inicializacao')

    def _parar(self, motivo='parada_pelo_usuario'):
        self.coletor.parar()
        self.coletor.ao_amostra = None
        self._log('Coleta de telemetria parada.')

        ultimo_evento = None
        outbox_pendente = 0
        publicador = None
        if self.servidor is not None:
            servicer = getattr(self.servidor, '_sentinel_servicer', None)
            ultimo_evento = getattr(servicer, 'ultimo_evento_estado', None)
            try:
                parar_servidor(self.servidor, grace=3)
            except Exception as e:
                self._log(f'Aviso ao parar o servidor: {e}')
            publicador = getattr(servicer, 'publicador', None)
            if publicador is not None:
                try:
                    outbox_pendente = publicador.outbox.quantidade_pendente()
                except Exception:
                    outbox_pendente = 0
            self.servidor = None
            self._log('Servidor gRPC parado.')

        consumidor_ativo = (
            self.processo_consumer is not None
            and self.processo_consumer.poll() is None
        )
        state_store = getattr(self, 'state_store', None)
        if consumidor_ativo and ultimo_evento and state_store is not None:
            if outbox_pendente:
                debug_log.registrar(
                    'app', 'outbox_sessao_com_pendencias', nivel='WARNING',
                    pending_count=outbox_pendente, last_event_id=ultimo_evento,
                )
                self._log(
                    f'Aviso: {outbox_pendente} evento(s) da sessão ficaram no '
                    'outbox e não serão reenviados.'
                )
            else:
                limite = time.monotonic() + TEMPO_DRENAGEM_SESSAO
                processado = False
                pendentes = []
                while time.monotonic() < limite:
                    try:
                        processado = state_store.evento_processado(
                            ultimo_evento
                        )
                        pendentes = state_store.alertas_pendentes()
                    except Exception:
                        break
                    if processado and not pendentes:
                        break
                    time.sleep(0.1)
                if not processado or pendentes:
                    debug_log.registrar(
                        'app', 'drenagem_sessao_incompleta', nivel='WARNING',
                        last_event_id=ultimo_evento, processed=processado,
                        pending_alerts=len(pendentes),
                    )
                    self._log(
                        'Aviso: o consumidor não confirmou todos os eventos '
                        'da sessão antes do encerramento.'
                    )
        if publicador is not None:
            try:
                pendentes_finais = publicador.outbox.quantidade_pendente()
            except Exception:
                pendentes_finais = 0
            if pendentes_finais and pendentes_finais != outbox_pendente:
                debug_log.registrar(
                    'app', 'outbox_sessao_com_pendencias', nivel='WARNING',
                    pending_count=pendentes_finais,
                    last_event_id=ultimo_evento, final_check=True,
                )
                self._log(
                    f'Aviso: {pendentes_finais} evento(s) da sessão ficaram '
                    'no outbox no encerramento.'
                )
        self._parar_consumidor()
        self._encerrar_rabbitmq()
        self._iniciando = False
        self._set_pill(self.pill_servidor, 'Servidor', 'parado', theme.ERR)
        self.btn_iniciar.set_enabled(True)
        self.btn_parar.set_enabled(False)
        self._finalizar_sessao(motivo)

    def _iniciar_consumidor(self, porta):
        if self.processo_consumer is not None:
            if self.processo_consumer.poll() is None:
                return
            self.processo_consumer = None

        congelado = getattr(sys, 'frozen', False)
        if congelado:
            comando = [sys.executable, '--consumer']
            saida = subprocess.DEVNULL
            erro = subprocess.DEVNULL
            diretorio = os.path.dirname(sys.executable)
        else:
            comando = [
                sys.executable,
                '-u',
                os.path.join(_RAIZ, 'consumer', 'main.py'),
            ]
            saida = subprocess.PIPE
            erro = subprocess.STDOUT
            diretorio = _RAIZ

        ambiente = os.environ.copy()
        ambiente['GRPC_TARGET'] = f'127.0.0.1:{porta}'
        opcoes = {
            'args': comando,
            'cwd': diretorio,
            'env': ambiente,
            'stdin': subprocess.DEVNULL,
            'stdout': saida,
            'stderr': erro,
        }
        if os.name == 'nt':
            opcoes['creationflags'] = getattr(subprocess, 'CREATE_NO_WINDOW', 0)

        try:
            processo = subprocess.Popen(**opcoes)
        except Exception as erro:
            self._log(f'Falha ao iniciar o consumidor de estados: {erro}')
            return

        self.processo_consumer = processo
        self._log(f'Consumidor de estados iniciado (PID {processo.pid}).')
        if processo.stdout is not None:
            threading.Thread(
                target=self._ler_log_consumidor,
                args=(processo,),
                daemon=True,
            ).start()

    def _ler_log_consumidor(self, processo):
        try:
            for linha in processo.stdout:
                texto = linha.rstrip()
                if texto:
                    self._log(f'[Consumidor] {texto}')
        except Exception as erro:
            self._log(f'Aviso ao ler log do consumidor: {erro}')
        finally:
            try:
                processo.stdout.close()
            except Exception:
                pass

    def _parar_consumidor(self):
        processo = self.processo_consumer
        self.processo_consumer = None
        if processo is None or processo.poll() is not None:
            return

        self._log('Encerrando o consumidor de estados...')
        try:
            processo.terminate()
            processo.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            processo.kill()
            processo.wait(timeout=3.0)
        except Exception as erro:
            self._log(f'Aviso ao encerrar o consumidor: {erro}')

    def _encerrar_rabbitmq(self):
        with self._rabbitmq_lock:
            if not self._parar_rabbitmq_ao_encerrar:
                return
            self._parar_rabbitmq_ao_encerrar = False

        self._log('Encerrando o serviço RabbitMQ...')
        try:
            parado, detalhe = parar_rabbitmq()
        except Exception as erro:
            parado, detalhe = False, str(erro)
        if parado:
            self._log(detalhe)
        else:
            self._log(f'Aviso ao parar RabbitMQ: {detalhe}')

    def _verificar_ollama_async(self):
        host = self._host()

        def trabalho():
            ativo, nomes, erro = ollama_check.verificar(host)
            self.root.after(0, self._aplicar_status_ollama, ativo, nomes, erro)

        threading.Thread(target=trabalho, daemon=True).start()

    def _aplicar_status_ollama(self, ativo, nomes, erro):
        if ativo:
            self._set_pill(self.pill_ollama, 'Ollama', 'ativo', theme.OK)
            if ollama_check.modelo_presente(self._modelo(), nomes):
                self._set_pill(self.pill_modelo, 'Modelo', 'ok', theme.OK)
                self.btn_baixar.set_enabled(False)
            else:
                self._set_pill(self.pill_modelo, 'Modelo', 'ausente', theme.WARN)
                self.btn_baixar.set_enabled(True)
        else:
            self._set_pill(self.pill_ollama, 'Ollama', 'off', theme.ERR)
            self._set_pill(self.pill_modelo, 'Modelo', '—', theme.GRAY)
            self._log(f'Ollama indisponível em {self._host()}: {erro}')

    def _baixar_modelo(self):
        modelo = self._modelo()
        self.btn_baixar.set_enabled(False)
        self._set_pill(self.pill_modelo, 'Modelo', 'baixando…', theme.WARN)
        self._log(f'Baixando modelo {modelo} (pode demorar)...')

        def trabalho():
            try:
                ollama_check.baixar_modelo(modelo, self._host())
                self.root.after(0, self._log, f'Modelo {modelo} baixado.')
                self.root.after(0, self._verificar_ollama_async)
            except Exception as e:
                self.root.after(0, self._log, f'Falha ao baixar o modelo: {e}')
                self.root.after(0, lambda: self._set_pill(self.pill_modelo, 'Modelo', 'erro', theme.ERR))
                self.root.after(0, lambda: self.btn_baixar.set_enabled(True))

        threading.Thread(target=trabalho, daemon=True).start()

    def _ler_limites_interface(self):
        limites = {}
        rotulos = {
            'cpu_global': 'CPU do sistema',
            'ram_global': 'RAM do sistema',
        }
        for chave, rotulo in rotulos.items():
            valor = config_limites.validar_texto(
                self.ent_limites[chave].get()
            )
            if valor is None:
                messagebox.showwarning(
                    'Limite inválido',
                    f'{rotulo}: informe um número maior que 0 e até 100.',
                    parent=self.root,
                )
                self._log(f'Limite inválido para {rotulo.lower()}.')
                return None
            limites[chave] = valor
        return limites

    def _modelo(self):
        return self.ent_modelo.get().strip() or 'llama3.1'

    def _host(self):
        return self.ent_host.get().strip() or 'http://127.0.0.1:11434'

    def _porta(self):
        try:
            return int(self.ent_porta.get().strip())
        except ValueError:
            return 50051

    def _log(self, msg):
        self.fila_log.put(msg)
        maiuscula = str(msg).upper()
        nivel = (
            'ERROR' if any(palavra in maiuscula for palavra in ('FALHA', 'ERRO'))
            else 'WARNING' if any(palavra in maiuscula for palavra in ('AVISO', 'INDISPONÍVEL'))
            else 'INFO'
        )
        debug_log.registrar('app', 'log', nivel=nivel, message=str(msg))

    def _registrar_historico(
        self, categoria, mensagem, ocorrido_em=None, nivel='INFO',
        origem='', host_id='',
    ):
        debug_log.registrar(
            'historico', 'registro', category=categoria, level=nivel,
            source=origem, host_id=host_id, timestamp=ocorrido_em,
            message=mensagem,
        )
        self.historico_sessao.registrar(
            categoria,
            mensagem,
            ocorrido_em=ocorrido_em,
            nivel=nivel,
            origem=origem,
            host_id=host_id,
        )
        if self._janela_notificacoes is not None:
            self._renderizar_notificacoes()

    @staticmethod
    def _classificar_log(mensagem):
        maiuscula = mensagem.upper()
        if '[DECISÃO]' in maiuscula or '[DECISAO]' in maiuscula:
            if '-> CRÍTICO' in maiuscula or '-> CRITICO' in maiuscula:
                nivel = 'CRITICAL'
            elif '-> ATENÇÃO' in maiuscula or '-> ATENCAO' in maiuscula:
                nivel = 'WARNING'
            else:
                nivel = 'INFO'
            return 'Decisões', nivel, 'consumidor'
        if any(palavra in maiuscula for palavra in ('FALHA', 'AVISO', 'ERRO', 'INDISPONÍVEL')):
            return 'Sistema', 'WARNING', 'aplicação'
        return 'Sistema', 'INFO', 'aplicação'

    def _agendar(self):
        self._atualizar_graficos()
        self._atualizar_metricas()
        self._atualizar_diagnosticos()
        self._atualizar_alertas()
        self._atualizar_log()

    def _atualizar_metricas(self):
        amostra = self.coletor.ultima()
        if amostra is not None:
            cor_gpu = theme.GPU if amostra.gpu is not None else theme.GRAY
            self._set_pill(self.pill_gpu, 'GPU', charts.formatar_pct(amostra.gpu), cor_gpu)
            self._set_pill(
                self.pill_temp, 'GPU Temp',
                charts.formatar_temp(amostra.temp), charts.cor_temp(amostra.temp),
            )
            if amostra.gpu is None and not self._avisou_gpu:
                self._avisou_gpu = True
                self._log('GPU não detectada: nvidia-smi indisponível.')
            if amostra.temp is None and not self._avisou_temp:
                self._avisou_temp = True
                self._log('Sensor de temperatura da GPU indisponível nesta máquina.')
        self.root.after(500, self._atualizar_metricas)

    def _atualizar_graficos(self):
        self.graficos.atualizar(self.coletor.historico)
        self.root.after(500, self._atualizar_graficos)

    def _atualizar_diagnosticos(self):
        for ts, texto in self.coletor.consumir_diagnosticos():
            nivel = (
                'WARNING'
                if texto.startswith('[Erro ao consultar a IA:')
                else 'INFO'
            )
            self._registrar_historico(
                'Diagnósticos', texto, ocorrido_em=ts,
                nivel=nivel, origem='servidor',
            )
            if texto == self._ultimo_diag:
                continue
            self._ultimo_diag = texto
            self.feed.config(state='normal')
            self.feed.insert('end', f'{ts}\n', ('card', 'hora'))
            markdown.inserir(self.feed, texto, ('card',))
            self.feed.insert('end', '\n')
            self.feed.config(state='disabled')
            self.feed.see('end')
        self.root.after(150, self._atualizar_diagnosticos)

    def _receber_alerta(self, alerta):
        debug_log.registrar(
            'app', 'alerta_recebido', alert_id=alerta.get('alert_id'),
            host_id=alerta.get('host_id'), level=alerta.get('level'),
            source=alerta.get('source'), message=alerta.get('message'),
        )
        self.fila_alertas.put(alerta)

    def _atualizar_alertas(self):
        while True:
            try:
                alerta = self.fila_alertas.get_nowait()
            except queue.Empty:
                break

            if (
                alerta.get('alert_type') == 'state_transition'
                and alerta.get('current_state') != 'CRÍTICO'
            ):
                for pendente in self.notificacoes:
                    if (
                        pendente.get('process')
                        and pendente.get('action_status') == 'PENDENTE'
                    ):
                        self._finalizar_sugestao(
                            pendente,
                            'NAO_MAIS_ALTO',
                            'O estado global saiu de CRÍTICO; encerramento cancelado.',
                        )

            alert_id = alerta['alert_id']
            if alert_id in self._alertas_ids:
                debug_log.registrar(
                    'app', 'alerta_duplicado_na_interface', alert_id=alert_id,
                )
                continue
            item = {**alerta, 'lida': False}
            if item.get('process'):
                item['action_status'] = 'PENDENTE'
            self._alertas_ids.add(alert_id)
            self.notificacoes.insert(0, item)
            self.notificacoes = self.notificacoes[:100]
            self._alertas_ids = {n['alert_id'] for n in self.notificacoes}
            self.sino_notificacoes.set_quantidade(
                sum(not n['lida'] for n in self.notificacoes)
            )
            self._registrar_historico(
                'Decisões' if item.get('process') else 'Notificações',
                (
                    f"Possível processo associado: {item['process']['name']} "
                    f"(PID {item['process']['pid']}). {alerta['message']}"
                    if item.get('process') else alerta['message']
                ),
                ocorrido_em=alerta['received_at'],
                nivel=alerta['level'],
                origem=alerta['source'],
                host_id=alerta['host_id'],
            )
            self._renderizar_notificacoes()
            self._renderizar_acoes_pendentes()
            self._mostrar_toast(item)

        self.root.after(150, self._atualizar_alertas)

    def _mostrar_toast(self, alerta):
        if self._popup_alerta is not None:
            self._fechar_toast(self._popup_alerta, 'substituido')

        cores = {
            'CRITICAL': theme.ERR,
            'WARNING': theme.WARN,
            'INFO': theme.ACCENT,
        }
        cor = cores.get(alerta['level'], theme.ACCENT)
        popup = tk.Toplevel(self.root)
        self._popup_alerta = popup
        self._popup_alerta_id = alerta['alert_id']
        popup.overrideredirect(True)
        popup.transient(self.root)
        popup.configure(bg=theme.SURFACE)
        popup.attributes('-topmost', True)

        cartao = tk.Frame(
            popup, bg=theme.SURFACE, highlightthickness=1,
            highlightbackground=cor,
        )
        cartao.pack(fill='both', expand=True)
        cabecalho = tk.Label(
            cartao,
            text=f"{alerta['level']} · {alerta['host_id']}",
            font=('Segoe UI Semibold', 9), fg=cor, bg=theme.SURFACE,
            anchor='w',
        )
        cabecalho.pack(fill='x', padx=14, pady=(10, 3))
        mensagem = tk.Label(
            cartao,
            text=alerta['message'],
            font=('Segoe UI', 9), fg=theme.FG, bg=theme.SURFACE,
            wraplength=330, justify='left', anchor='w',
        )
        mensagem.pack(fill='x', padx=14, pady=(0, 10))

        processo = alerta.get('process')
        if processo:
            tk.Label(
                cartao,
                text=(
                    f"Possível processo relacionado: {processo['name']} "
                    f"(PID {processo['pid']}) · CPU {processo['cpu_percent']:.1f}% · "
                    f"RAM {processo['ram_percent']:.1f}%"
                ),
                font=('Segoe UI', 8), fg=theme.FG_MUTED, bg=theme.SURFACE,
                anchor='w', wraplength=330, justify='left',
            ).pack(fill='x', padx=14, pady=(0, 8))
            botoes = tk.Frame(cartao, bg=theme.SURFACE)
            botoes.pack(fill='x', padx=14, pady=(0, 10))
            tk.Button(
                botoes, text='Encerrar processo',
                command=lambda: self._resolver_sugestao(
                    alerta['alert_id'], 'ENCERRAR',
                ),
                bg=theme.ERR, fg='#ffffff', activebackground=theme.ERR,
                activeforeground='#ffffff', relief='flat', borderwidth=0,
                padx=10, pady=5, cursor='hand2',
            ).pack(side='left', padx=(0, 8))
            tk.Button(
                botoes, text='Ignorar',
                command=lambda: self._resolver_sugestao(
                    alerta['alert_id'], 'IGNORAR',
                ),
                bg=theme.SURFACE_2, fg=theme.FG, activebackground=theme.SURFACE_2,
                activeforeground=theme.FG, relief='flat', borderwidth=0,
                padx=10, pady=5, cursor='hand2',
            ).pack(side='left')

        def abrir(_evento=None):
            self._abrir_notificacoes()

        for widget in (popup, cartao, cabecalho, mensagem):
            widget.bind('<Button-1>', abrir)

        self.root.update_idletasks()
        x = self.root.winfo_rootx() + max(12, self.root.winfo_width() - 380)
        y = self.root.winfo_rooty() + 72
        popup.geometry(f'+{x}+{y}')
        popup.lift()
        debug_log.registrar(
            'app', 'popup_exibido', alert_id=alerta['alert_id'],
            host_id=alerta['host_id'], level=alerta['level'],
            process=(
                {
                    chave: valor for chave, valor in processo.items()
                    if chave != 'exe_path'
                }
                if processo else None
            ),
            message=alerta['message'],
            timeout_ms=10000 if processo else 6500,
        )
        popup.after(
            10000 if processo else 6500,
            lambda: self._fechar_toast(popup, 'timeout'),
        )

    def _fechar_toast(self, popup, motivo='fechado'):
        alert_id = self._popup_alerta_id if self._popup_alerta is popup else None
        try:
            if popup.winfo_exists():
                popup.destroy()
        except tk.TclError:
            pass
        if self._popup_alerta is popup:
            self._popup_alerta = None
            self._popup_alerta_id = None
        if alert_id:
            debug_log.registrar(
                'app', 'popup_fechado', alert_id=alert_id, reason=motivo,
            )

    def _resolver_sugestao(self, alert_id, acao):
        alerta = next(
            (
                item for item in self.notificacoes
                if item['alert_id'] == alert_id and item.get('process')
            ),
            None,
        )
        if alerta is None or alerta.get('action_status') != 'PENDENTE':
            return
        if acao == 'IGNORAR':
            self._finalizar_sugestao(
                alerta, 'IGNORADO', 'Sugestão ignorada pelo usuário.',
            )
            return

        if not self._sugestao_ainda_valida(alerta):
            self._finalizar_sugestao(
                alerta, 'NAO_MAIS_ALTO',
                'O sistema normalizou ou saiu de CRÍTICO; encerramento cancelado.',
            )
            return

        alerta['action_status'] = 'PREPARANDO'
        self._renderizar_notificacoes()
        self._renderizar_acoes_pendentes()
        threading.Thread(
            target=self._preparar_grupo_async,
            args=(alerta,),
            daemon=True,
        ).start()

    def _preparar_grupo_async(self, alerta):
        processo = alerta['process']
        status, mensagem, grupo = preparar_grupo_processo(
            processo['pid'], processo['create_time'], processo['name'],
        )
        try:
            self.root.after(
                0, self._confirmar_grupo_preparado,
                alerta, status, mensagem, grupo,
            )
        except (tk.TclError, RuntimeError):
            debug_log.registrar(
                'process_action', 'grupo_preparado_sem_interface', nivel='WARNING',
                alert_id=alerta['alert_id'], status=status, result=mensagem,
            )

    def _confirmar_grupo_preparado(
        self, alerta, status, mensagem, grupo, aviso=''
    ):
        if alerta.get('action_status') != 'PREPARANDO':
            return
        if not self._sugestao_ainda_valida(alerta):
            self._finalizar_sugestao(
                alerta, 'NAO_MAIS_ALTO',
                'O sistema normalizou enquanto o grupo era verificado.',
            )
            return
        if status != 'OK':
            self._finalizar_sugestao(alerta, status, mensagem)
            return

        processo = alerta['process']
        membros = grupo['processes']
        lista_membros = ', '.join(
            f"{membro['name']} (PID {membro['pid']})"
            for membro in membros
        )
        prefixo_aviso = f'{aviso}\n\n' if aviso else ''
        confirmar = messagebox.askyesno(
            'Confirmar encerramento',
            f"{prefixo_aviso}Deseja encerrar todas as instâncias de {processo['name']} e seus "
            f"processos filhos? Foram encontrados {len(membros)} processo(s).\n\n"
            f"Executável: {grupo['exe_path']}\n"
            f"Grupo: {lista_membros}\n\n"
            f"Uso observado: CPU {processo['cpu_percent']:.1f}% · "
            f"RAM {processo['ram_percent']:.1f}%.\n\n"
            'Todas as instâncias deste executável serão fechadas. O encerramento '
            'forçado pode causar perda de dados não salvos.',
            icon='warning',
            parent=self.root,
        )
        if not confirmar:
            debug_log.registrar(
                'process_action', 'encerramento_cancelado',
                alert_id=alerta['alert_id'], pid=processo['pid'],
                process_name=processo['name'], reason='usuario_cancelou_confirmacao',
            )
            self._finalizar_sugestao(
                alerta, 'IGNORADO', 'Encerramento cancelado pelo usuário.',
            )
            return

        alerta['action_status'] = 'ENCERRANDO'
        alerta['_grupo_processos'] = [
            {chave: membro[chave] for chave in ('pid', 'name', 'create_time')}
            for membro in membros
        ]
        self._renderizar_notificacoes()
        self._renderizar_acoes_pendentes()
        threading.Thread(
            target=self._encerrar_processo_async,
            args=(alerta, grupo),
            daemon=True,
        ).start()

    def _sistema_acima_dos_limites(self):
        amostra = self.coletor.ultima()
        return bool(
            amostra is not None
            and (
                amostra.cpu >= self.limites['cpu_global']
                or amostra.ram >= self.limites['ram_global']
            )
        )

    def _sugestao_ainda_valida(self, alerta):
        return bool(
            self.state_store is not None
            and self.state_store.estado_atual(alerta['host_id']) == 'CRÍTICO'
            and self._sistema_acima_dos_limites()
        )

    def _aguardar_nova_amostra(self, posterior_a):
        limite = time.monotonic() + TEMPO_REAVALIACAO_PROCESSO
        while time.monotonic() < limite and not getattr(self, '_app_fechando', False):
            amostra = self.coletor.ultima()
            if amostra is not None and getattr(amostra, 'inicio_monotonic', 0) > posterior_a:
                return amostra
            time.sleep(0.1)
        return None

    def _encerrar_processo_async(self, alerta, grupo=None):
        processo = alerta['process']
        if not self._sugestao_ainda_valida(alerta):
            status, mensagem = (
                'NAO_MAIS_ALTO',
                'O sistema normalizou antes do encerramento; ação cancelada.',
            )
        else:
            if grupo is None:
                status, mensagem, grupo = preparar_grupo_processo(
                    processo['pid'], processo['create_time'], processo['name'],
                )
                if status == 'OK':
                    status, mensagem = terminar_grupo_processo(grupo)
            else:
                status, mensagem = terminar_grupo_processo(grupo)
        if status == 'GRUPO_ALTERADO':
            try:
                self.root.after(
                    0, self._reconfirmar_grupo_alterado,
                    alerta, grupo, mensagem,
                )
            except (tk.TclError, RuntimeError):
                debug_log.registrar(
                    'process_action', 'grupo_alterado_sem_interface', nivel='WARNING',
                    alert_id=alerta['alert_id'], result=mensagem,
                )
            return
        if grupo is not None:
            alerta['_grupo_processos'] = [
                {chave: membro[chave] for chave in ('pid', 'name', 'create_time')}
                for membro in grupo['processes']
            ]
        fim_encerramento = time.monotonic()
        if status in {'ENCERRADO', 'JA_ENCERRADO'} and not getattr(
            self, '_app_fechando', False,
        ):
            tentados = set(alerta.get('_processos_tentados', ()))
            tentados.update(
                (membro['pid'], membro['create_time'])
                for membro in (grupo or {}).get('processes', ())
            )
            if grupo:
                tentados.add(('exe', grupo['exe_path']))
            if not grupo:
                tentados.add((processo['pid'], processo['create_time']))
            amostra = self._aguardar_nova_amostra(fim_encerramento)
            if amostra is None:
                mensagem += ' Não foi possível obter uma nova amostra para reavaliar.'
            elif amostra.cpu < self.limites['cpu_global'] and amostra.ram < self.limites['ram_global']:
                mensagem += ' CPU e RAM voltaram abaixo dos limites configurados.'
            else:
                proximo = _escolher_processo(amostra, self.limites, tentados)
                if proximo is None:
                    mensagem += (
                        ' O sistema continua acima dos limites, mas não há outro '
                        'processo seguro entre os candidatos atuais.'
                    )
                else:
                    alerta_seguinte = {
                        'alert_id': str(uuid.uuid4()),
                        'alert_type': 'process_recommendation',
                        'host_id': alerta['host_id'],
                        'level': 'CRITICAL',
                        'source': 'reavaliacao_pos_encerramento',
                        'message': (
                            f"Consumo ainda acima dos limites: CPU {amostra.cpu:.1f}% "
                            f"(limite {self.limites['cpu_global']:g}%), RAM "
                            f"{amostra.ram:.1f}% (limite "
                            f"{self.limites['ram_global']:g}%). Próxima sugestão: "
                            f"{proximo['name']} (PID {proximo['pid']})."
                        ),
                        'received_at': datetime.now().astimezone().isoformat(),
                        'process': proximo,
                        '_processos_tentados': tentados,
                    }
                    self._receber_alerta(alerta_seguinte)
                    mensagem += (
                        f" O sistema continua acima dos limites; uma nova sugestão "
                        f"para {proximo['name']} (PID {proximo['pid']}) foi enviada."
                    )
        try:
            self.root.after(
                0, self._finalizar_sugestao, alerta, status, mensagem,
            )
        except (tk.TclError, RuntimeError):
            debug_log.registrar(
                'process_action', 'resultado_sem_interface', nivel='WARNING',
                alert_id=alerta['alert_id'], status=status, result=mensagem,
            )

    def _reconfirmar_grupo_alterado(self, alerta, grupo, mensagem):
        if (
            getattr(self, '_app_fechando', False)
            or alerta.get('action_status') != 'ENCERRANDO'
        ):
            return
        alerta['action_status'] = 'PREPARANDO'
        alerta['_grupo_processos'] = [
            {chave: membro[chave] for chave in ('pid', 'name', 'create_time')}
            for membro in grupo['processes']
        ]
        self._renderizar_notificacoes()
        self._renderizar_acoes_pendentes()
        self._confirmar_grupo_preparado(
            alerta, 'OK', '', grupo, aviso=mensagem,
        )

    def _finalizar_sugestao(self, alerta, status, mensagem):
        alerta['action_status'] = status
        alerta['lida'] = True
        processo = alerta['process']
        debug_log.registrar(
            'process_action', 'decisao_registrada', alert_id=alerta['alert_id'],
            host_id=alerta['host_id'], pid=processo['pid'],
            process_name=processo['name'], create_time=processo['create_time'],
            group_processes=alerta.get('_grupo_processos'),
            status=status, result=mensagem,
        )
        self._registrar_historico(
            'Decisões',
            f"{processo['name']} (PID {processo['pid']}): {status}. {mensagem}",
            nivel=(
                'INFO'
                if status in {'IGNORADO', 'ENCERRADO', 'JA_ENCERRADO', 'NAO_MAIS_ALTO'}
                else 'WARNING'
            ),
            origem='usuário',
            host_id=alerta['host_id'],
        )
        self.sino_notificacoes.set_quantidade(
            sum(not item['lida'] for item in self.notificacoes)
        )
        self._renderizar_notificacoes()
        self._renderizar_acoes_pendentes()
        if self._popup_alerta_id == alerta['alert_id'] and self._popup_alerta is not None:
            self._fechar_toast(self._popup_alerta, 'acao_resolvida')

    def _abrir_notificacoes(self):
        for alerta in self.notificacoes:
            alerta['lida'] = True
        self.sino_notificacoes.set_quantidade(0)

        if self._janela_notificacoes is not None:
            try:
                if self._janela_notificacoes.winfo_exists():
                    self._janela_notificacoes.lift()
                    return
            except tk.TclError:
                pass

        janela = tk.Toplevel(self.root)
        self._janela_notificacoes = janela
        janela.title('Histórico da sessão · Cyber Sentinel')
        janela.geometry('540x520')
        janela.minsize(420, 340)
        janela.configure(bg=theme.BG)
        janela.transient(self.root)

        cabecalho = tk.Frame(janela, bg=theme.BG)
        cabecalho.pack(fill='x', padx=18, pady=(16, 12))
        tk.Label(
            cabecalho, text='Histórico da sessão', font=('Segoe UI Semibold', 13),
            fg=theme.FG, bg=theme.BG,
        ).pack(side='left')
        fechar = tk.Label(
            cabecalho, text='×', font=('Segoe UI', 18),
            fg=theme.FG_MUTED, bg=theme.BG, cursor='hand2',
        )
        fechar.pack(side='right')
        fechar.bind('<Button-1>', lambda _e: janela.destroy())

        filtros = tk.Frame(janela, bg=theme.BG)
        filtros.pack(fill='x', padx=18, pady=(0, 10))
        self._historico_filtro = tk.StringVar(janela, value='Todos')
        filtro = ttk.Combobox(
            filtros,
            textvariable=self._historico_filtro,
            values=('Todos', 'Diagnósticos', 'Decisões', 'Notificações', 'Sistema'),
            state='readonly',
            width=17,
            style='Historico.TCombobox',
        )
        filtro.pack(side='left', padx=(0, 10))
        filtro.bind('<<ComboboxSelected>>', self._filtrar_historico)

        self._historico_busca = tk.StringVar(janela)
        busca = ttk.Entry(filtros, textvariable=self._historico_busca)
        busca.pack(side='left', fill='x', expand=True)
        busca.insert(0, '')
        busca.bind('<KeyRelease>', self._filtrar_historico)

        self._historico_acoes = tk.Frame(janela, bg=theme.BG)
        self._historico_acoes.pack(fill='x', padx=18, pady=(0, 8))
        self._renderizar_acoes_pendentes()

        conteudo = tk.Frame(janela, bg=theme.SURFACE)
        conteudo.pack(fill='both', expand=True, padx=18, pady=(0, 10))
        barra = tk.Scrollbar(conteudo, orient='vertical')
        texto = tk.Text(
            conteudo,
            wrap='word',
            bg=theme.SURFACE,
            fg=theme.FG,
            font=('Segoe UI', 9),
            relief='flat',
            borderwidth=0,
            state='disabled',
            yscrollcommand=barra.set,
            padx=12,
            pady=10,
        )
        barra.configure(command=texto.yview)
        barra.pack(side='right', fill='y')
        texto.pack(side='left', fill='both', expand=True)
        texto.tag_configure('meta', foreground=theme.FG_MUTED, font=('Segoe UI', 8, 'bold'))
        texto.tag_configure('critical', foreground=theme.ERR, font=('Segoe UI', 9, 'bold'))
        texto.tag_configure('warning', foreground=theme.WARN, font=('Segoe UI', 9, 'bold'))
        texto.tag_configure('info', foreground=theme.ACCENT, font=('Segoe UI', 9, 'bold'))
        texto.tag_configure('decision', foreground=theme.ACCENT, font=('Segoe UI', 9, 'bold'))
        texto.tag_configure('message', foreground=theme.FG, spacing3=10)
        self._texto_notificacoes = texto
        self._renderizar_notificacoes()

        rodape = tk.Frame(janela, bg=theme.BG)
        rodape.pack(fill='x', padx=18, pady=(0, 16))
        self._historico_btn_novos = PillButton(
            rodape, 'Mais recentes', lambda: self._mudar_pagina_historico(-100),
        )
        self._historico_btn_novos.pack(side='left', padx=(0, 8))
        self._historico_status = tk.Label(
            rodape, text='', font=('Segoe UI', 8),
            fg=theme.FG_MUTED, bg=theme.BG,
        )
        self._historico_status.pack(side='left', fill='x', expand=True)
        self._historico_btn_antigos = PillButton(
            rodape, 'Mais antigos', lambda: self._mudar_pagina_historico(100),
        )
        self._historico_btn_antigos.pack(side='right')
        self._historico_offset = 0
        self._renderizar_notificacoes()

        def ao_destruir(evento):
            if evento.widget is janela:
                self._janela_notificacoes = None
                self._texto_notificacoes = None
                self._historico_filtro = None
                self._historico_busca = None
                self._historico_status = None
                self._historico_btn_novos = None
                self._historico_btn_antigos = None
                self._historico_acoes = None

        janela.bind('<Destroy>', ao_destruir)

    def _filtrar_historico(self, _evento=None):
        self._historico_offset = 0
        self._renderizar_notificacoes()

    def _mudar_pagina_historico(self, deslocamento):
        self._historico_offset = max(0, self._historico_offset + deslocamento)
        self._renderizar_notificacoes()

    def _renderizar_acoes_pendentes(self):
        painel = self._historico_acoes
        if painel is None:
            return
        try:
            if not painel.winfo_exists():
                return
            for widget in painel.winfo_children():
                widget.destroy()
            pendentes = [
                alerta for alerta in self.notificacoes
                if alerta.get('process')
                and alerta.get('action_status') in {
                    'PENDENTE', 'PREPARANDO', 'ENCERRANDO',
                }
            ]
            if not pendentes:
                return
            tk.Label(
                painel, text='Sugestões de processo (relacionadas ao alerta global)',
                font=('Segoe UI Semibold', 9), fg=theme.FG, bg=theme.BG,
            ).pack(anchor='w', pady=(0, 5))
            for alerta in pendentes:
                processo = alerta['process']
                cartao = tk.Frame(painel, bg=theme.SURFACE, padx=9, pady=7)
                cartao.pack(fill='x', pady=(0, 5))
                tk.Label(
                    cartao,
                    text=(
                        f"{processo['name']} (PID {processo['pid']}) · "
                        f"CPU {processo['cpu_percent']:.1f}% · "
                        f"RAM {processo['ram_percent']:.1f}%"
                    ),
                    font=('Segoe UI', 8), fg=theme.FG, bg=theme.SURFACE,
                    anchor='w',
                ).pack(side='left', fill='x', expand=True)
                if alerta['action_status'] in {'PREPARANDO', 'ENCERRANDO'}:
                    tk.Label(
                        cartao,
                        text=(
                            'Verificando grupo…'
                            if alerta['action_status'] == 'PREPARANDO'
                            else 'Encerrando grupo…'
                        ),
                        font=('Segoe UI', 8),
                        fg=theme.WARN, bg=theme.SURFACE,
                    ).pack(side='right')
                else:
                    tk.Button(
                        cartao, text='Encerrar',
                        command=lambda a=alerta: self._resolver_sugestao(
                            a['alert_id'], 'ENCERRAR',
                        ),
                        bg=theme.ERR, fg='#ffffff', activebackground=theme.ERR,
                        activeforeground='#ffffff', relief='flat', borderwidth=0,
                        padx=8, pady=3, cursor='hand2',
                    ).pack(side='right', padx=(6, 0))
                    tk.Button(
                        cartao, text='Ignorar',
                        command=lambda a=alerta: self._resolver_sugestao(
                            a['alert_id'], 'IGNORAR',
                        ),
                        bg=theme.SURFACE_2, fg=theme.FG,
                        activebackground=theme.SURFACE_2,
                        activeforeground=theme.FG, relief='flat', borderwidth=0,
                        padx=8, pady=3, cursor='hand2',
                    ).pack(side='right')
        except tk.TclError:
            return

    def _renderizar_notificacoes(self):
        texto = self._texto_notificacoes
        if texto is None or self._historico_filtro is None:
            return
        try:
            if not texto.winfo_exists():
                return
            registros, total = self.historico_sessao.buscar(
                categoria=self._historico_filtro.get(),
                busca=self._historico_busca.get() if self._historico_busca else '',
                limite=100,
                deslocamento=self._historico_offset,
            )
            texto.configure(state='normal')
            texto.delete('1.0', 'end')
            if not registros:
                texto.insert('end', 'Nenhum registro nesta sessão.', 'meta')
            else:
                for registro in registros:
                    categoria = registro['categoria']
                    nivel = registro['nivel']
                    tag = (
                        'critical' if nivel == 'CRITICAL'
                        else 'warning' if nivel == 'WARNING'
                        else 'decision' if categoria == 'Decisões'
                        else 'info'
                    )
                    metadados = [registro['ocorrido_em'], categoria]
                    if nivel:
                        metadados.append(nivel)
                    if registro['host_id']:
                        metadados.append(registro['host_id'])
                    if registro['origem']:
                        metadados.append(registro['origem'])
                    texto.insert(
                        'end',
                        ' · '.join(metadados) + '\n',
                        'meta',
                    )
                    texto.insert(
                        'end', registro['mensagem'] + '\n\n',
                        ('message', tag),
                    )
            texto.configure(state='disabled')
            if self._historico_status is not None:
                inicio = min(self._historico_offset + 1, total) if total else 0
                fim = min(self._historico_offset + len(registros), total)
                self._historico_status.config(
                    text=f'{inicio}–{fim} de {total}'
                )
            if self._historico_btn_novos is not None:
                self._historico_btn_novos.set_enabled(self._historico_offset > 0)
            if self._historico_btn_antigos is not None:
                self._historico_btn_antigos.set_enabled(
                    self._historico_offset + len(registros) < total
                )
        except tk.TclError:
            self._texto_notificacoes = None

    def _atualizar_log(self):
        while True:
            try:
                msg = self.fila_log.get_nowait()
            except queue.Empty:
                break
            categoria, nivel, origem = self._classificar_log(msg)
            self._registrar_historico(
                categoria, msg, nivel=nivel, origem=origem,
            )
            self.log.config(state='normal')
            self.log.insert('end', msg + '\n')
            self.log.see('end')
            self.log.config(state='disabled')
        self.root.after(150, self._atualizar_log)

    def _ao_fechar(self):
        self._app_fechando = True
        if self._iniciando and self._thread_preparacao is not None:
            self._thread_preparacao.join(timeout=55.0)
        self._parar('fechamento_da_janela')
        self.historico_sessao.fechar()
        self.root.destroy()


def main():
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == '__main__':
    if '--consumer' in sys.argv[1:]:
        from consumer.main import consumir
        consumir()
    else:
        main()
