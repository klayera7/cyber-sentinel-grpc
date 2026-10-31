import os
import queue
import sys
import threading
import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk

_RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (_RAIZ, os.path.join(_RAIZ, 'proto')):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from server.main import criar_servidor
from app import charts, markdown, ollama_check, theme
from app.coletor import Coletor


def _round_rect(canvas, x1, y1, x2, y2, r, **kw):
    pontos = [
        x1 + r, y1, x2 - r, y1, x2, y1, x2, y1 + r,
        x2, y2 - r, x2, y2, x2 - r, y2, x1 + r, y2,
        x1, y2, x1, y2 - r, x1, y1 + r, x1, y1,
    ]
    return canvas.create_polygon(pontos, smooth=True, **kw)


class Pill(tk.Canvas):
    def __init__(self, parent, texto, cor, altura=26):
        self._fonte = tkfont.Font(family='Segoe UI', size=9, weight='bold')
        self._texto = texto
        self._cor = cor
        self._altura = altura
        super().__init__(parent, height=altura, bg=theme.BG, highlightthickness=0)
        self._render()

    def _largura(self):
        return self._fonte.measure(self._texto) + 40

    def _render(self):
        w = self._largura()
        h = self._altura
        self.config(width=w)
        self.delete('all')
        _round_rect(self, 1, 1, w - 1, h - 1, h // 2, fill=theme.SURFACE, outline='')
        self.create_oval(12, h // 2 - 4, 12 + 8, h // 2 + 4, fill=self._cor, outline='')
        self.create_text(27, h // 2, text=self._texto, fill=theme.FG, font=self._fonte, anchor='w')

    def set(self, texto, cor):
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


class App:
    def __init__(self, root):
        self.root = root
        self.root.title('Cyber Sentinel')
        self.root.geometry('1160x760')
        self.root.minsize(980, 640)
        self.root.configure(bg=theme.BG)

        self.fila_log = queue.Queue()
        self.servidor = None
        self.coletor = Coletor(intervalo=2.0, on_erro=self._log)

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

        tk.Label(cfg, text='MODELO', font=('Segoe UI', 8), fg=theme.FG_MUTED, bg=theme.BG).grid(row=0, column=0, sticky='w')
        self.ent_modelo = ttk.Entry(cfg, width=16)
        self.ent_modelo.insert(0, os.getenv('OLLAMA_MODEL', 'llama3.1'))
        self.ent_modelo.grid(row=1, column=0, sticky='w', padx=(0, 20))

        tk.Label(cfg, text='HOST OLLAMA', font=('Segoe UI', 8), fg=theme.FG_MUTED, bg=theme.BG).grid(row=0, column=1, sticky='w')
        self.ent_host = ttk.Entry(cfg, width=30)
        self.ent_host.insert(0, os.getenv('OLLAMA_HOST', 'http://127.0.0.1:11434'))
        self.ent_host.grid(row=1, column=1, sticky='w', padx=(0, 20))

        tk.Label(cfg, text='PORTA gRPC', font=('Segoe UI', 8), fg=theme.FG_MUTED, bg=theme.BG).grid(row=0, column=2, sticky='w')
        self.ent_porta = ttk.Entry(cfg, width=10)
        self.ent_porta.insert(0, os.getenv('GRPC_PORT', '50051'))
        self.ent_porta.grid(row=1, column=2, sticky='w')

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
        if self.servidor is not None:
            self._log('O servidor já está em execução.')
            return
        try:
            porta = self._porta()
            modelo = self._modelo()
            host = self._host()

            os.environ['OLLAMA_MODEL'] = modelo
            os.environ['OLLAMA_HOST'] = host

            self.servidor = criar_servidor(porta)
            self._log(f'Servidor gRPC iniciado na porta {porta}.')

            self.coletor.iniciar(porta=porta)
            self._log('Coleta de telemetria iniciada.')

            self._set_pill(self.pill_servidor, 'Servidor', 'ativo', theme.OK)
            self.btn_iniciar.set_enabled(False)
            self.btn_parar.set_enabled(True)

            self._verificar_ollama_async()
        except Exception as e:
            self._log(f'Falha ao iniciar: {e}')
            if self.servidor is not None:
                try:
                    self.servidor.stop(grace=0)
                except Exception:
                    pass
                self.servidor = None

    def _parar(self):
        self.coletor.parar()
        self._log('Coleta de telemetria parada.')

        if self.servidor is not None:
            try:
                self.servidor.stop(grace=1)
            except Exception as e:
                self._log(f'Aviso ao parar o servidor: {e}')
            self.servidor = None
            self._log('Servidor gRPC parado.')

        self._set_pill(self.pill_servidor, 'Servidor', 'parado', theme.ERR)
        self.btn_iniciar.set_enabled(True)
        self.btn_parar.set_enabled(False)

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

    def _agendar(self):
        self._atualizar_graficos()
        self._atualizar_diagnosticos()
        self._atualizar_log()

    def _atualizar_graficos(self):
        self.graficos.atualizar(self.coletor.historico)
        self.root.after(500, self._atualizar_graficos)

    def _atualizar_diagnosticos(self):
        for ts, texto in self.coletor.consumir_diagnosticos():
            self.feed.config(state='normal')
            self.feed.insert('end', f'{ts}\n', ('card', 'hora'))
            markdown.inserir(self.feed, texto, ('card',))
            self.feed.insert('end', '\n')
            self.feed.config(state='disabled')
            self.feed.see('end')
        self.root.after(150, self._atualizar_diagnosticos)

    def _atualizar_log(self):
        while True:
            try:
                msg = self.fila_log.get_nowait()
            except queue.Empty:
                break
            self.log.config(state='normal')
            self.log.insert('end', msg + '\n')
            self.log.see('end')
            self.log.config(state='disabled')
        self.root.after(150, self._atualizar_log)

    def _ao_fechar(self):
        self._parar()
        self.root.destroy()


def main():
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == '__main__':
    main()
