import matplotlib
matplotlib.use('TkAgg')

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

from app import theme


class Graficos:
    def __init__(self, parent):
        self.figura = Figure(
            figsize=(5, 3.6), dpi=100, tight_layout=True, facecolor=theme.BG
        )
        self.ax_cpu = self.figura.add_subplot(211)
        self.ax_ram = self.figura.add_subplot(212)

        self.canvas = FigureCanvasTkAgg(self.figura, master=parent)
        self.canvas.draw()
        self.widget = self.canvas.get_tk_widget()
        self.widget.configure(background=theme.BG)
        self.widget.bind('<Configure>', self._ao_redimensionar)

    def _ao_redimensionar(self, evento):
        largura = evento.width / 100.0
        altura = evento.height / 100.0
        if largura > 1 and altura > 1:
            self.figura.set_size_inches(largura, altura)
            self.canvas.draw_idle()

    def atualizar(self, historico):
        x = list(range(len(historico)))
        cpu = [amostra[1] for amostra in historico]
        ram = [amostra[2] for amostra in historico]

        cpu_atual = cpu[-1] if cpu else 0.0
        ram_atual = ram[-1] if ram else 0.0

        self._desenhar(self.ax_cpu, x, cpu, f'CPU  {cpu_atual:.1f}%', theme.ACCENT)
        self._desenhar(self.ax_ram, x, ram, f'RAM  {ram_atual:.1f}%', theme.OK)
        self.canvas.draw_idle()

    @staticmethod
    def _desenhar(ax, x, y, titulo, cor):
        ax.clear()
        ax.set_facecolor(theme.BG)
        ax.set_title(titulo, fontsize=12, color=theme.FG, loc='left', pad=10, fontweight='bold')
        ax.set_ylim(0, 100)
        ax.tick_params(colors=theme.FG_MUTED, labelsize=8, length=0)
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.grid(True, axis='y', linestyle='--', alpha=0.18, color=theme.GRID)
        if y:
            ax.plot(x, y, color=cor, linewidth=1.8, solid_capstyle='round')
            ax.fill_between(x, y, 0, color=cor, alpha=0.15, linewidth=0)
