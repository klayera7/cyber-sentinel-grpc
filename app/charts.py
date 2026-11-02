import math

import matplotlib
matplotlib.use('TkAgg')

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

from app import theme

_VAZIO = math.nan
_TEMP_MIN = 35.0
_TEMP_MAX = 100.0


def formatar_pct(valor):
    return f'{valor:.1f}%' if valor is not None else '—'


def formatar_temp(valor):
    return f'{valor:.1f} °C' if valor is not None else '—'


def carga_termica(valor):
    if valor is None:
        return None
    return max(0.0, min(100.0, (valor - _TEMP_MIN) / (_TEMP_MAX - _TEMP_MIN) * 100.0))


def cor_temp(valor):
    if valor is None:
        return theme.FG_MUTED
    if valor >= 75:
        return theme.ERR
    if valor >= 60:
        return theme.WARN
    return theme.OK


def _ultimo(valores):
    for valor in reversed(valores):
        if valor == valor:
            return valor
    return None


def _escala_auto(valores, minimo, maximo, faixa_minima):
    validos = [valor for valor in valores if math.isfinite(valor)]
    if not validos:
        return minimo, maximo

    menor = min(validos)
    maior = max(validos)
    amplitude = maior - menor
    margem = max(amplitude * 0.15, faixa_minima * 0.05)
    inferior = max(minimo, menor - margem)
    superior = min(maximo, maior + margem)

    if superior - inferior < faixa_minima:
        centro = (menor + maior) / 2
        inferior = max(minimo, centro - faixa_minima / 2)
        superior = min(maximo, inferior + faixa_minima)
        inferior = max(minimo, superior - faixa_minima)

    return inferior, superior


class Graficos:
    def __init__(self, parent):
        self.figura = Figure(
            figsize=(5, 4.2), dpi=100, tight_layout=True, facecolor=theme.BG
        )
        self.ax_cpu = self.figura.add_subplot(221)
        self.ax_ram = self.figura.add_subplot(222)
        self.ax_gpu = self.figura.add_subplot(223)
        self.ax_temp = self.figura.add_subplot(224)

        self.canvas = FigureCanvasTkAgg(self.figura, master=parent)
        self.canvas.draw()
        self.widget = self.canvas.get_tk_widget()
        self.widget.configure(background=theme.BG, width=320, height=200)

    def atualizar(self, historico):
        amostras = list(historico)
        x = list(range(len(amostras)))
        cpu = [a.cpu for a in amostras]
        ram = [a.ram for a in amostras]
        gpu = [a.gpu if a.gpu is not None else _VAZIO for a in amostras]
        vram = [a.gpu_mem if a.gpu_mem is not None else _VAZIO for a in amostras]
        temp = [a.temp if a.temp is not None else _VAZIO for a in amostras]

        ultimo_cpu = _ultimo(cpu)
        ultimo_ram = _ultimo(ram)
        ultimo_gpu = _ultimo(gpu)
        ultima_vram = _ultimo(vram)
        ultima_temp = _ultimo(temp)

        titulo_gpu = f'GPU  {formatar_pct(ultimo_gpu)}'
        if ultima_vram is not None:
            titulo_gpu += f' · VRAM {formatar_pct(ultima_vram)}'

        titulo_temp = f'GPU TEMP  {formatar_temp(ultima_temp)}'
        carga = carga_termica(ultima_temp)
        if carga is not None:
            titulo_temp += f' · {carga:.0f}% térmica'

        cor_temperatura = cor_temp(ultima_temp)
        escala_ram = _escala_auto(ram, 0.0, 100.0, 16.0)
        escala_temp = _escala_auto(temp, 0.0, 110.0, 16.0)

        self._desenhar(self.ax_cpu, x, [('CPU', cpu, theme.ACCENT, '-')], f'CPU  {formatar_pct(ultimo_cpu)}')
        self._desenhar(
            self.ax_ram, x, [('RAM', ram, theme.OK, '-')],
            f'RAM  {formatar_pct(ultimo_ram)}', ylim=escala_ram,
        )
        self._desenhar(
            self.ax_gpu, x,
            [('GPU', gpu, theme.GPU, '-'), ('VRAM', vram, theme.WARN, '--')],
            titulo_gpu,
        )
        self._desenhar(
            self.ax_temp, x, [('GPU temp', temp, cor_temperatura, '-')],
            titulo_temp, ylim=escala_temp, cor_titulo=cor_temperatura,
        )
        self.canvas.draw_idle()

    @staticmethod
    def _desenhar(ax, x, series, titulo, ylim=(0, 100), cor_titulo=theme.FG):
        ax.clear()
        ax.set_facecolor(theme.BG)
        ax.set_title(titulo, fontsize=11, color=cor_titulo, loc='left', pad=8, fontweight='bold')
        ax.set_ylim(*ylim)
        ax.tick_params(colors=theme.FG_MUTED, labelsize=8, length=0)
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.grid(True, axis='y', linestyle='--', alpha=0.18, color=theme.GRID)

        com_dados = False
        for rotulo, valores, cor, estilo in series:
            com_dados = com_dados or any(valor == valor for valor in valores)
            ax.plot(x, valores, color=cor, linewidth=1.8, solid_capstyle='round', linestyle=estilo, label=rotulo)
            if estilo == '-':
                ax.fill_between(x, valores, ylim[0], color=cor, alpha=0.15, linewidth=0)

        if len(series) > 1 and com_dados:
            ax.legend(loc='upper right', fontsize=7, frameon=False, labelcolor=theme.FG_MUTED, ncol=2)
