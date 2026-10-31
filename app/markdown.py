import re

_INLINE = re.compile(r'(\*\*.+?\*\*|`[^`]+?`|\*[^*\n]+?\*)')


def _inserir_inline(widget, texto, base):
    pos = 0
    for m in _INLINE.finditer(texto):
        if m.start() > pos:
            widget.insert('end', texto[pos:m.start()], base)
        trecho = m.group(0)
        if trecho.startswith('**') and trecho.endswith('**'):
            widget.insert('end', trecho[2:-2], (*base, 'bold'))
        elif trecho.startswith('`') and trecho.endswith('`'):
            widget.insert('end', trecho[1:-1], (*base, 'code'))
        else:
            widget.insert('end', trecho[1:-1], (*base, 'italic'))
        pos = m.end()
    if pos < len(texto):
        widget.insert('end', texto[pos:], base)


def inserir(widget, texto, base=()):
    linhas = (texto or '').splitlines()
    for linha in linhas:
        if linha.startswith('### '):
            widget.insert('end', linha[4:] + '\n', (*base, 'h3'))
        elif linha.startswith('## '):
            widget.insert('end', linha[3:] + '\n', (*base, 'h2'))
        elif linha.startswith('# '):
            widget.insert('end', linha[2:] + '\n', (*base, 'h1'))
        elif re.match(r'^\s*\d+[.)]\s+', linha):
            marc = re.match(r'^\s*\d+[.)]\s+', linha).group(0)
            resto = linha[len(marc):]
            widget.insert('end', '  ' + marc, (*base, 'li'))
            _inserir_inline(widget, resto + '\n', (*base, 'li'))
        elif re.match(r'^\s*[-*•]\s+', linha):
            marc = re.match(r'^\s*[-*•]\s+', linha).group(0)
            resto = linha[len(marc):]
            widget.insert('end', '  •  ', (*base, 'li'))
            _inserir_inline(widget, resto + '\n', (*base, 'li'))
        elif not linha.strip():
            widget.insert('end', '\n', base)
        else:
            _inserir_inline(widget, linha + '\n', base)
