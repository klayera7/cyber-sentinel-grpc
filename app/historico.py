import sqlite3
from datetime import datetime


class HistoricoSessao:
    def __init__(self):
        self._db = sqlite3.connect(':memory:')
        self._db.row_factory = sqlite3.Row
        self._db.execute(
            '''CREATE TABLE registros (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   ocorrido_em TEXT NOT NULL,
                   categoria TEXT NOT NULL,
                   nivel TEXT NOT NULL,
                   origem TEXT NOT NULL,
                   host_id TEXT NOT NULL,
                   mensagem TEXT NOT NULL
               )'''
        )
        self._db.execute(
            'CREATE INDEX idx_registros_categoria_id ON registros(categoria, id DESC)'
        )
        self._db.commit()

    def registrar(
        self, categoria, mensagem, ocorrido_em=None, nivel='INFO',
        origem='', host_id='',
    ):
        ocorrido_em = ocorrido_em or datetime.now().astimezone().isoformat(
            timespec='seconds'
        )
        cursor = self._db.execute(
            '''INSERT INTO registros
               (ocorrido_em, categoria, nivel, origem, host_id, mensagem)
               VALUES (?, ?, ?, ?, ?, ?)''',
            (
                ocorrido_em,
                categoria,
                nivel,
                origem,
                host_id,
                str(mensagem),
            ),
        )
        self._db.commit()
        return cursor.lastrowid

    def buscar(self, categoria='Todos', busca='', limite=100, deslocamento=0):
        filtros = []
        parametros = []
        if categoria != 'Todos':
            filtros.append('categoria = ?')
            parametros.append(categoria)
        busca = (busca or '').strip()
        if busca:
            filtros.append(
                '(mensagem LIKE ? OR origem LIKE ? OR host_id LIKE ? OR nivel LIKE ?)'
            )
            termo = f'%{busca}%'
            parametros.extend((termo, termo, termo, termo))

        where = f" WHERE {' AND '.join(filtros)}" if filtros else ''
        total = self._db.execute(
            f'SELECT COUNT(*) FROM registros{where}', parametros
        ).fetchone()[0]
        linhas = self._db.execute(
            f'''SELECT ocorrido_em, categoria, nivel, origem, host_id, mensagem
                FROM registros{where}
                ORDER BY id DESC LIMIT ? OFFSET ?''',
            (*parametros, limite, deslocamento),
        ).fetchall()
        return [dict(linha) for linha in linhas], total

    def fechar(self):
        self._db.close()
