import sqlite3
import time
import os

from messaging.config import caminho_dados


class OutboxSQLite:
    def __init__(self, caminho=None):
        self.caminho = caminho or caminho_dados(
            'SENTINEL_OUTBOX_DB', 'rabbit_outbox.sqlite3'
        )
        os.makedirs(os.path.dirname(os.path.abspath(self.caminho)), exist_ok=True)
        self._inicializar()

    def _conectar(self):
        conexao = sqlite3.connect(self.caminho, timeout=10)
        conexao.row_factory = sqlite3.Row
        conexao.execute('PRAGMA busy_timeout=10000')
        conexao.execute('PRAGMA synchronous=FULL')
        return conexao

    def _inicializar(self):
        conexao = self._conectar()
        try:
            conexao.execute('PRAGMA journal_mode=WAL')
            conexao.execute(
                '''CREATE TABLE IF NOT EXISTS outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    routing_key TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT
                )'''
            )
            conexao.commit()
        finally:
            conexao.close()

    def adicionar(self, event_id, routing_key, payload):
        conexao = self._conectar()
        try:
            conexao.execute(
                '''INSERT OR IGNORE INTO outbox
                   (event_id, routing_key, payload, created_at)
                   VALUES (?, ?, ?, ?)''',
                (event_id, routing_key, payload, time.time()),
            )
            conexao.commit()
        finally:
            conexao.close()

    def proximo(self):
        conexao = self._conectar()
        try:
            linha = conexao.execute(
                '''SELECT id, event_id, routing_key, payload, attempts
                   FROM outbox ORDER BY id LIMIT 1'''
            ).fetchone()
            return dict(linha) if linha else None
        finally:
            conexao.close()

    def marcar_enviado(self, item_id):
        conexao = self._conectar()
        try:
            conexao.execute('DELETE FROM outbox WHERE id = ?', (item_id,))
            conexao.commit()
        finally:
            conexao.close()

    def registrar_falha(self, item_id, erro):
        conexao = self._conectar()
        try:
            conexao.execute(
                '''UPDATE outbox
                   SET attempts = attempts + 1, last_error = ?
                   WHERE id = ?''',
                (str(erro)[:1000], item_id),
            )
            conexao.commit()
        finally:
            conexao.close()

    def quantidade_pendente(self):
        conexao = self._conectar()
        try:
            linha = conexao.execute(
                'SELECT COUNT(*) AS quantidade FROM outbox'
            ).fetchone()
            return int(linha['quantidade'])
        finally:
            conexao.close()
