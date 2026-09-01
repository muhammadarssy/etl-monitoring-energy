"""Koneksi database source & datamart."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

import psycopg2
import psycopg2.extensions
import structlog

log = structlog.get_logger(__name__)


class Database:
    def __init__(self, dsn: str, label: str):
        self.dsn = dsn
        self.label = label
        self._conn: Optional[psycopg2.extensions.connection] = None

    def connect(self) -> None:
        self._conn = psycopg2.connect(self.dsn)
        self._conn.autocommit = False
        log.info("db_connected", db=self.label)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            log.info("db_closed", db=self.label)

    @property
    def conn(self) -> psycopg2.extensions.connection:
        if self._conn is None:
            raise RuntimeError(f"Database {self.label} belum connect")
        return self._conn

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    @contextmanager
    def cursor(self) -> Generator[psycopg2.extensions.cursor, None, None]:
        cur = self.conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def execute_sql_file(self, path: Path) -> None:
        sql = path.read_text(encoding="utf-8")
        with self.cursor() as cur:
            cur.execute(sql)
        self.commit()
        log.info("schema_applied", db=self.label, path=str(path))
