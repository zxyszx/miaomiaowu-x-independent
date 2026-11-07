"""Bounded test-only SQLite page limit; never fills the host filesystem."""
from contextlib import contextmanager
from pathlib import Path
import sqlite3
from unittest import mock


@contextmanager
def page_limited_connections(path):
    original=sqlite3.connect
    def connect(database,*args,**kwargs):
        db=original(database,*args,**kwargs)
        try:
            if Path(database)==Path(path):
                pages=db.execute('PRAGMA page_count').fetchone()[0]
                actual=db.execute(f'PRAGMA max_page_count={pages}').fetchone()[0]
                if actual!=pages: raise AssertionError('page limit was not applied')
            return db
        except BaseException:
            db.close()
            raise
    with mock.patch('sqlite3.connect',side_effect=connect): yield
