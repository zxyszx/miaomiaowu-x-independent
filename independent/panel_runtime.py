"""Durable admission guard for a single panel HTTP process.

The running marker precedes serving. An unclean exit requires offline quarantine
restore, even if the last identity anchor happens to still look clean.
"""
import fcntl
import json
import os
import stat
import tempfile

from core import Error


class PanelRuntime:
    def __init__(self, identity):
        self.identity = identity
        self.fd = None
        self.running = False
        try:
            with identity._lock():
                self.fd = os.open(identity.control/'runtime.lock', os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW, 0o600)
                info = os.fstat(self.fd)
                if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600 or not stat.S_ISREG(info.st_mode):
                    raise Error('运行锁权限无效', 503)
                fcntl.flock(self.fd, fcntl.LOCK_EX|fcntl.LOCK_NB)
                anchor = identity._read_anchor()
                db = identity._db()
                try: identity._verify(db, anchor)
                finally: db.close()
                marker = identity.control/'running.json'
                if marker.exists() or marker.is_symlink():
                    fd = os.open(marker, os.O_RDONLY|os.O_NOFOLLOW)
                    with os.fdopen(fd) as file:
                        info = os.fstat(file.fileno())
                        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600 or not stat.S_ISREG(info.st_mode):
                            raise Error('运行标记权限无效', 503)
                        previous = json.load(file)
                    generation = anchor['restore_generation']
                    if (previous['deployment_id'] != anchor['deployment_id'] or
                        previous['restore_generation'] == generation or
                        {'event':'quarantine_complete', 'id':generation} not in anchor.get('recoveries', [])):
                        raise Error('上次运行未安全结束，必须停服修复存储并受控隔离恢复', 503)
                self._mark({key:anchor[key] for key in ('deployment_id','restore_generation')})
                self.running = True
        except (Error, OSError, ValueError, KeyError, TypeError) as exc:
            if self.fd is not None: os.close(self.fd); self.fd = None
            if isinstance(exc, Error): raise
            raise Error('面板运行保护不可用', 503) from exc

    def _mark(self, value):
        fd, name = tempfile.mkstemp(prefix='.running-', dir=self.identity.control)
        try:
            with os.fdopen(fd, 'w') as file:
                json.dump(value, file); file.flush(); os.fsync(file.fileno())
            os.replace(name, self.identity.control/'running.json')
            self._sync()
        finally:
            if os.path.exists(name): os.unlink(name)

    def _sync(self):
        fd = os.open(self.identity.control, os.O_RDONLY)
        try: os.fsync(fd)
        finally: os.close(fd)

    def invoke(self, operation):
        try: return operation()
        except Error as error:
            if error.status == 503:
                # No attempted cleanup/write is needed for the durable fence.
                os._exit(78)
            raise

    def close(self):
        """Only after HTTP workers have stopped and joined; never on fatal exit."""
        if not self.running: return
        try:
            with self.identity._lock():
                db = self.identity._db()
                try: self.identity._verify(db, self.identity._read_anchor())
                finally: db.close()
                os.unlink(self.identity.control/'running.json'); self._sync()
        except (Error, OSError):
            os._exit(78)
        self.running = False
        os.close(self.fd); self.fd = None
