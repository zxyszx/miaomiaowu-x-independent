"""Internal local enforcement of one finite allocation, not an issuance protocol.

The caller must eventually authenticate issuance and execution separately. This
adapter is deliberately absent from Agent configuration. Pre-write reservations
are durable high waters, including uncertain writes, never confirmed traffic.
"""
from contextlib import closing
import json
import math
from pathlib import Path
import sqlite3
import time
import uuid

from quota_store import PAYLOAD_LOCK_TIMEOUT


FIELDS = {'allocation_id', 'policy_id', 'cycle_id', 'server_id', 'session_id',
          'node_id', 'user_id', 'credential', 'reserved_bytes', 'issued_at', 'valid_until'}
MAX_COUNTER = 2**63 - 1


class AllocationQuota:
    def __init__(self, path, scope, *, constraints=None):
        if not isinstance(scope, dict) or set(scope) != FIELDS:
            raise ValueError('额度预留身份字段无效')
        for key in ('policy_id', 'server_id', 'node_id', 'user_id', 'reserved_bytes'):
            if type(scope[key]) is not int or not 1 <= scope[key] <= MAX_COUNTER:
                raise ValueError('额度预留编号或字节数无效')
        for key in ('allocation_id', 'cycle_id', 'session_id', 'credential'):
            if not isinstance(scope[key], str) or not 1 <= len(scope[key]) <= 128:
                raise ValueError('额度预留身份无效')
        for key in ('issued_at', 'valid_until'):
            if (type(scope[key]) not in (int, float) or not 0 <= scope[key] <= MAX_COUNTER
                    or not math.isfinite(scope[key])):
                raise ValueError('额度预留期限无效')
        if scope['valid_until'] <= scope['issued_at']:
            raise ValueError('额度预留期限无效')
        # Copy the caller's object: later mutations cannot enlarge this budget.
        self._canonical = json.dumps(scope, sort_keys=True, separators=(',', ':'))
        self._scope = json.loads(self._canonical)
        if constraints is not None:
            if (type(constraints) is not dict or set(constraints) != {'digest','upload','download'}
                    or type(constraints['digest']) is not str or len(constraints['digest']) != 64
                    or any(c not in '0123456789abcdef' for c in constraints['digest'])
                    or any(x is not None and (type(x) is not int or x < 0)
                           for x in (constraints['upload'],constraints['download']))):
                raise ValueError('额度执行约束无效')
        self._constraints = json.dumps(constraints,sort_keys=True,separators=(',',':'))
        self.path = Path(path)
        self._inflight = set()
        self._update()

    def validate_binding(self, server_id, key):
        s = self._scope
        if server_id != s['server_id'] or key != (s['node_id'], s['user_id'], s['credential']):
            raise ValueError('载荷身份与额度预留不一致')

    def _update(self, direction=None, count=0, ticket=None, complete=None, freeze=False):
        s = self._scope
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with closing(sqlite3.connect(self.path, timeout=PAYLOAD_LOCK_TIMEOUT)) as db, db:
                db.execute('PRAGMA synchronous=FULL')
                db.execute('BEGIN IMMEDIATE')
                db.execute('CREATE TABLE IF NOT EXISTS allocation_owner(server_id INTEGER NOT NULL)')
                owner = db.execute('SELECT server_id FROM allocation_owner').fetchall()
                if not owner:
                    db.execute('INSERT INTO allocation_owner VALUES(?)', (s['server_id'],))
                elif owner != [(s['server_id'],)]:
                    raise ValueError('额度预留存储服务器身份变化')
                db.execute('''CREATE TABLE IF NOT EXISTS allocation_quota(
                    allocation_id TEXT PRIMARY KEY, scope TEXT NOT NULL,
                    upload INTEGER NOT NULL CHECK(upload>=0),
                    download INTEGER NOT NULL CHECK(download>=0))''')
                db.execute('''CREATE TABLE IF NOT EXISTS allocation_writes(
                    ticket TEXT PRIMARY KEY, allocation_id TEXT NOT NULL,
                    direction INTEGER NOT NULL CHECK(direction IN (0,1)),
                    bytes INTEGER NOT NULL CHECK(bytes>0),
                    successful INTEGER NOT NULL CHECK(successful IN (0,1)))''')
                db.execute('CREATE INDEX IF NOT EXISTS allocation_writes_owner ON allocation_writes(allocation_id)')
                row = db.execute('SELECT scope,upload,download FROM allocation_quota WHERE allocation_id=?',
                                 (s['allocation_id'],)).fetchone()
                if row is not None and row[0] != self._canonical:
                    raise ValueError('同一额度预留不可更换身份、版本、期限或上限')
                db.execute('''CREATE TABLE IF NOT EXISTS allocation_constraints(
                    allocation_id TEXT PRIMARY KEY, constraints_json TEXT NOT NULL)''')
                bound=db.execute('SELECT constraints_json FROM allocation_constraints WHERE allocation_id=?',
                                 (s['allocation_id'],)).fetchone()
                if bound is not None and bound[0] != self._constraints:
                    raise ValueError('同一额度执行约束不可替换或移除')
                if bound is None:
                    # Never retrofit a different policy onto an already spent
                    # allocation. Old unbound rows retain their original mode.
                    if row is not None and self._constraints != 'null':
                        raise ValueError('已有额度不能补造执行约束')
                    db.execute('INSERT INTO allocation_constraints VALUES(?,?)',
                               (s['allocation_id'],self._constraints))
                used = list(row[1:] if row else (0, 0))
                if any(type(n) is not int or n < 0 for n in used) or sum(used) > s['reserved_bytes']:
                    raise ValueError('额度预留存储计数超过上限')
                allowed = 0
                if direction is not None and s['issued_at'] <= time.time() < s['valid_until']:
                    allowed = min(count, s['reserved_bytes'] - sum(used))
                    limits=json.loads(self._constraints)
                    if limits is not None:
                        limit=limits[('upload','download')[direction]]
                        if limit is not None: allowed=min(allowed,max(0,limit-used[direction]))
                    used[direction] += allowed
                    if ticket is not None and allowed:
                        db.execute('INSERT INTO allocation_writes VALUES(?,?,?,?,0)',
                                   (ticket, s['allocation_id'], direction, allowed))
                if complete is not None:
                    write = db.execute('SELECT allocation_id,successful FROM allocation_writes WHERE ticket=?',
                                       (complete,)).fetchone()
                    if write is None or write[0] != s['allocation_id']:
                        raise ValueError('写入证据与额度预留不一致')
                    if not write[1]:
                        if complete not in self._inflight:
                            raise ValueError('不能将恢复后的未知写入标记为成功')
                        db.execute('UPDATE allocation_writes SET successful=1 WHERE ticket=?', (complete,))
                attributed = [0, 0]; successful = [0, 0]
                # Aggregate for reads, not on every payload block (quadratic work).
                if direction is None and complete is None:
                    for d, n, ok in db.execute('SELECT direction,bytes,successful FROM allocation_writes WHERE allocation_id=?',
                                              (s['allocation_id'],)):
                        if type(d) is not int or d not in (0, 1) or type(n) is not int or n <= 0 or ok not in (0, 1):
                            raise ValueError('写入证据计数无效')
                        attributed[d] += n
                        if ok: successful[d] += n
                    if any(attributed[d] > used[d] for d in (0, 1)):
                        raise ValueError('写入证据超过持久预留')
                db.execute('INSERT OR REPLACE INTO allocation_quota VALUES(?,?,?,?)',
                           (s['allocation_id'], self._canonical, *used))
                if freeze:
                    frozen = self._freeze_snapshot(db, self._snapshot(used, successful, attributed))
            return frozen if freeze else (allowed, used, successful, attributed)
        except (sqlite3.Error, OverflowError) as error:
            raise OSError('额度预留持久存储失败，停止载荷转发') from error

    def reserve(self, direction, count):
        self._validate_request(direction, count)
        return self._update(direction, count)[0]

    @staticmethod
    def _validate_request(direction, count):
        if type(direction) is not int or direction not in (0, 1):
            raise ValueError('流量方向无效')
        if type(count) is not int or not 0 <= count <= MAX_COUNTER:
            raise ValueError('流量字节数无效')

    def reserve_write(self, direction, count):
        """Atomically reserve bytes and save an unresolved transport intent."""
        self._validate_request(direction, count)
        ticket = uuid.uuid4().hex
        allowed = self._update(direction, count, ticket=ticket)[0]
        if not allowed: return 0, None
        self._inflight.add(ticket)
        return allowed, ticket

    def complete_write(self, ticket):
        """Caller must have actually returned from transport.write successfully.

        This is local evidence, not remote authentication or master confirmation.
        A new adapter cannot resolve an old pending ticket after a crash.
        """
        if not isinstance(ticket, str) or len(ticket) != 32:
            raise ValueError('写入证据编号无效')
        self._update(complete=ticket)
        self._inflight.discard(ticket)

    def abandon_write(self, ticket):
        # Keep the durable unknown amount; discard only the in-process ability
        # to assert success after an observed transport/storage failure.
        self._inflight.discard(ticket)

    def _freeze_snapshot(self, db, snapshot):
        from allocation_outbox import freeze_snapshot
        return freeze_snapshot(db, snapshot)

    def freeze_evidence(self):
        """Freeze one pending request and sequence with the exact local snapshot.

        No network, execution grant, billing or automatic worker is enabled.
        """
        return self._update(freeze=True)

    def snapshot(self):
        _, used, successful, attributed = self._update()
        return self._snapshot(used, successful, attributed)

    def _snapshot(self, used, successful, attributed):
        return dict(self._scope, reserved_upload_bytes=used[0], reserved_download_bytes=used[1],
                    successful_upload_bytes=successful[0], successful_download_bytes=successful[1],
                    unknown_upload_bytes=used[0]-successful[0], unknown_download_bytes=used[1]-successful[1],
                    unattributed_upload_bytes=used[0]-attributed[0], unattributed_download_bytes=used[1]-attributed[1],
                    remaining_bytes=self._scope['reserved_bytes']-sum(used),
                    ready_for_execution=False)
