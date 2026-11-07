"""Durable cumulative report queue; transport and executor scheduling are separate.

Only committed snapshots survive a crash. The attached execution worker captures
each successful payload write. A new process needs a new session and fresh
counters: recovered reports retain their original execution proof identity.
Pre-write targets survive separately as uncertainty, never as billable totals.
"""
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import uuid
from usage_ledger import integer, validate

RETRY_BASE=5
RETRY_MAX=60
# Payload checkpoints run on the executor event loop. A busy database must
# fence forwarding promptly rather than stall every connection for ten seconds.
PAYLOAD_LOCK_TIMEOUT=0.05


class UsageOutbox:
    def __init__(self, path, server_id):
        if not integer(server_id,1): raise ValueError('流量服务器编号无效')
        self.path=Path(path); self.server_id=server_id
        self.path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        fd=os.open(self.path,os.O_CREAT|os.O_RDWR,0o600); os.close(fd)
        os.chmod(self.path,0o600)
        with self.connect(timeout=PAYLOAD_LOCK_TIMEOUT) as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS write_intents(
                    session TEXT,node INTEGER,user INTEGER,credential TEXT,
                    upload INTEGER NOT NULL,download INTEGER NOT NULL,
                    PRIMARY KEY(session,node,user,credential));
                CREATE TABLE IF NOT EXISTS write_resolutions(
                    session TEXT,node INTEGER,user INTEGER,credential TEXT,
                    lower_upload INTEGER NOT NULL,lower_download INTEGER NOT NULL,
                    target_upload INTEGER NOT NULL,target_download INTEGER NOT NULL,
                    checkpoint_upload INTEGER NOT NULL,checkpoint_download INTEGER NOT NULL,
                    recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(session,node,user,credential,target_upload,target_download));
                CREATE TABLE IF NOT EXISTS recovered_writes(
                    session TEXT,node INTEGER,user INTEGER,credential TEXT,
                    lower_upload INTEGER NOT NULL,lower_download INTEGER NOT NULL,
                    target_upload INTEGER NOT NULL,target_download INTEGER NOT NULL,
                    PRIMARY KEY(session,node,user,credential,target_upload,target_download));
                CREATE TABLE IF NOT EXISTS identity(server_id INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS totals(
                    session TEXT,node INTEGER,user INTEGER,credential TEXT,
                    upload INTEGER,download INTEGER,acked_upload INTEGER DEFAULT 0,
                    acked_download INTEGER DEFAULT 0,acked_sequence INTEGER DEFAULT 0,
                    PRIMARY KEY(session,node,user,credential));
                CREATE TABLE IF NOT EXISTS cursors(session TEXT PRIMARY KEY,sequence INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS pending(session TEXT PRIMARY KEY,body TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS retries(session TEXT PRIMARY KEY,sequence INTEGER NOT NULL,
                    attempts INTEGER NOT NULL,next_attempt REAL NOT NULL,reason TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS isolation(session TEXT PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS retired(session TEXT PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS closed_epochs(session TEXT PRIMARY KEY,
                    body TEXT NOT NULL,digest TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS closed_deliveries(
                    cycle TEXT NOT NULL,session TEXT NOT NULL,digest TEXT NOT NULL,
                    status TEXT NOT NULL,attempts INTEGER NOT NULL,next_attempt REAL NOT NULL,
                    PRIMARY KEY(cycle,session));
                CREATE TABLE IF NOT EXISTS closed_discoveries(
                    session TEXT PRIMARY KEY,digest TEXT NOT NULL,status TEXT NOT NULL,
                    after_key TEXT,attempts INTEGER NOT NULL,next_attempt REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS recoveries(session TEXT,sequence INTEGER,created_at REAL NOT NULL,
                    PRIMARY KEY(session,sequence));
                CREATE TABLE IF NOT EXISTS recovery_actions(session TEXT,sequence INTEGER,operator TEXT NOT NULL,
                    reason TEXT NOT NULL,report_digest TEXT NOT NULL,created_at REAL NOT NULL,
                    PRIMARY KEY(session,sequence));
                CREATE TABLE IF NOT EXISTS failures(session TEXT,sequence INTEGER,body TEXT NOT NULL,
                    reason TEXT NOT NULL,created_at REAL NOT NULL,PRIMARY KEY(session,sequence));
                CREATE TABLE IF NOT EXISTS quarantine(session TEXT,node INTEGER,user INTEGER,credential TEXT,
                    sequence INTEGER NOT NULL,PRIMARY KEY(session,node,user,credential));
            ''')
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT server_id FROM identity').fetchone()
            if row and row[0]!=server_id: raise ValueError('流量队列服务器身份不一致')
            if not row: db.execute('INSERT INTO identity VALUES(?)',(server_id,))
            # Only intents observed on reopening need recovery audit. Ordinary
            # successful payloads must not create permanent per-write history.
            db.execute('''INSERT OR IGNORE INTO recovered_writes
                SELECT w.session,w.node,w.user,w.credential,
                    COALESCE(t.upload,0),COALESCE(t.download,0),w.upload,w.download
                FROM write_intents w LEFT JOIN totals t ON
                    w.session=t.session AND w.node=t.node AND w.user=t.user AND w.credential=t.credential''')

    @contextmanager
    def connect(self,timeout=10):
        db=sqlite3.connect(self.path,timeout=timeout)
        db.row_factory=sqlite3.Row
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db: yield db
        finally: db.close()

    @staticmethod
    def _session_value(value):
        if not isinstance(value,str) or str(uuid.UUID(value))!=value:
            raise ValueError('流量会话或UUID无效')

    def prepare_write(self, session, item):
        """Persist a cumulative write target, never a billable usage receipt.

        Call before transport.write. Only a later successful capture covering
        both targets resolves it. A crash leaves an auditable uncertain range.
        """
        self._session_value(session)
        validate(dict(session_id=session,sequence=1,items=[item]),self)
        key=(session,item['node_id'],item['user_id'],item['credential'])
        target=(item['upload_bytes'],item['download_bytes'])
        with self.connect(timeout=PAYLOAD_LOCK_TIMEOUT) as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM retired WHERE session=?',(session,)).fetchone():
                raise ValueError('流量会话已结清退役')
            if db.execute('SELECT 1 FROM closed_epochs WHERE session=?',(session,)).fetchone():
                raise ValueError('流量会话已停止写入')
            old=db.execute('SELECT upload,download FROM totals WHERE session=? AND node=? AND user=? AND credential=?',key).fetchone()
            pending=db.execute('SELECT upload,download FROM write_intents WHERE session=? AND node=? AND user=? AND credential=?',key).fetchone()
            if pending is not None:
                raise ValueError('载荷写入状态尚未确认')
            baseline=tuple(old) if old is not None else (0,0)
            if any(t<b for t,b in zip(target,baseline)) or target==baseline:
                raise ValueError('载荷写入目标无效')
            db.execute('INSERT INTO write_intents VALUES(?,?,?,?,?,?)',(*key,*target))

    def uncertain_writes(self):
        """Recover uncertainty as diagnostics; never enqueue it for settlement."""
        with self.connect() as db:
            return [dict(row) for row in db.execute('''
                SELECT w.session,w.node,w.user,w.credential,
                    COALESCE(t.upload,0) AS committed_upload,
                    COALESCE(t.download,0) AS committed_download,
                    w.upload AS possible_upload,w.download AS possible_download
                FROM write_intents w LEFT JOIN totals t ON
                    w.session=t.session AND w.node=t.node AND w.user=t.user AND w.credential=t.credential
                ORDER BY w.session,w.node,w.user,w.credential''')]

    def capture(self, session, snapshot):
        if (not isinstance(snapshot,dict) or set(snapshot)!={'server_id','items'}
                or not integer(snapshot['server_id'],1) or snapshot['server_id']!=self.server_id):
            raise ValueError('流量快照服务器无效')
        # Reuse the ledger's exact integer/UUID/duplicate rules; empty is a no-op.
        self._session_value(session)
        if not isinstance(snapshot['items'],list): raise ValueError('流量条目无效')
        seen=set()
        for offset in range(0,len(snapshot['items']),64):
            validate(dict(session_id=session,sequence=1,items=snapshot['items'][offset:offset+64]),self)
        with self.connect(timeout=PAYLOAD_LOCK_TIMEOUT) as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM retired WHERE session=?',(session,)).fetchone():
                raise ValueError('流量会话已结清退役；新执行进程必须使用新会话')
            closed=db.execute('SELECT body FROM closed_epochs WHERE session=?',(session,)).fetchone()
            if closed is not None:
                # Retry after a committed close/cancel may repeat the final
                # snapshot, but cannot introduce identities or grow counters.
                final={(i['node_id'],i['user_id'],i['credential']):
                       (i['upload_bytes'],i['download_bytes'])
                       for i in json.loads(closed['body'])['items']}
                for item in snapshot['items']:
                    key=(item['node_id'],item['user_id'],item['credential'])
                    if key in seen or final.get(key)!=(item['upload_bytes'],item['download_bytes']):
                        raise ValueError('停止写入后的流量快照发生变化')
                    seen.add(key)
                return
            for item in snapshot['items']:
                key=(session,item['node_id'],item['user_id'],item['credential'])
                if key in seen: raise ValueError('流量快照重复身份')
                seen.add(key)
                old=db.execute('SELECT upload,download FROM totals WHERE session=? AND node=? AND user=? AND credential=?',key).fetchone()
                up,down=item['upload_bytes'],item['download_bytes']
                if old and (up<old['upload'] or down<old['download']):
                    raise ValueError('流量快照回退；新执行进程必须使用新会话')
                intent=db.execute('SELECT upload,download FROM write_intents WHERE session=? AND node=? AND user=? AND credential=?',key).fetchone()
                if intent and up>=intent['upload'] and down>=intent['download']:
                    # The successful checkpoint is the evidence, not the intent
                    # target. Archive both bounds atomically with totals/delete;
                    # an audit failure must retain the intent and old counters.
                    recovered=db.execute('SELECT lower_upload,lower_download FROM recovered_writes WHERE session=? AND node=? AND user=? AND credential=? AND target_upload=? AND target_download=?',
                                         (*key,intent['upload'],intent['download'])).fetchone()
                    if recovered:
                        db.execute('INSERT INTO write_resolutions(session,node,user,credential,lower_upload,lower_download,target_upload,target_download,checkpoint_upload,checkpoint_download) VALUES(?,?,?,?,?,?,?,?,?,?)',
                                   (*key,*tuple(recovered),intent['upload'],intent['download'],up,down))
                db.execute('INSERT INTO totals(session,node,user,credential,upload,download) VALUES(?,?,?,?,?,?) ON CONFLICT(session,node,user,credential) DO UPDATE SET upload=excluded.upload,download=excluded.download',(*key,up,down))
                db.execute('DELETE FROM write_intents WHERE session=? AND node=? AND user=? AND credential=? AND upload<=? AND download<=?',(*key,up,down))

    def close_epoch(self,session):
        """Fence a locally stopped executor epoch, retaining final/unknown data.

        Caller MUST first await runtime.close() and capture final counters. A
        digest is a local integrity identifier, not master authentication or
        proof of remote settlement. Pending reports remain deliverable.
        """
        self._session_value(session)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT body,digest FROM closed_epochs WHERE session=?',(session,)).fetchone()
            if old is not None: return dict(checkpoint=json.loads(old['body']),digest=old['digest'])
            if db.execute('SELECT 1 FROM retired WHERE session=?',(session,)).fetchone():
                raise ValueError('已清理会话缺少最终流量证据')
            items=[dict(node_id=r['node'],user_id=r['user'],credential=r['credential'],
                        upload_bytes=r['upload'],download_bytes=r['download'])
                   for r in db.execute('SELECT * FROM totals WHERE session=? ORDER BY node,user,credential',(session,))]
            unknown=[dict(node_id=r['node'],user_id=r['user'],credential=r['credential'],
                          possible_upload=r['upload'],possible_download=r['download'])
                     for r in db.execute('SELECT * FROM write_intents WHERE session=? ORDER BY node,user,credential',(session,))]
            checkpoint=dict(format=1,server_id=self.server_id,session_id=session,
                            items=items,uncertain_writes=unknown)
            body=json.dumps(checkpoint,sort_keys=True,separators=(',',':'))
            digest=hashlib.sha256(body.encode()).hexdigest()
            db.execute('INSERT INTO closed_epochs VALUES(?,?,?)',(session,body,digest))
            return dict(checkpoint=checkpoint,digest=digest)

    def closed_epoch(self,session):
        self._session_value(session)
        with self.connect() as db:
            row=db.execute('SELECT body,digest FROM closed_epochs WHERE session=?',(session,)).fetchone()
            return None if row is None else dict(checkpoint=json.loads(row['body']),digest=row['digest'])

    def write_resolution_history(self):
        """Local checkpoint audit; timestamps are informational, not proof clocks.

        Not a settlement receipt or a manual resolution API. Keep credential
        generations distinguishable without returning their usable UUIDs.
        Retirement of confirmed totals must not erase this evidence.
        """
        with self.connect() as db:
            rows=db.execute('SELECT * FROM write_resolutions ORDER BY session,node,user,credential,target_upload,target_download').fetchall()
            result=[]
            for row in rows:
                item=dict(row)
                item['credential_fingerprint']=hashlib.sha256(item.pop('credential').encode()).hexdigest()
                item['source']='successful_checkpoint'
                result.append(item)
            return result

    def retire_confirmed(self, current_session):
        """Prune one fully confirmed old epoch, preserving a non-reuse tombstone.

        The owning delivery worker supplies its current execution epoch. Never
        retire pending, partially confirmed, or audited/quarantined sessions.
        The cursor remains, and future capture of a retired epoch is rejected.
        """
        self._session_value(current_session)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            # Check each epoch once. Repeating the correlated totals scan for
            # every identity makes a large retained/audited epoch quadratic.
            row=db.execute('''SELECT t.session FROM
                (SELECT DISTINCT session FROM totals WHERE session!=?) t
                WHERE NOT EXISTS (SELECT 1 FROM totals x WHERE x.session=t.session
                    AND (x.upload!=x.acked_upload OR x.download!=x.acked_download OR x.acked_sequence=0))
                AND NOT EXISTS (SELECT 1 FROM write_intents x WHERE x.session=t.session)
                AND NOT EXISTS (SELECT 1 FROM pending x WHERE x.session=t.session)
                AND NOT EXISTS (SELECT 1 FROM retries x WHERE x.session=t.session)
                AND NOT EXISTS (SELECT 1 FROM isolation x WHERE x.session=t.session)
                AND NOT EXISTS (SELECT 1 FROM failures x WHERE x.session=t.session)
                AND NOT EXISTS (SELECT 1 FROM quarantine x WHERE x.session=t.session)
                ORDER BY t.session LIMIT 1''',(current_session,)).fetchone()
            if row is None: return 0
            session=row['session']
            db.execute('INSERT INTO retired VALUES(?)',(session,))
            return db.execute('DELETE FROM totals WHERE session=?',(session,)).rowcount

    def outstanding_sessions(self):
        """Discover crash-recovered work without claiming retired sessions can post."""
        with self.connect() as db:
            return [r[0] for r in db.execute('''SELECT session FROM pending UNION SELECT session FROM totals t
                WHERE (upload>acked_upload OR download>acked_download) AND NOT EXISTS
                (SELECT 1 FROM quarantine q WHERE q.session=t.session AND q.node=t.node AND q.user=t.user AND q.credential=t.credential)
                ORDER BY session''')]

    @staticmethod
    def _clock(now):
        if type(now) not in (int,float) or not math.isfinite(now) or now<0:
            raise ValueError('流量重试时间无效')

    def ready_sessions(self, now):
        self._clock(now)
        with self.connect() as db:
            return [r[0] for r in db.execute('''
                SELECT session FROM (
                    SELECT session FROM pending UNION SELECT session FROM totals t
                    WHERE (upload>acked_upload OR download>acked_download) AND NOT EXISTS
                    (SELECT 1 FROM quarantine q WHERE q.session=t.session AND q.node=t.node AND q.user=t.user AND q.credential=t.credential)
                ) work LEFT JOIN retries USING(session)
                WHERE next_attempt IS NULL OR next_attempt<=? OR next_attempt>?
                ORDER BY session''',(now,now+RETRY_MAX))]

    def defer(self, report, reason, now):
        """Persist scheduling failure only: no accounting cursor/ack changes."""
        validate(report,self); self._clock(now)
        if reason not in ('network','rejected','receipt'):
            raise ValueError('流量重试原因无效')
        session=report['session_id']
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            pending=db.execute('SELECT body FROM pending WHERE session=?',(session,)).fetchone()
            if pending is None or json.loads(pending['body'])!=report:
                raise ValueError('流量重试没有对应待发报告')
            old=db.execute('SELECT * FROM retries WHERE session=?',(session,)).fetchone()
            attempts=min(old['attempts']+1,5) if old and old['sequence']==report['sequence'] else 1
            delay=min(RETRY_MAX,RETRY_BASE*2**(attempts-1))
            db.execute('INSERT INTO retries VALUES(?,?,?,?,?) ON CONFLICT(session) DO UPDATE SET sequence=excluded.sequence,attempts=excluded.attempts,next_attempt=excluded.next_attempt,reason=excluded.reason',
                       (session,report['sequence'],attempts,now+delay,reason))

    def retry_status(self):
        """Local diagnostics omit report credentials, URLs and exception text."""
        with self.connect() as db:
            return [dict(r) for r in db.execute('SELECT * FROM retries ORDER BY session')]

    def isolate(self, report, now):
        """Archive a definitively rejected atomic batch; never mark it received.

        Multi-identity batches retry individually at fresh sequence numbers.
        A single rejected identity is retained outside the automatic queue.
        Only the transport's authenticated HTTP 403 path may call this method.
        """
        validate(report,self); self._clock(now)
        if not integer(report['sequence']+1,1): raise ValueError('流量回执序号耗尽')
        session=report['session_id']; single=len(report['items'])==1
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            pending=db.execute('SELECT body FROM pending WHERE session=?',(session,)).fetchone()
            if pending is None or json.loads(pending['body'])!=report:
                raise ValueError('流量隔离没有对应待发报告')
            db.execute('INSERT INTO failures VALUES(?,?,?,?,?)',(session,report['sequence'],pending['body'],
                       'identity_rejected' if single else 'split_after_403',now))
            db.execute('INSERT OR IGNORE INTO isolation VALUES(?)',(session,))
            if single:
                item=report['items'][0]
                db.execute('INSERT INTO quarantine VALUES(?,?,?,?,?)',
                           (session,item['node_id'],item['user_id'],item['credential'],report['sequence']))
            # This is a sequence reservation, not a received-byte watermark.
            # The server accepts gaps and still computes cumulative deltas.
            db.execute('INSERT INTO cursors VALUES(?,?) ON CONFLICT(session) DO UPDATE SET sequence=excluded.sequence',
                       (session,report['sequence']))
            db.execute('DELETE FROM pending WHERE session=?',(session,))
            db.execute('DELETE FROM retries WHERE session=?',(session,))

    def retry_quarantined(self, report, now, *, operator=None, reason=None):
        """Explicit local retry of one exact rejection, never an authorization.

        Preserve the failed report and accounting watermarks. A fresh report
        uses current cumulative totals; the server must still authorize it.
        A stale operator action cannot release a later rejection.
        """
        digest=validate(report,self); self._clock(now)
        if operator is not None or reason is not None:
            for value,limit in ((operator,80),(reason,500)):
                if (not isinstance(value,str) or not value.strip() or len(value)>limit
                        or any(ord(c)<32 or ord(c)==127 for c in value)):
                    raise ValueError('恢复操作人和原因无效')
        if len(report['items'])!=1: raise ValueError('只能重试单个隔离身份')
        item=report['items'][0]; session=report['session_id']; sequence=report['sequence']
        key=(session,item['node_id'],item['user_id'],item['credential'])
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            identity=db.execute('SELECT server_id FROM identity').fetchall()
            if len(identity)!=1 or identity[0][0]!=self.server_id:
                raise ValueError('流量队列服务器身份不一致')
            row=db.execute('SELECT sequence FROM quarantine WHERE session=? AND node=? AND user=? AND credential=?',key).fetchone()
            failure=db.execute('SELECT body,reason FROM failures WHERE session=? AND sequence=?',(session,sequence)).fetchone()
            if (row is None or row['sequence']!=sequence or failure is None
                    or failure['reason']!='identity_rejected' or json.loads(failure['body'])!=report):
                raise ValueError('隔离记录已变化，请重新检查后重试')
            db.execute('INSERT INTO recoveries VALUES(?,?,?)',(session,sequence,now))
            if operator is not None:
                db.execute('INSERT INTO recovery_actions VALUES(?,?,?,?,?,?)',
                           (session,sequence,operator,reason,digest,now))
            db.execute('DELETE FROM quarantine WHERE session=? AND node=? AND user=? AND credential=?',key)

    def recovery_status(self):
        """Local retry audit, without credentials or the report body."""
        with self.connect() as db:
            return [dict(r) for r in db.execute('SELECT * FROM recoveries ORDER BY session,sequence')]

    def isolation_status(self):
        """Local audit: original frozen evidence and unacknowledged totals."""
        with self.connect() as db:
            failures=[dict(r,report=json.loads(r['body'])) for r in db.execute('SELECT * FROM failures ORDER BY session,sequence')]
            for row in failures: del row['body']
            quarantined=[dict(r) for r in db.execute('''SELECT t.*,q.sequence AS rejected_sequence FROM totals t
                JOIN quarantine q USING(session,node,user,credential) ORDER BY session,node,user,credential''')]
            return dict(failures=failures,quarantined=quarantined)

    def next_report(self, session):
        self._session_value(session)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            pending=db.execute('SELECT body FROM pending WHERE session=?',(session,)).fetchone()
            if pending: return json.loads(pending['body'])
            limit=1 if db.execute('SELECT 1 FROM isolation WHERE session=?',(session,)).fetchone() else 64
            rows=db.execute('''SELECT * FROM totals t WHERE session=? AND (upload>acked_upload OR download>acked_download)
                AND NOT EXISTS (SELECT 1 FROM quarantine q WHERE q.session=t.session AND q.node=t.node AND q.user=t.user AND q.credential=t.credential)
                ORDER BY acked_sequence,node,user,credential LIMIT ?''',(session,limit)).fetchall()
            if not rows: return None
            old=db.execute('SELECT sequence FROM cursors WHERE session=?',(session,)).fetchone()
            sequence=(old['sequence'] if old else 0)+1
            if not integer(sequence,1): raise ValueError('流量回执序号耗尽')
            report=dict(session_id=session,sequence=sequence,items=[dict(node_id=r['node'],user_id=r['user'],credential=r['credential'],upload_bytes=r['upload'],download_bytes=r['download']) for r in rows])
            db.execute('INSERT INTO pending VALUES(?,?)',(session,json.dumps(report,sort_keys=True,separators=(',',':'))))
            return report

    def acknowledge(self, report, receipt):
        validate(report,self)
        if (not isinstance(receipt,dict) or receipt.get('accepted') is not True
                or type(receipt.get('server_id')) is not int or receipt['server_id']!=self.server_id
                or receipt.get('session_id')!=report['session_id']
                or type(receipt.get('sequence')) is not int or receipt['sequence']!=report['sequence']):
            raise ValueError('流量确认与待发报告不一致')
        session=report['session_id']
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            pending=db.execute('SELECT body FROM pending WHERE session=?',(session,)).fetchone()
            if pending is None or json.loads(pending['body'])!=report:
                raise ValueError('流量确认没有对应待发报告')
            for item in report['items']:
                db.execute('UPDATE totals SET acked_upload=?,acked_download=?,acked_sequence=? WHERE session=? AND node=? AND user=? AND credential=?',
                           (item['upload_bytes'],item['download_bytes'],report['sequence'],session,item['node_id'],item['user_id'],item['credential']))
            db.execute('INSERT INTO cursors VALUES(?,?) ON CONFLICT(session) DO UPDATE SET sequence=excluded.sequence',(session,report['sequence']))
            db.execute('DELETE FROM pending WHERE session=?',(session,))
            db.execute('DELETE FROM retries WHERE session=?',(session,))
