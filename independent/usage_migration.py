"""Internal one-way drain barrier; never a new-protocol activation switch.

Withhold further legacy execution while retaining authenticated old tails.
Expiry alone cannot prove unknown usage settled, so no automatic ready state.
"""
import time
import hashlib
import json
from core import Error
from closed_receiver import SCHEMA as FRAGMENT_SCHEMA

SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_cycle_drains(
 cycle_id TEXT PRIMARY KEY REFERENCES usage_cycles(id) ON DELETE CASCADE,
 started_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS usage_drain_inventory(
 cycle_id TEXT NOT NULL REFERENCES usage_cycle_drains(cycle_id) ON DELETE CASCADE,
 server_id INTEGER NOT NULL, session_id TEXT NOT NULL,
 node_id INTEGER NOT NULL, user_id INTEGER NOT NULL, credential TEXT NOT NULL,
 token_hash TEXT NOT NULL, issued_at REAL NOT NULL, settle_until REAL NOT NULL,
 upload_bytes INTEGER, download_bytes INTEGER,
 PRIMARY KEY(cycle_id,server_id,session_id,node_id,user_id,credential));
CREATE INDEX IF NOT EXISTS usage_drain_inventory_agent_session
 ON usage_drain_inventory(server_id,session_id,cycle_id);
CREATE TABLE IF NOT EXISTS usage_drain_inventory_captures(
 cycle_id TEXT PRIMARY KEY REFERENCES usage_cycle_drains(cycle_id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS usage_drain_checkpoints(
 cycle_id TEXT NOT NULL REFERENCES usage_cycle_drains(cycle_id) ON DELETE CASCADE,
 server_id INTEGER NOT NULL, session_id TEXT NOT NULL,
 body TEXT NOT NULL, digest TEXT NOT NULL, received_at REAL NOT NULL,
 PRIMARY KEY(cycle_id,server_id,session_id));
CREATE TABLE IF NOT EXISTS usage_drain_seals(
 cycle_id TEXT PRIMARY KEY REFERENCES usage_cycle_drains(cycle_id) ON DELETE CASCADE,
 body TEXT NOT NULL, digest TEXT NOT NULL, sealed_at REAL NOT NULL);
'''+FRAGMENT_SCHEMA


def draining(db,cycle_id):
    return cycle_id is not None and db.execute(
        'SELECT 1 FROM usage_cycle_drains WHERE cycle_id=?',(cycle_id,)).fetchone() is not None


def require_unsealed_growth(db,cycle_id):
    if db.execute('SELECT 1 FROM usage_drain_seals WHERE cycle_id=?',(cycle_id,)).fetchone():
        raise Error('原周期证据已封存，不能追加流量',409)


def require_open_counter(db,sid,session,item):
    """Final accepted identities cannot grow, even through a later sequence."""
    rows=db.execute('SELECT body FROM usage_drain_checkpoints WHERE server_id=? AND session_id=?',
                    (sid,session)).fetchall()
    for row in rows:
        for final in json.loads(row[0])['items']:
            if all(final[k]==item[k] for k in ('node_id','user_id','credential')):
                if any(final[k]!=item[k] for k in ('upload_bytes','download_bytes')):
                    raise Error('已封存的执行累计流量不可修改',409)


def require_legacy_tail(db,sid,session,item,cycle_id):
    # Even after a new period opens, a drained old execution cannot relabel
    # cumulative bytes into it via the ordinary receipt-time ingestion route.
    key=(sid,session,item['node_id'],item['user_id'],item['credential'])
    old=db.execute('''SELECT g.cycle_id FROM usage_execution_grants g
        JOIN usage_cycle_drains d ON d.cycle_id=g.cycle_id
        WHERE g.server_id=? AND g.session_id=? AND g.node_id=? AND g.user_id=? AND g.credential=?''',key).fetchone()
    if not draining(db,cycle_id) and old is None: return
    from usage_settlement import authorize
    cycles=authorize(db,sid,dict(session_id=session,items=[item]))
    if cycles[key[2:]]!=cycle_id:
        raise Error('排空中的旧执行流量必须使用原周期尾报结算',409)


class UsageMigration:
    def __init__(self,registry): self.registry=registry

    def discover(self,token,data):
        """Return only this Agent's captured drain cycles, never infer closure.

        Bounded keyset pages are hints for previously closed local epochs. The
        final checkpoint endpoint still validates the complete original proof.
        """
        if not isinstance(data,dict) or set(data)!={'sessions','after'}:
            raise Error('排空周期查询字段无效')
        sessions=data['sessions']; after=data['after']
        if (not isinstance(sessions,list) or not 1<=len(sessions)<=64 or
            any(not isinstance(s,str) for s in sessions) or len(set(sessions))!=len(sessions)):
            raise Error('排空周期查询需要1–64个不同会话')
        for session in sessions: self.registry._session_value(session)
        if after is not None:
            if (not isinstance(after,list) or len(after)!=2 or
                not isinstance(after[0],str) or not 0<len(after[0])<=200 or
                not isinstance(after[1],str)):
                raise Error('排空周期分页位置无效')
            self.registry._session_value(after[1])
        with self.registry.connect() as db:
            db.execute('BEGIN')
            sid=self.registry._agent_id(db,token)
            fingerprint=db.execute('SELECT token_hash FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
            # Group before paging: a single mismatched/expired original identity
            # must not be hidden by filtering individual inventory rows.
            query='''SELECT i.cycle_id,i.session_id,
                max(i.issued_at) AS issued_max,
                min(i.settle_until) AS deadline,
                sum(CASE WHEN i.token_hash=? THEN 0 ELSE 1 END) AS revoked,
                EXISTS(SELECT 1 FROM usage_drain_checkpoints c WHERE
                    c.cycle_id=i.cycle_id AND c.server_id=i.server_id AND c.session_id=i.session_id) AS accepted
                FROM usage_drain_inventory i JOIN usage_drain_inventory_captures capture
                    ON capture.cycle_id=i.cycle_id
                WHERE i.server_id=? AND i.session_id IN ('''+','.join('?' for _ in sessions)+''')
                GROUP BY i.cycle_id,i.server_id,i.session_id
                HAVING revoked=0 AND (accepted=1 OR (issued_max<=? AND deadline>=?))'''
            now=time.time(); params=[fingerprint,sid,*sessions,now,now]
            if after is not None:
                query+=' AND (i.cycle_id,i.session_id)>(?,?)'; params.extend(after)
            rows=db.execute(query+' ORDER BY i.cycle_id,i.session_id LIMIT 65',params).fetchall()
            page=rows[:64]
            return dict(server_id=sid,sessions=sessions,
                        drains=[dict(cycle_id=row['cycle_id'],session_id=row['session_id']) for row in page],
                        next_after=([page[-1]['cycle_id'],page[-1]['session_id']] if len(rows)>64 else None),
                        ready_for_execution=False)

    def accept_closed(self,cycle_id,token,envelope):
        """Authenticated final evidence acceptance, not an execution switch.

        The Agent must close its actual runtime first. A digest detects altered
        content; authentication comes from its original credential and grant.
        No unknown amount is released and no checkpoint creates billable usage.
        """
        if not isinstance(cycle_id,str) or not cycle_id or len(cycle_id)>200:
            raise Error('周期编号无效')
        if not isinstance(envelope,dict) or set(envelope)!={'checkpoint','digest'}:
            raise Error('最终检查点字段无效')
        body=envelope['checkpoint']
        if not isinstance(body,dict) or set(body)!={'format','server_id','session_id','items','uncertain_writes'}:
            raise Error('最终检查点内容无效')
        if type(body['format']) is not int or body['format']!=1:
            raise Error('最终检查点格式无效')
        from usage_ledger import validate,integer
        if not integer(body['server_id'],1): raise Error('检查点服务器无效')
        validate(dict(session_id=body['session_id'],sequence=1,items=body['items']),self.registry)
        if body['uncertain_writes']!=[]:
            raise Error('最终检查点仍有未确认写入，不能封存',409)
        encoded=json.dumps(body,sort_keys=True,separators=(',',':'))
        digest=hashlib.sha256(encoded.encode()).hexdigest()
        if envelope['digest']!=digest: raise Error('最终检查点摘要不符')
        r=self.registry
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            return self._accept_closed(db,cycle_id,token,body,digest)

    def _accept_closed(self,db,cycle_id,token,body,digest):
        # Internal caller must validate all items, whole checkpoint digest and
        # unknown-write barrier before entering this transaction-scoped helper.
        r=self.registry
        sid=r._agent_id(db,token)
        if sid!=body['server_id']: raise Error('检查点不属于此服务器',403)
        captured=db.execute('SELECT 1 FROM usage_drain_inventory_captures WHERE cycle_id=?',(cycle_id,)).fetchone()
        if captured is None: raise Error('缺少排空身份清单，不能补造最终证据',409)
        frontier=db.execute('SELECT * FROM usage_drain_inventory WHERE cycle_id=? AND server_id=? AND session_id=?',
                            (cycle_id,sid,body['session_id'])).fetchall()
        if not frontier: raise Error('排空清单没有此执行身份',409)
        prior=db.execute('SELECT digest FROM usage_drain_checkpoints WHERE cycle_id=? AND server_id=? AND session_id=?',
                         (cycle_id,sid,body['session_id'])).fetchone()
        if prior and prior[0]!=digest: raise Error('最终检查点内容冲突',409)
        current_hash=db.execute('SELECT token_hash FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
        # Exact accepted retries may recover a lost response after expiry,
        # but token rotation still revokes the original authority.
        if any(row['token_hash']!=current_hash for row in frontier):
            raise Error('原执行凭据已撤销',403)
        reply=dict(accepted=True,cycle_id=cycle_id,server_id=sid,
                   session_id=body['session_id'],digest=digest,ready_for_execution=False)
        if prior: return dict(reply,duplicate=True)
        now=time.time()
        if any(not row['issued_at']<=now<=row['settle_until'] for row in frontier):
            raise Error('原执行最终结算窗口已结束',403)
        items={(i['node_id'],i['user_id'],i['credential']):i for i in body['items']}
        for row in frontier:
            key=(row['node_id'],row['user_id'],row['credential'])
            item=items.get(key)
            if item is None: raise Error('最终检查点缺少排空身份',409)
            if any(row[k] is not None and item[k]<row[k] for k in ('upload_bytes','download_bytes')):
                raise Error('最终累计低于排空前已确认高水位',409)
            confirmed=db.execute('SELECT upload_bytes,download_bytes FROM usage_counters WHERE server_id=? AND session_id=? AND node_id=? AND user_id=? AND credential=?',
                                 (sid,body['session_id'],*key)).fetchone()
            if confirmed is None or any(confirmed[k]!=item[k] for k in ('upload_bytes','download_bytes')):
                raise Error('最终累计尚未全部确认，不能封存',409)
        # Ignore other cycles in a multi-user Agent session: only captured
        # identities in this cycle are sealed by this acceptance.
        scoped=dict(body,items=[items[(row['node_id'],row['user_id'],row['credential'])] for row in frontier])
        db.execute('INSERT INTO usage_drain_checkpoints VALUES(?,?,?,?,?,?)',
                   (cycle_id,sid,body['session_id'],json.dumps(scoped,sort_keys=True,separators=(',',':')),digest,now))
        return dict(reply,duplicate=False)

    def begin_drain(self,cycle_id):
        if not isinstance(cycle_id,str) or not cycle_id: raise Error('周期编号无效')
        r=self.registry
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            cycle=db.execute('SELECT user_id FROM usage_cycles WHERE id=?',(cycle_id,)).fetchone()
            if cycle is None: raise Error('周期不存在',404)
            row=db.execute('SELECT started_at FROM usage_cycle_drains WHERE cycle_id=?',(cycle_id,)).fetchone()
            if row is None:
                # Capture known history before hiding identities, not guessed
                # legacy bytes and not a refund or execution-budget transfer.
                from total_usage import directions
                directions(db,cycle_id)
                db.execute('INSERT INTO usage_cycle_drains VALUES(?,?)',(cycle_id,time.time()))
                # Preserve the frontier before ownership cleanup can cascade
                # away grants/counters. Missing counters remain unknown, not 0.
                # This inventory is not a final Agent checkpoint or a refund.
                db.execute('''INSERT INTO usage_drain_inventory
                    SELECT g.cycle_id,g.server_id,g.session_id,g.node_id,g.user_id,g.credential,
                        g.token_hash,g.issued_at,g.settle_until,c.upload_bytes,c.download_bytes
                    FROM usage_execution_grants g LEFT JOIN usage_counters c ON
                        c.server_id=g.server_id AND c.session_id=g.session_id AND
                        c.node_id=g.node_id AND c.user_id=g.user_id AND c.credential=g.credential
                    WHERE g.cycle_id=?''',(cycle_id,))
                db.execute('INSERT INTO usage_drain_inventory_captures VALUES(?)',(cycle_id,))
                hosts=db.execute('''SELECT DISTINCT n.server_id FROM nodes n
                    JOIN node_users u ON u.node_id=n.id WHERE u.user_id=?''',(cycle['user_id'],)).fetchall()
                for host in hosts: r._bump(db,host[0])
                row=db.execute('SELECT started_at FROM usage_cycle_drains WHERE cycle_id=?',(cycle_id,)).fetchone()
            return dict(cycle_id=cycle_id,started_at=row[0],state='draining',ready_for_execution=False)

    def inventory(self,cycle_id):
        """Internal evidence completeness, never migration execution readiness.

        Existing drains are deliberately not backfilled: deleted historical
        executions cannot be reconstructed from today's surviving grants.
        """
        with self.registry.connect() as db:
            db.execute('BEGIN')
            if not draining(db,cycle_id): raise Error('周期尚未排空',409)
            captured=db.execute('SELECT 1 FROM usage_drain_inventory_captures WHERE cycle_id=?',(cycle_id,)).fetchone() is not None
            count=db.execute('SELECT count(*) FROM usage_drain_inventory WHERE cycle_id=?',(cycle_id,)).fetchone()[0]
            return dict(cycle_id=cycle_id,frontier_captured=captured,
                        execution_identities=count,ready_for_execution=False)
