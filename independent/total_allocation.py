"""Internal durable aggregate reservations, NOT delivered execution grants.

All hosts and policy versions share a cycle's balance. Legacy reservations
remain fully held (possible double counting, never an automatic refund).
The separate first-grant ledger can move attributed bytes from its hold to
confirmed usage atomically; it has no actual execution producer yet.
"""
import time
import uuid
from core import Error
from total_policy import current
from usage_ledger import integer
from usage_settlement import EXECUTION_LEASE

SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_total_allocations(
 request_id TEXT PRIMARY KEY, allocation_id TEXT NOT NULL UNIQUE,
 cycle_id TEXT NOT NULL REFERENCES usage_cycles(id) ON DELETE CASCADE,
 policy_id INTEGER NOT NULL REFERENCES usage_total_policy_versions(id),
 server_id INTEGER NOT NULL,session_id TEXT NOT NULL,token_hash TEXT NOT NULL,
 node_id INTEGER NOT NULL,user_id INTEGER NOT NULL,credential TEXT NOT NULL,
 requested_bytes INTEGER NOT NULL CHECK(requested_bytes>0),
 reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes>0 AND reserved_bytes<=requested_bytes),
 issued_at REAL NOT NULL,valid_until REAL NOT NULL);
CREATE INDEX IF NOT EXISTS usage_total_allocation_cycle ON usage_total_allocations(cycle_id);
CREATE TABLE IF NOT EXISTS usage_total_allocation_reports(
 allocation_id TEXT NOT NULL REFERENCES usage_total_allocations(allocation_id) ON DELETE CASCADE,
 sequence INTEGER NOT NULL, upload_bytes INTEGER NOT NULL CHECK(upload_bytes>=0),
 download_bytes INTEGER NOT NULL CHECK(download_bytes>=0), received_at REAL NOT NULL,
 PRIMARY KEY(allocation_id,sequence));
'''


def result(row):
    return {**{key:row[key] for key in ('request_id','allocation_id','cycle_id','policy_id',
             'server_id','session_id','node_id','user_id','credential','requested_bytes',
             'reserved_bytes','issued_at','valid_until')},'ready_for_execution':False}


def held(db,cycle_id):
    """Full unresolved reservations, including expired and historical policies.

    This is a conservative internal hold, not proof of delivered execution.
    Sum in Python: the aggregate can exceed SQLite's integer accumulator.
    """
    if not db.in_transaction: raise RuntimeError('reservation balance requires a transaction')
    legacy=sum(row[0] for row in db.execute(
        'SELECT reserved_bytes FROM usage_total_allocations WHERE cycle_id=?',(cycle_id,)))
    from usage_accounting import outstanding
    return legacy+outstanding(db,cycle_id)


def require_legacy_capacity(db,cycle_id,total_limit,confirmed,delta):
    """Ordinary untagged ingestion cannot spend held new-protocol balance.

    Bounded old-execution settlements remain a separate unresolved migration
    boundary. Never guess which reservation an untagged receipt consumed.
    """
    if total_limit is not None and confirmed+delta+held(db,cycle_id)>total_limit:
        raise Error('周期合计额度包含未结算预留，请使用原执行尾报结算',409)


class TotalAllocations:
    def __init__(self,registry): self.registry=registry

    def report(self,token,data):
        """Internal Agent claims, not socket evidence or legacy billable usage.

        A tagged cumulative report never refunds its unresolved reservation.
        Keeping this separate prevents double billing an untagged legacy delta.
        Current credentials/session are required; retired-session settlement
        needs an explicit protocol before this can become an execution API.
        """
        fields={'allocation_id','policy_id','session_id','sequence','upload_bytes','download_bytes'}
        if not isinstance(data,dict) or set(data)!=fields: raise Error('预留流量回执字段无效')
        r=self.registry
        for key in ('allocation_id','session_id'): r._session_value(data[key])
        for key in ('policy_id','sequence','upload_bytes','download_bytes'):
            if not integer(data[key],1 if key in ('policy_id','sequence') else 0):
                raise Error('预留流量回执需要有效整数字段')
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            sid=r._agent_id(db,token); r._check_session(db,sid,data['session_id'])
            token_hash=db.execute('SELECT token_hash FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
            allocation=db.execute('SELECT * FROM usage_total_allocations WHERE allocation_id=?',(data['allocation_id'],)).fetchone()
            if allocation is None or allocation['server_id']!=sid or allocation['token_hash']!=token_hash or allocation['session_id']!=data['session_id']:
                raise Error('预留不属于此Agent会话',403)
            if allocation['policy_id']!=data['policy_id']: raise Error('预留策略版本不匹配',409)
            latest=db.execute('SELECT * FROM usage_total_allocation_reports WHERE allocation_id=? ORDER BY sequence DESC LIMIT 1',(allocation['allocation_id'],)).fetchone()
            exact=db.execute('SELECT * FROM usage_total_allocation_reports WHERE allocation_id=? AND sequence=?',(allocation['allocation_id'],data['sequence'])).fetchone()
            duplicate=exact is not None
            if exact is not None:
                if any(exact[k]!=data[k] for k in ('upload_bytes','download_bytes')):
                    raise Error('预留回执编号已用于不同内容',409)
            else:
                if latest and (data['sequence']<=latest['sequence'] or any(data[k]<latest[k] for k in ('upload_bytes','download_bytes'))):
                    raise Error('预留累计流量不可回退',409)
                if data['upload_bytes']+data['download_bytes']>allocation['reserved_bytes']:
                    raise Error('预留双向合计流量超出授予范围',409)
                db.execute('INSERT INTO usage_total_allocation_reports VALUES(?,?,?,?,?)',
                    (allocation['allocation_id'],data['sequence'],data['upload_bytes'],data['download_bytes'],time.time()))
                latest=data
            # Old retries acknowledge their sequence while describing the
            # latest high water, never reverting current accounting state.
            claimed=latest['upload_bytes']+latest['download_bytes']
            return dict(accepted=True,duplicate=duplicate,sequence=data['sequence'],
                        allocation_id=allocation['allocation_id'],policy_id=allocation['policy_id'],
                        reported_bytes=claimed,unresolved_bytes=allocation['reserved_bytes']-claimed,
                        held_bytes=allocation['reserved_bytes'],ready_for_execution=False)

    def reserve(self,token,data):
        fields={'request_id','session_id','cycle_id','node_id','user_id','credential','requested_bytes'}
        if not isinstance(data,dict) or set(data)!=fields: raise Error('合计额度预留字段无效')
        r=self.registry
        for key in ('request_id','session_id','credential'): r._session_value(data[key])
        if not isinstance(data['cycle_id'],str) or not data['cycle_id']: raise Error('周期编号无效')
        if any(not integer(data[key],1) for key in ('node_id','user_id','requested_bytes')):
            raise Error('额度预留需要有效资源编号和正整数字节')
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            sid=r._agent_id(db,token); r._check_session(db,sid,data['session_id'])
            token_hash=db.execute('SELECT token_hash FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
            old=db.execute('SELECT * FROM usage_total_allocations WHERE request_id=?',(data['request_id'],)).fetchone()
            if old is not None:
                if old['server_id']!=sid or old['token_hash']!=token_hash:
                    raise Error('预留不属于此Agent',403)
                if any(old[key]!=data[key] for key in fields): raise Error('预留请求编号已用于不同内容',409)
                # Recover the original receipt after a lost response; never
                # extend its deadline, refund it or authorize execution here.
                return result(old)
            now=time.time(); snapshot=r._snapshot(db,sid)
            client=None
            for node in snapshot['nodes']:
                if node['id']==data['node_id'] and node['config']['enabled']:
                    client=next((c for c in node.get('clients',[]) if c['user_id']==data['user_id'] and c['uuid']==data['credential']),None)
            if client is None or client.get('cycle_id')!=data['cycle_id']:
                raise Error('预留不属于此服务器当前执行身份和周期',403)
            cycle=db.execute('SELECT * FROM usage_cycles WHERE id=?',(data['cycle_id'],)).fetchone()
            if cycle is None or cycle['expired'] or not cycle['starts_at']<=now<cycle['ends_at']:
                raise Error('预留周期未生效或已结束',409)
            until=min(now+EXECUTION_LEASE,cycle['ends_at'],snapshot.get('valid_until',float('inf')),client.get('valid_until',float('inf')))
            if until<=now: raise Error('预留执行窗口已结束',409)
            policy=current(db,cycle['id'])
            if policy is None: raise Error('尚未配置合计额度策略',409)
            from total_usage import current as confirmed_total
            used=confirmed_total(db,cycle['id'])
            # Do not release on lease expiry, policy change, token rotation,
            # host deletion or process death: delivery/spend may be unknown.
            reserved=held(db,cycle['id'])
            count=data['requested_bytes']
            if policy['total_bytes'] is not None: count=min(count,max(0,policy['total_bytes']-used-reserved))
            if count==0: raise Error('合计额度没有可预留余额',409)
            ident=str(uuid.uuid4())
            db.execute('''INSERT INTO usage_total_allocations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (data['request_id'],ident,cycle['id'],policy['id'],sid,data['session_id'],token_hash,
                 data['node_id'],data['user_id'],data['credential'],data['requested_bytes'],count,now,until))
            return result(db.execute('SELECT * FROM usage_total_allocations WHERE allocation_id=?',(ident,)).fetchone())
