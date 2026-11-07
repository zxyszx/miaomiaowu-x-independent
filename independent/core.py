"""Independent resource registry. Does not load the official runtime or its data."""
from contextlib import contextmanager
import hashlib
import hmac
import json
import math
import os
import secrets
import sqlite3
import time
import uuid
from pathlib import Path
from node_config import SCHEMA, XRAY_SCHEMA, digest, profile, bandwidth, connections, automatic_bandwidth
from agent_liveness import ONLINE_WINDOW

PROBE_JOB_LEASE = 60
PROBE_MAX_ATTEMPTS = 3
PROBE_RETRY_DELAY = 15
PROBE_RETRY_ERRORS = frozenset(('timeout','dns_error','connection_error','probe_error'))

KINDS = ('servers', 'nodes', 'users')

class Error(ValueError):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status

class Registry:
    def __init__(self, path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS servers(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS nodes(id INTEGER PRIMARY KEY, name TEXT NOT NULL,
                    server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE RESTRICT);
                CREATE TABLE IF NOT EXISTS quotas(kind TEXT PRIMARY KEY, value INTEGER);
                CREATE TABLE IF NOT EXISTS agents(server_id INTEGER PRIMARY KEY REFERENCES servers(id) ON DELETE CASCADE,
                    token_hash TEXT NOT NULL UNIQUE, last_seen REAL, uptime REAL, hostname TEXT, session_id TEXT);
                CREATE TABLE IF NOT EXISTS agent_sessions(
                    server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
                    session_id TEXT NOT NULL, PRIMARY KEY(server_id,session_id));
                CREATE TABLE IF NOT EXISTS server_configs(server_id INTEGER PRIMARY KEY REFERENCES servers(id) ON DELETE CASCADE,
                    revision INTEGER NOT NULL, staged_revision INTEGER, staged_digest TEXT, staged_at REAL);
                CREATE TABLE IF NOT EXISTS node_configs(node_id INTEGER PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
                    payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS runtime_receipts(server_id INTEGER PRIMARY KEY REFERENCES servers(id) ON DELETE CASCADE,
                    revision INTEGER NOT NULL, digest TEXT NOT NULL, reported_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS node_users(node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, credential TEXT NOT NULL UNIQUE,
                    PRIMARY KEY(node_id,user_id));
                CREATE TABLE IF NOT EXISTS automatic_bandwidth_global(id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS automatic_bandwidth_users(user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS user_bandwidth(user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                    payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS user_connections(user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                    payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS server_bandwidth(server_id INTEGER PRIMARY KEY REFERENCES servers(id) ON DELETE CASCADE,
                    payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS plans(id INTEGER PRIMARY KEY, name TEXT NOT NULL, bandwidth TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS plan_connections(plan_id INTEGER PRIMARY KEY REFERENCES plans(id) ON DELETE CASCADE,
                    payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS user_plans(user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                    plan_id INTEGER NOT NULL REFERENCES plans(id) ON DELETE RESTRICT);
                CREATE TABLE IF NOT EXISTS temporary_bandwidth(user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                    payload TEXT NOT NULL, expires_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS reality_domains(id INTEGER PRIMARY KEY AUTOINCREMENT,
                    domain TEXT NOT NULL, port INTEGER NOT NULL, enabled INTEGER NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 1, probe_generation INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'not_checked', checked_at REAL, result TEXT,
                    UNIQUE(domain,port));
                CREATE TABLE IF NOT EXISTS reality_failover(
                    node_id INTEGER PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
                    settings TEXT NOT NULL, state TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS reality_server_probes(
                    server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
                    domain_id INTEGER NOT NULL REFERENCES reality_domains(id) ON DELETE CASCADE,
                    domain_revision INTEGER NOT NULL, probe_generation INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'not_checked', checked_at REAL, result TEXT,
                    PRIMARY KEY(server_id,domain_id));
                CREATE TABLE IF NOT EXISTS reality_probe_jobs(
                    id TEXT PRIMARY KEY, server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
                    domain_id INTEGER NOT NULL REFERENCES reality_domains(id) ON DELETE CASCADE,
                    domain_revision INTEGER NOT NULL, probe_generation INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending', result TEXT, created_at REAL NOT NULL,
                    claimed_at REAL, claim_token TEXT, completed_at REAL);
                CREATE TABLE IF NOT EXISTS reality_probe_schedules(
                    domain_id INTEGER PRIMARY KEY REFERENCES reality_domains(id) ON DELETE CASCADE,
                    server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
                    interval_seconds INTEGER NOT NULL, next_run_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS schema_migrations(name TEXT PRIMARY KEY);
                INSERT OR IGNORE INTO server_configs(server_id,revision) SELECT id,1 FROM servers;
            ''')
            from grant_ack import SCHEMA as GRANT_ACK_SCHEMA
            db.executescript(GRANT_ACK_SCHEMA)
            from grant_capabilities import SCHEMA as GRANT_CAPABILITIES_SCHEMA
            db.executescript(GRANT_CAPABILITIES_SCHEMA)
            from grant_lifecycle import SCHEMA as GRANT_LIFECYCLE_SCHEMA
            db.executescript(GRANT_LIFECYCLE_SCHEMA)
            from grant_execution import SCHEMA as GRANT_EXECUTION_SCHEMA
            db.executescript(GRANT_EXECUTION_SCHEMA)
            from sharing import SCHEMA as SHARING_SCHEMA
            db.executescript(SHARING_SCHEMA)
            from plan_lifecycle import SCHEMA as LIFECYCLE_SCHEMA
            db.executescript(LIFECYCLE_SCHEMA)
            from plan_grace import SCHEMA as GRACE_SCHEMA
            db.executescript(GRACE_SCHEMA)
            from plan_renewals import SCHEMA as RENEWALS_SCHEMA
            db.executescript(RENEWALS_SCHEMA)
            from accounts import SCHEMA as ACCOUNTS_SCHEMA
            db.executescript(ACCOUNTS_SCHEMA)
            from plan_order import SCHEMA as PLAN_ORDER_SCHEMA
            db.executescript(PLAN_ORDER_SCHEMA)
            from usage_ledger import SCHEMA as USAGE_SCHEMA
            db.executescript(USAGE_SCHEMA)
            from usage_settlement import SCHEMA as SETTLEMENT_SCHEMA
            db.executescript(SETTLEMENT_SCHEMA)
            from usage_cycles import SCHEMA as CYCLES_SCHEMA
            db.executescript(CYCLES_SCHEMA)
            # Additive migration: bind newly accepted deltas to the immutable identity cycle when present.
            if not any(row['name']=='cycle_id' for row in db.execute('PRAGMA table_info(usage_deltas)')):
                db.execute('ALTER TABLE usage_deltas ADD COLUMN cycle_id TEXT REFERENCES usage_cycles(id) ON DELETE SET NULL')
            if not any(row['name']=='cycle_id' for row in db.execute('PRAGMA table_info(usage_execution_grants)')):
                db.execute('ALTER TABLE usage_execution_grants ADD COLUMN cycle_id TEXT REFERENCES usage_cycles(id) ON DELETE SET NULL')
            from plan_nodes import SCHEMA as PLAN_NODES_SCHEMA
            db.executescript(PLAN_NODES_SCHEMA)
            from node_groups import SCHEMA as NODE_GROUPS_SCHEMA
            db.executescript(NODE_GROUPS_SCHEMA)
            from plan_node_bandwidth import SCHEMA as PLAN_NODE_BANDWIDTH_SCHEMA
            db.executescript(PLAN_NODE_BANDWIDTH_SCHEMA)
            from plan_node_connections import SCHEMA as PLAN_NODE_CONNECTIONS_SCHEMA
            db.executescript(PLAN_NODE_CONNECTIONS_SCHEMA)
            # A format change is a new configuration, even when the underlying
            # business rule did not change. Serialize migration with DB writers
            # and persist its marker atomically with all affected revisions.
            db.execute('BEGIN IMMEDIATE')
            # These tables are part of the attributed-accounting migration.
            # Keep their DDL, seeded evidence and marker in one transaction;
            # executescript() would commit the DDL before the evidence exists.
            from usage_accounting import SCHEMA as ACCOUNTING_SCHEMA
            for statement in ACCOUNTING_SCHEMA.split(';'):
                if statement.strip():
                    db.execute(statement)
            probe_columns={column['name'] for column in db.execute('PRAGMA table_info(reality_probe_jobs)')}
            if 'claim_token' not in probe_columns:
                db.execute('ALTER TABLE reality_probe_jobs ADD COLUMN claim_token TEXT')
            if 'attempts' not in probe_columns:
                db.execute('ALTER TABLE reality_probe_jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0')
                db.execute("UPDATE reality_probe_jobs SET attempts=1 WHERE status='claimed'")
            if 'next_attempt_at' not in probe_columns:
                db.execute('ALTER TABLE reality_probe_jobs ADD COLUMN next_attempt_at REAL NOT NULL DEFAULT 0')
            # Legacy results belong only to the server of the matching job.
            # Never copy a master/other-server success into every host's state.
            migration='per-server-reality-probes-v1'
            if db.execute('SELECT 1 FROM schema_migrations WHERE name=?',(migration,)).fetchone() is None:
                db.execute('''INSERT OR IGNORE INTO reality_server_probes
                    (server_id,domain_id,domain_revision,probe_generation,status,checked_at,result)
                    SELECT j.server_id,d.id,d.revision,d.probe_generation,d.status,d.checked_at,d.result
                    FROM reality_probe_jobs j JOIN reality_domains d ON d.id=j.domain_id
                    WHERE j.domain_revision=d.revision AND j.probe_generation=d.probe_generation
                    AND j.status IN ('pending','claimed','completed')''')
                db.execute("UPDATE reality_domains SET status='not_checked',checked_at=NULL,result=NULL "
                           "WHERE id IN (SELECT domain_id FROM reality_server_probes)")
                db.execute('INSERT INTO schema_migrations(name) VALUES(?)',(migration,))
            if 'session_id' not in {column['name'] for column in db.execute('PRAGMA table_info(agents)')}:
                db.execute('ALTER TABLE agents ADD COLUMN session_id TEXT')
            from usage_accounting import migrate as migrate_accounting
            migrate_accounting(db)
            migration='per-user-temporary-deadline-v1'
            if db.execute('SELECT 1 FROM schema_migrations WHERE name=?',(migration,)).fetchone() is None:
                affected=set()
                for row in db.execute('''SELECT DISTINCT n.server_id,c.payload FROM nodes n
                    JOIN node_configs c ON c.node_id=n.id
                    JOIN node_users u ON u.node_id=n.id
                    JOIN temporary_bandwidth t ON t.user_id=u.user_id
                    WHERE t.expires_at>?''',(time.time(),)):
                    config=json.loads(row['payload'])
                    if config.get('protocol')=='vless' and config['enabled']:
                        affected.add(row['server_id'])
                for server_id in affected: self._bump(db,server_id)
                db.execute('INSERT INTO schema_migrations(name) VALUES(?)',(migration,))
        os.chmod(self.path,0o600)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON')
        if db.execute('PRAGMA foreign_keys').fetchone()[0] != 1:
            db.close()
            raise RuntimeError('SQLite foreign key enforcement is required')
        try:
            with db:
                yield db
        finally:
            db.close()

    def kind(self, kind):
        if kind not in KINDS:
            raise Error('资源类型不存在', 404)
        return kind

    def fields(self, kind, data):
        allowed = {'name', 'server_id'} if kind == 'nodes' else {'name'}
        if not isinstance(data, dict) or set(data) != allowed:
            raise Error('字段不完整或包含不支持的字段')
        name = data['name']
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 120:
            raise Error('名称长度应为 1–120 个字符')
        result = {'name': name.strip()}
        if kind == 'nodes':
            if type(data['server_id']) is not int or data['server_id'] < 1:
                raise Error('服务器编号无效')
            result['server_id'] = data['server_id']
        return result

    @contextmanager
    def _resource_transaction(self,db=None,*,write=False):
        """Explicit caller-owned transaction; never commits/closes a borrowed DB."""
        if db is not None:
            if not db.in_transaction or db.execute('PRAGMA foreign_keys').fetchone()[0]!=1:
                raise Error('调用者资源事务无效',503)
            yield db
        else:
            with self.connect() as connection:
                connection.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
                yield connection

    def list(self, kind, *, versioned=False, db=None):
        self.kind(kind)
        with self._resource_transaction(db) as db:
            if kind=='nodes' and versioned:
                from node_admin import response
                return [response(db,row) for row in db.execute('SELECT * FROM nodes ORDER BY id')]
            if kind=='servers' and versioned:
                from server_admin import response
                return [response(row) for row in db.execute('SELECT * FROM servers ORDER BY id')]
            return [dict(row) for row in db.execute(f'SELECT * FROM {kind} ORDER BY id')]

    def quotas(self,*,db=None):
        with self._resource_transaction(db) as db:
            configured = dict(db.execute('SELECT kind,value FROM quotas'))
            return {kind: {'limit': configured.get(kind), 'used': db.execute(f'SELECT COUNT(*) FROM {kind}').fetchone()[0]} for kind in KINDS}

    def set_quota(self, kind, value,*,db=None):
        self.kind(kind)
        if value is not None and (type(value) is not int or value < 0):
            raise Error('配额应为非负整数，null 表示不限制')
        with self._resource_transaction(db,write=True) as db:
            used = db.execute(f'SELECT COUNT(*) FROM {kind}').fetchone()[0]
            if value is not None and value < used:
                raise Error('配额不能低于当前资源数量', 409)
            db.execute('INSERT INTO quotas(kind,value) VALUES(?,?) ON CONFLICT(kind) DO UPDATE SET value=excluded.value', (kind, value))

    def save(self, kind, data, resource_id=None, *, if_match=None, versioned=False,db=None):
        self.kind(kind)
        if kind=='nodes':
            from node_admin import validate,response
            validate(if_match)
            if resource_id is None and if_match is not None: raise Error('创建节点不接受编辑版本')
        elif kind=='servers':
            from server_admin import validate
            validate(if_match)
            if resource_id is None and if_match is not None: raise Error('创建服务器不接受编辑版本')
        elif if_match is not None: raise Error('此资源不支持编辑版本')
        fields = self.fields(kind, data)
        try:
            with self._resource_transaction(db,write=True) as db:
                old = db.execute(f'SELECT * FROM {kind} WHERE id=?',(resource_id,)).fetchone() if resource_id is not None else None
                if kind=='nodes' and resource_id is not None and if_match is not None:
                    if old is None: raise Error('资源不存在',404)
                    if response(db,old)['version']!=if_match: raise Error('节点配置已变化，请重新读取后再编辑',412)
                if kind=='servers' and resource_id is not None and if_match is not None:
                    from server_admin import response as server_response
                    if old is None: raise Error('资源不存在',404)
                    if server_response(old)['version']!=if_match: raise Error('服务器记录已变化，请重新读取后再编辑',412)
                if kind=='nodes' and old is not None:
                    if fields['server_id']!=old['server_id'] and db.execute('SELECT 1 FROM shared_nodes WHERE node_id=?',(resource_id,)).fetchone():
                        raise Error('分享节点不能迁移到其他服务器',409)
                    stored=db.execute('SELECT payload FROM node_configs WHERE node_id=?',(resource_id,)).fetchone()
                    if stored: self._check_port(db,fields['server_id'],resource_id,json.loads(stored['payload']))
                if resource_id is None:
                    quota = db.execute('SELECT value FROM quotas WHERE kind=?', (kind,)).fetchone()
                    if quota is not None and quota[0] is not None:
                        if db.execute(f'SELECT COUNT(*) FROM {kind}').fetchone()[0] >= quota[0]:
                            raise Error('已达到管理员设置的资源配额', 409)
                    keys = list(fields)
                    cursor = db.execute(f'INSERT INTO {kind}({",".join(keys)}) VALUES({",".join("?" for _ in keys)})', tuple(fields.values()))
                    resource_id = cursor.lastrowid
                else:
                    cursor = db.execute(f'UPDATE {kind} SET '+','.join(key+'=?' for key in fields)+' WHERE id=?', (*fields.values(), resource_id))
                    if not cursor.rowcount:
                        raise Error('资源不存在', 404)
                if kind=='servers':
                    if old is None: db.execute('INSERT INTO server_configs(server_id,revision) VALUES(?,1)',(resource_id,))
                    else: self._bump(db,resource_id)
                if kind=='nodes':
                    for server in {fields['server_id']} | ({old['server_id']} if old else set()): self._bump(db,server)
                row=db.execute(f'SELECT * FROM {kind} WHERE id=?', (resource_id,)).fetchone()
                if kind=='servers' and versioned:
                    from server_admin import response as server_response
                    return server_response(row)
                return response(db,row) if kind=='nodes' and versioned else dict(row)
        except sqlite3.IntegrityError:
            raise Error('关联的服务器不存在', 409) from None

    def delete(self, kind, resource_id,*,db=None):
        self.kind(kind)
        try:
            with self._resource_transaction(db,write=True) as db:
                old=db.execute(f'SELECT * FROM {kind} WHERE id=?',(resource_id,)).fetchone()
                if kind=='users' and db.execute('SELECT 1 FROM plan_accounts WHERE parent_id=?',(resource_id,)).fetchone():
                    raise Error('用户仍有套餐子账号，请先移除子账号',409)
                affected=[row[0] for row in db.execute('SELECT DISTINCT n.server_id FROM nodes n JOIN node_users u ON u.node_id=n.id WHERE u.user_id=?',(resource_id,))] if kind=='users' else []
                if kind=='nodes':
                    # Preserve known pre-upgrade usage before cascading detail
                    # deletion. Removing a node must not mint cycle balance.
                    from total_usage import preserve_nodes
                    preserve_nodes(db,[resource_id])
                cursor = db.execute(f'DELETE FROM {kind} WHERE id=?', (resource_id,))
                if not cursor.rowcount:
                    raise Error('资源不存在', 404)
                if kind=='nodes': self._bump(db,old['server_id'])
                for server_id in affected: self._bump(db,server_id)
        except sqlite3.IntegrityError:
            raise Error('服务器仍有关联节点，请先迁移或删除节点', 409) from None

    def provision_agent(self, server_id):
        token = secrets.token_urlsafe(32)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT id FROM servers WHERE id=?',(server_id,)).fetchone() is None:
                raise Error('服务器不存在',404)
            db.execute('INSERT INTO agents(server_id,token_hash) VALUES(?,?) ON CONFLICT(server_id) DO UPDATE SET token_hash=excluded.token_hash,last_seen=NULL,uptime=NULL,hostname=NULL,session_id=NULL',
                       (server_id,hashlib.sha256(token.encode()).hexdigest()))
            db.execute('UPDATE server_configs SET staged_revision=NULL,staged_digest=NULL,staged_at=NULL WHERE server_id=?',(server_id,))
            db.execute('DELETE FROM runtime_receipts WHERE server_id=?',(server_id,))
        return {'server_id':server_id,'token':token}

    def revoke_agent(self, server_id):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if not db.execute('DELETE FROM agents WHERE server_id=?',(server_id,)).rowcount:
                raise Error('接入凭据不存在',404)
            db.execute('UPDATE server_configs SET staged_revision=NULL,staged_digest=NULL,staged_at=NULL WHERE server_id=?',(server_id,))
            db.execute('DELETE FROM runtime_receipts WHERE server_id=?',(server_id,))

    def heartbeat(self, token, data):
        if not isinstance(token,str) or not 32 <= len(token) <= 128:
            raise Error('Agent 凭据无效',401)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT server_id,uptime,session_id FROM agents WHERE token_hash=?',(hashlib.sha256(token.encode()).hexdigest(),)).fetchone()
            if row is None: raise Error('Agent 凭据无效',401)
            if not isinstance(data,dict) or set(data) not in ({'uptime','hostname'},{'uptime','hostname','session_id'}):
                raise Error('心跳字段无效')
            uptime, hostname = data['uptime'], data['hostname']
            if type(uptime) not in (int,float) or not math.isfinite(uptime) or uptime < 0:
                raise Error('运行时间无效')
            if not isinstance(hostname,str) or not 1 <= len(hostname.strip()) <= 255:
                raise Error('主机名称无效')
            if 'session_id' in data: self._session_value(data['session_id'])
            self._check_session(db,row['server_id'],data.get('session_id'))
            # A restarted process must not inherit a recently reported listener
            # state from the previous process. Validate the heartbeat first so
            # malformed input cannot invalidate otherwise current receipts.
            if row['uptime'] is not None and uptime < row['uptime']:
                db.execute('UPDATE server_configs SET staged_revision=NULL,staged_digest=NULL,staged_at=NULL WHERE server_id=?',(row['server_id'],))
                db.execute('DELETE FROM runtime_receipts WHERE server_id=?',(row['server_id'],))
            db.execute('UPDATE agents SET last_seen=?,uptime=?,hostname=? WHERE server_id=?',(time.time(),uptime,hostname.strip(),row['server_id']))
            return {'accepted':True,'server_id':row['server_id'],
                    'session_id':data.get('session_id')}

    def reality_probe_schedule(self, domain_id, data=None, delete=False):
        if data is not None and (not isinstance(data,dict) or set(data)!={'server_id','interval_seconds'}
                or type(data['server_id']) is not int or data['server_id']<1
                or type(data['interval_seconds']) is not int or not 300<=data['interval_seconds']<=86400):
            raise Error('巡检需要服务器编号及300–86400秒的整数间隔')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            domain=db.execute('SELECT * FROM reality_domains WHERE id=?',(domain_id,)).fetchone()
            if domain is None: raise Error('域名记录不存在',404)
            if delete:
                db.execute('DELETE FROM reality_probe_schedules WHERE domain_id=?',(domain_id,))
            elif data is not None:
                if not domain['enabled']: raise Error('请先启用该域名',409)
                if db.execute('SELECT 1 FROM agents WHERE server_id=?',(data['server_id'],)).fetchone() is None:
                    raise Error('服务器尚未接入 Agent',409)
                db.execute('INSERT INTO reality_probe_schedules(domain_id,server_id,interval_seconds,next_run_at) VALUES(?,?,?,?) '
                           'ON CONFLICT(domain_id) DO UPDATE SET server_id=excluded.server_id,'
                           'interval_seconds=excluded.interval_seconds,next_run_at=excluded.next_run_at '
                           'WHERE server_id!=excluded.server_id OR interval_seconds!=excluded.interval_seconds',
                           (domain_id,data['server_id'],data['interval_seconds'],time.time()))
            row=db.execute('SELECT * FROM reality_probe_schedules WHERE domain_id=?',(domain_id,)).fetchone()
            return {'schedule':dict(row) if row else None}

    @staticmethod
    def _enqueue_reality_probe(db, server_id, row, now):
        db.execute("UPDATE reality_probe_jobs SET status='discarded',completed_at=?,claim_token=NULL "
                   "WHERE domain_id=? AND server_id=? AND status IN ('pending','claimed')",(now,row['id'],server_id))
        previous=db.execute('SELECT probe_generation FROM reality_server_probes WHERE server_id=? AND domain_id=?',
                            (server_id,row['id'])).fetchone()
        generation=previous['probe_generation']+1 if previous else 1
        db.execute('INSERT INTO reality_server_probes(server_id,domain_id,domain_revision,probe_generation,status) '
                   "VALUES(?,?,?,?,'checking') ON CONFLICT(server_id,domain_id) DO UPDATE SET "
                   "domain_revision=excluded.domain_revision,probe_generation=excluded.probe_generation,"
                   "status='checking',checked_at=NULL,result=NULL",
                   (server_id,row['id'],row['revision'],generation))
        job=secrets.token_urlsafe(24)
        db.execute('INSERT INTO reality_probe_jobs(id,server_id,domain_id,domain_revision,probe_generation,created_at) VALUES(?,?,?,?,?,?)',
                   (job,server_id,row['id'],row['revision'],generation,now))
        return {'job_id':job,'server_id':server_id,'domain_id':row['id'],'domain_revision':row['revision'],
                'domain':row['domain'],'port':row['port'],'status':'pending'}

    def create_reality_probe_job(self, server_id, domain_id):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM servers WHERE id=?', (server_id,)).fetchone() is None:
                raise Error('服务器不存在',404)
            row=db.execute('SELECT * FROM reality_domains WHERE id=?', (domain_id,)).fetchone()
            if row is None: raise Error('域名记录不存在',404)
            if not row['enabled']: raise Error('请先启用该域名',409)
            agent=db.execute('SELECT 1 FROM agents WHERE server_id=?', (server_id,)).fetchone()
            if agent is None: raise Error('服务器尚未接入 Agent',409)
            now=time.time()
            job=self._enqueue_reality_probe(db,server_id,row,now)
            db.execute('UPDATE reality_probe_schedules SET next_run_at=?+interval_seconds WHERE domain_id=? AND server_id=?',
                       (now,domain_id,server_id))
            return job

    def claim_reality_probe(self, token):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE'); server_id=self._agent_id(db,token)
            now=time.time()
            # Retire obsolete work before selecting the queue head. Otherwise a
            # changed/disabled candidate can be dispatched repeatedly and starve
            # current jobs even though its eventual result would be rejected.
            db.execute("UPDATE reality_probe_jobs SET status='discarded',completed_at=?,claim_token=NULL "
                       "WHERE server_id=? AND status IN ('pending','claimed') AND NOT EXISTS "
                       "(SELECT 1 FROM reality_domains d JOIN reality_server_probes p ON p.domain_id=d.id "
                       "WHERE d.id=reality_probe_jobs.domain_id AND p.server_id=reality_probe_jobs.server_id "
                       "AND d.enabled=1 AND d.revision=reality_probe_jobs.domain_revision "
                       "AND p.domain_revision=d.revision AND p.probe_generation=reality_probe_jobs.probe_generation)",
                       (now,server_id))
            # Exhausted workers must not keep a candidate "checking" forever.
            # Obsolete generations were retired above, so only current results
            # can be published here. No node configuration is changed.
            expired=db.execute("SELECT id,domain_id FROM reality_probe_jobs WHERE server_id=? "
                               "AND status='claimed' AND claimed_at<=? AND attempts>=?",
                               (server_id,now-PROBE_JOB_LEASE,PROBE_MAX_ATTEMPTS)).fetchall()
            for job in expired:
                result=json.dumps({'ok':False,'error':'lease_expired'})
                db.execute("UPDATE reality_probe_jobs SET status='completed',completed_at=?,result=?,claim_token=NULL WHERE id=?",
                           (now,result,job['id']))
                db.execute("UPDATE reality_server_probes SET status='unavailable',checked_at=?,result=? WHERE domain_id=? AND server_id=?",
                           (now,result,job['domain_id'],server_id))
            # Poll-driven scheduling: no network work in the DB transaction,
            # no offline backlog, and no replacement of active manual/retry jobs.
            due=db.execute('SELECT d.* FROM reality_probe_schedules s JOIN reality_domains d ON d.id=s.domain_id '
                           'WHERE s.server_id=? AND s.next_run_at<=? AND d.enabled=1 AND NOT EXISTS '
                           "(SELECT 1 FROM reality_probe_jobs j WHERE j.domain_id=d.id AND j.server_id=s.server_id "
                           "AND j.status IN ('pending','claimed') AND j.domain_revision=d.revision) "
                           'ORDER BY s.next_run_at,d.id LIMIT 16',(server_id,now)).fetchall()
            for domain in due:
                self._enqueue_reality_probe(db,server_id,domain,now)
                db.execute('UPDATE reality_probe_schedules SET next_run_at=?+interval_seconds WHERE domain_id=?',
                           (now,domain['id']))
            row=db.execute("SELECT j.*,d.domain,d.port FROM reality_probe_jobs j JOIN reality_domains d ON d.id=j.domain_id "
                           "WHERE j.server_id=? AND j.attempts<? AND j.next_attempt_at<=? "
                           "AND (j.status='pending' OR (j.status='claimed' AND j.claimed_at<=?)) "
                           "ORDER BY j.next_attempt_at,j.created_at,j.id LIMIT 1",
                           (server_id,PROBE_MAX_ATTEMPTS,now,now-PROBE_JOB_LEASE)).fetchone()
            if row is None: return {'job':None}
            lease=secrets.token_urlsafe(24)
            db.execute("UPDATE reality_probe_jobs SET status='claimed',claimed_at=?,claim_token=?,attempts=attempts+1 WHERE id=?",(now,lease,row['id']))
            return {'job':{'job_id':row['id'],'domain_id':row['domain_id'],'domain_revision':row['domain_revision'],
                           'domain':row['domain'],'port':row['port'],'lease_token':lease}}

    def complete_reality_probe(self, token, job_id, lease_token, result):
        if not isinstance(job_id,str) or not 8 <= len(job_id) <= 128 or not isinstance(lease_token,str) or not 16 <= len(lease_token) <= 128 or not isinstance(result,dict):
            raise Error('探测回执无效')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE'); server_id=self._agent_id(db,token)
            job=db.execute('SELECT * FROM reality_probe_jobs WHERE id=? AND server_id=?',(job_id,server_id)).fetchone()
            if job is None: raise Error('探测任务不存在',404)
            now=time.time()
            if (job['status'] != 'claimed' or job['claimed_at'] is None
                    or now >= job['claimed_at'] + PROBE_JOB_LEASE
                    or not hmac.compare_digest(job['claim_token'] or '',lease_token)):
                raise Error('探测任务已处理、未领取或租约已失效',409)
            domain=db.execute('SELECT * FROM reality_domains WHERE id=?',(job['domain_id'],)).fetchone()
            if domain is None: raise Error('域名记录已删除',409)
            state=db.execute('SELECT * FROM reality_server_probes WHERE server_id=? AND domain_id=?',
                             (server_id,domain['id'])).fetchone()
            if (not domain['enabled'] or domain['revision'] != job['domain_revision'] or state is None
                    or state['domain_revision'] != domain['revision']
                    or state['probe_generation'] != job['probe_generation']):
                db.execute("UPDATE reality_probe_jobs SET status='discarded',completed_at=? WHERE id=?",(time.time(),job_id))
                raise Error('域名已修改或重新探测，丢弃旧结果',409)
            # A successful receipt must meet the same contract as selection and
            # binding. Do not publish "available" for an incomplete success or
            # consume its claim: the Agent can submit a corrected receipt.
            if type(result.get('ok')) is not bool:
                raise Error('探测结果需要布尔值 ok')
            ok=result['ok']
            latency=result.get('latency_ms')
            if ok and (result.get('tls_version') != 'TLSv1.3' or result.get('alpn') != 'h2'
                       or type(latency) not in (int,float) or not math.isfinite(latency) or latency < 0
                       or result.get('error') is not None):
                raise Error('成功探测需要 TLS 1.3、h2 和有效延迟，且不能包含错误')
            try:
                encoded=json.dumps(result,allow_nan=False)
            except (ValueError,TypeError,OverflowError):
                raise Error('探测结果不是有效 JSON') from None
            db.execute('UPDATE reality_server_probes SET status=?,checked_at=?,result=? WHERE domain_id=? AND server_id=?',
                       ('available' if ok else 'unavailable',now,encoded,domain['id'],server_id))
            retry=(not ok and isinstance(result.get('error'),str)
                   and result['error'] in PROBE_RETRY_ERRORS and job['attempts'] < PROBE_MAX_ATTEMPTS)
            if retry:
                next_attempt=now+PROBE_RETRY_DELAY * (2 ** (job['attempts']-1))
                db.execute("UPDATE reality_probe_jobs SET status='pending',result=?,next_attempt_at=?,"
                           "claim_token=NULL,claimed_at=NULL,completed_at=NULL WHERE id=?",
                           (encoded,next_attempt,job_id))
                return {'job_id':job_id,'status':'unavailable','result':result,
                        'retry_pending':True,'next_attempt_at':next_attempt}
            db.execute("UPDATE reality_probe_jobs SET status='completed',result=?,completed_at=? WHERE id=?",
                       (encoded,now,job_id))
            return {'job_id':job_id,'status':'available' if ok else 'unavailable','result':result}

    def _agent_id(self, db, token):
        if not isinstance(token,str) or not 32 <= len(token) <=128: raise Error('Agent 凭据无效',401)
        row=db.execute('SELECT server_id FROM agents WHERE token_hash=?',(hashlib.sha256(token.encode()).hexdigest(),)).fetchone()
        if row is None: raise Error('Agent 凭据无效',401)
        return row['server_id']

    @staticmethod
    def _session_value(value):
        if not isinstance(value,str): raise Error('Agent 会话无效')
        try:
            if str(uuid.UUID(value))!=value: raise ValueError()
        except (ValueError,AttributeError): raise Error('Agent 会话无效')
        return value

    def _check_session(self,db,server_id,session):
        active=db.execute('SELECT session_id FROM agents WHERE server_id=?',(server_id,)).fetchone()['session_id']
        if session is not None: self._session_value(session)
        if active!=session: raise Error('Agent 会话已失效，请重新启动并同步',403)

    def begin_session(self,token,data):
        if not isinstance(data,dict) or set(data)!={'session_id'}: raise Error('Agent 会话字段无效')
        session=self._session_value(data['session_id'])
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            server_id=self._agent_id(db,token)
            active=db.execute('SELECT session_id FROM agents WHERE server_id=?',(server_id,)).fetchone()['session_id']
            if active!=session:
                if db.execute('SELECT 1 FROM agent_sessions WHERE server_id=? AND session_id=?',(server_id,session)).fetchone():
                    raise Error('Agent 旧会话不可重新启用',403)
                db.execute('INSERT INTO agent_sessions(server_id,session_id) VALUES(?,?)',(server_id,session))
                db.execute('UPDATE agents SET session_id=?,last_seen=NULL,uptime=NULL,hostname=NULL WHERE server_id=?',(session,server_id))
                db.execute('UPDATE server_configs SET staged_revision=NULL,staged_digest=NULL,staged_at=NULL WHERE server_id=?',(server_id,))
                db.execute('DELETE FROM runtime_receipts WHERE server_id=?',(server_id,))
            return {'accepted':True,'server_id':server_id,'session_id':session}

    def _bump(self, db, server_id):
        db.execute('UPDATE server_configs SET revision=revision+1 WHERE server_id=?',(server_id,))

    def _check_port(self, db, server_id, node_id, config):
        if not config['enabled']: return
        for row in db.execute('SELECT c.payload FROM node_configs c JOIN nodes n ON n.id=c.node_id WHERE n.server_id=? AND n.id!=?',(server_id,node_id)):
            existing=json.loads(row['payload'])
            if existing['enabled'] and existing['listen_port']==config['listen_port']:
                raise Error('该服务器已有节点使用此监听端口',409)

    def configure_node(self, node_id, data):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            return self._configure_node(db, node_id, data)

    def _configure_node(self, db, node_id, data):
        """Validate and store inside the caller's write transaction."""
        try: config=profile(data)
        except ValueError as error: raise Error(str(error)) from None
        node=db.execute('SELECT server_id FROM nodes WHERE id=?',(node_id,)).fetchone()
        if node is None: raise Error('节点不存在',404)
        if config.get('protocol')!='vless' and db.execute('SELECT 1 FROM node_users WHERE node_id=?',(node_id,)).fetchone():
            raise Error('请先撤销此节点的用户授权，再改为 TCP 转发',409)
        self._check_port(db,node['server_id'],node_id,config)
        encoded=json.dumps(config,sort_keys=True)
        old=db.execute('SELECT payload FROM node_configs WHERE node_id=?',(node_id,)).fetchone()
        if old is None or old['payload']!=encoded:
            db.execute('INSERT INTO node_configs(node_id,payload) VALUES(?,?) ON CONFLICT(node_id) DO UPDATE SET payload=excluded.payload',(node_id,encoded))
            self._bump(db,node['server_id'])
        return config

    def authorize_node_user(self, node_id, user_id, revoke=False):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            node=db.execute('SELECT n.server_id,c.payload FROM nodes n LEFT JOIN node_configs c ON c.node_id=n.id WHERE n.id=?',(node_id,)).fetchone()
            if node is None or db.execute('SELECT 1 FROM users WHERE id=?',(user_id,)).fetchone() is None:
                raise Error('节点或用户不存在',404)
            if revoke:
                if not db.execute('DELETE FROM node_users WHERE node_id=? AND user_id=?',(node_id,user_id)).rowcount:
                    raise Error('用户未授权到此节点',404)
                self._bump(db,node['server_id'])
                return {'revoked':True}
            if not node['payload'] or json.loads(node['payload']).get('protocol')!='vless':
                raise Error('仅 VLESS 节点支持用户授权',409)
            from plan_nodes import check_user
            check_user(db,user_id,[node_id])
            existing=db.execute('SELECT credential FROM node_users WHERE node_id=? AND user_id=?',(node_id,user_id)).fetchone()
            if existing: return {'user_id':user_id,'uuid':existing['credential']}
            credential=str(uuid.uuid4())
            db.execute('INSERT INTO node_users(node_id,user_id,credential) VALUES(?,?,?)',(node_id,user_id,credential))
            self._bump(db,node['server_id'])
            return {'user_id':user_id,'uuid':credential}

    @staticmethod
    def _effective_bandwidth(db, user_id):
        from plan_policy import bandwidth
        return bandwidth(db,user_id)

    @staticmethod
    def _automatic_policy(db, user_id):
        row=db.execute('SELECT payload FROM automatic_bandwidth_users WHERE user_id=?',(user_id,)).fetchone()
        if row: return json.loads(row['payload'])
        row=db.execute('SELECT payload FROM automatic_bandwidth_global WHERE id=1').fetchone()
        return json.loads(row['payload']) if row else {'enabled':False}

    def automatic_bandwidth(self, user_id=None, data=None, *, inherit=False, db=None):
        if data is not None:
            try: data=automatic_bandwidth(data)
            except ValueError as error: raise Error(str(error)) from None
        with self._resource_transaction(db,write=True) as db:
            if user_id is not None and db.execute('SELECT 1 FROM users WHERE id=?',(user_id,)).fetchone() is None:
                raise Error('用户不存在',404)
            users=[row[0] for row in db.execute('SELECT id FROM users')] if user_id is None else [user_id]
            before={uid:self._automatic_policy(db,uid) for uid in users}
            table='automatic_bandwidth_global' if user_id is None else 'automatic_bandwidth_users'
            key='id' if user_id is None else 'user_id'
            ident=1 if user_id is None else user_id
            if inherit: db.execute(f'DELETE FROM {table} WHERE {key}=?',(ident,))
            elif data is not None:
                db.execute(f'INSERT INTO {table}({key},payload) VALUES(?,?) ON CONFLICT({key}) DO UPDATE SET payload=excluded.payload',
                           (ident,json.dumps(data,sort_keys=True)))
            affected=set()
            for uid,previous in before.items():
                if previous!=self._automatic_policy(db,uid):
                    affected.update(row[0] for row in db.execute('SELECT DISTINCT n.server_id FROM nodes n JOIN node_users u ON n.id=u.node_id WHERE u.user_id=?',(uid,)))
            for sid in affected: self._bump(db,sid)
            row=db.execute(f'SELECT payload FROM {table} WHERE {key}=?',(ident,)).fetchone()
            return {'source':'global' if user_id is None else ('user' if row else 'global'),
                    'override':json.loads(row['payload']) if row else None,
                    'effective':self._automatic_policy(db,user_id)}

    def _user_execution_policy(self, db, user_id):
        temporary=db.execute('SELECT expires_at FROM temporary_bandwidth WHERE user_id=?',(user_id,)).fetchone()
        from plan_node_bandwidth import override_for_user
        routes=tuple((r[0],json.dumps(override_for_user(db,user_id,r[0]),sort_keys=True)) for r in db.execute('SELECT node_id FROM node_users WHERE user_id=? ORDER BY node_id',(user_id,)))
        from plan_node_connections import override_for_user as connection_override
        from plan_lifecycle import policy as lifecycle_policy
        connection_routes=tuple((r[0],json.dumps(connection_override(db,user_id,r[0]),sort_keys=True)) for r in db.execute('SELECT node_id FROM node_users WHERE user_id=? ORDER BY node_id',(user_id,)))
        return self._effective_bandwidth(db,user_id), temporary['expires_at'] if temporary else None, self._effective_connections(db,user_id),routes,connection_routes,lifecycle_policy(db,user_id)

    def _expire_bandwidth(self, db):
        expired=db.execute('SELECT * FROM temporary_bandwidth WHERE expires_at<=?',(time.time(),)).fetchall()
        before={row['user_id']:self._user_execution_policy(db,row['user_id']) for row in expired}
        for row in expired: db.execute('DELETE FROM temporary_bandwidth WHERE user_id=?',(row['user_id'],))
        self._bump_changed_users(db,before)

    def _bump_changed_users(self, db, before):
        affected=set()
        for user_id,previous in before.items():
            if previous!=self._user_execution_policy(db,user_id):
                affected.update(row[0] for row in db.execute('SELECT DISTINCT n.server_id FROM nodes n JOIN node_users u ON n.id=u.node_id WHERE u.user_id=?',(user_id,)))
        for server_id in affected: self._bump(db,server_id)

    def _bandwidth_policy(self, db, user_id):
        self._expire_bandwidth(db)
        if db.execute('SELECT 1 FROM users WHERE id=?',(user_id,)).fetchone() is None:
            raise Error('用户不存在',404)
        plan=db.execute('SELECT plan_id FROM user_plans WHERE user_id=?',(user_id,)).fetchone()
        override=db.execute('SELECT payload FROM user_bandwidth WHERE user_id=?',(user_id,)).fetchone()
        temporary=db.execute('SELECT payload,expires_at FROM temporary_bandwidth WHERE user_id=?',(user_id,)).fetchone()
        result={'source':'temporary' if temporary else ('user' if override else ('plan' if plan else 'default')),
                'plan_id':plan['plan_id'] if plan else None,
                'override':json.loads(override['payload']) if override else None,
                'effective':self._effective_bandwidth(db,user_id) or {'upload_bps':None,'download_bps':None}}
        if temporary: result['temporary']={'bandwidth':json.loads(temporary['payload']),'expires_at':temporary['expires_at']}
        return result

    def bandwidth_policy(self, user_id, *, db=None):
        with self._resource_transaction(db,write=True) as db:
            return self._bandwidth_policy(db,user_id)

    def user_bandwidth(self, user_id, data=None, *, inherit=False, db=None):
        if data is not None:
            try: data=bandwidth(data)
            except ValueError as error: raise Error(str(error)) from None
        with self._resource_transaction(db,write=True) as db:
            self._bandwidth_policy(db,user_id)
            if data is None and not inherit:
                return self._effective_bandwidth(db,user_id) or {'upload_bps':None,'download_bps':None}
            before={user_id:self._user_execution_policy(db,user_id)}
            if inherit:
                db.execute('DELETE FROM user_bandwidth WHERE user_id=?',(user_id,))
            else:
                encoded=json.dumps(data,sort_keys=True)
                db.execute('INSERT INTO user_bandwidth(user_id,payload) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET payload=excluded.payload',(user_id,encoded))
            self._bump_changed_users(db,before)
            return self._effective_bandwidth(db,user_id) or {'upload_bps':None,'download_bps':None}

    @staticmethod
    def _effective_connections(db, user_id):
        from plan_policy import connections
        return connections(db,user_id)

    def _connections_policy(self, db, user_id):
        if db.execute('SELECT 1 FROM users WHERE id=?',(user_id,)).fetchone() is None:
            raise Error('用户不存在',404)
        plan=db.execute('SELECT plan_id FROM user_plans WHERE user_id=?',(user_id,)).fetchone()
        override=db.execute('SELECT payload FROM user_connections WHERE user_id=?',(user_id,)).fetchone()
        inherited=plan and db.execute('SELECT 1 FROM plan_connections WHERE plan_id=?',(plan['plan_id'],)).fetchone()
        return {'source':'user' if override else ('plan' if inherited else 'default'),
                'plan_id':plan['plan_id'] if plan else None,
                'override':json.loads(override['payload']) if override else None,
                'effective':self._effective_connections(db,user_id)}

    def connections_policy(self, user_id, *, db=None):
        if db is None:
            with self.connect() as connection:
                return self._connections_policy(connection,user_id)
        with self._resource_transaction(db) as connection:
            return self._connections_policy(connection,user_id)

    def user_connections(self, user_id, data=None, *, inherit=False, db=None):
        if data is not None:
            try: data=connections(data)
            except ValueError as error: raise Error(str(error)) from None
        with self._resource_transaction(db,write=True) as db:
            self._connections_policy(db,user_id)
            if data is None and not inherit: return self._effective_connections(db,user_id)
            before={user_id:self._user_execution_policy(db,user_id)}
            if inherit:
                db.execute('DELETE FROM user_connections WHERE user_id=?',(user_id,))
            else:
                db.execute('INSERT INTO user_connections(user_id,payload) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET payload=excluded.payload',
                           (user_id,json.dumps(data,sort_keys=True)))
            self._bump_changed_users(db,before)
            return self._effective_connections(db,user_id)

    def temporary_bandwidth(self, user_id, data=None, *, remove=False, db=None):
        limits=None
        if data is not None:
            if not isinstance(data,dict) or set(data)!={'bandwidth','expires_at'}:
                raise Error('临时规则需要 bandwidth 和 expires_at')
            try: limits=bandwidth(data['bandwidth'])
            except ValueError as error: raise Error(str(error)) from None
        with self._resource_transaction(db,write=True) as db:
            self._bandwidth_policy(db,user_id)
            if data is not None:
                expires=data['expires_at']
                now=time.time()
                if type(expires) not in (int,float) or not math.isfinite(expires) or not now<expires<=now+30*86400:
                    raise Error('到期时间应为未来 30 天内的 Unix 秒时间戳')
            before={user_id:self._user_execution_policy(db,user_id)}
            if remove:
                db.execute('DELETE FROM temporary_bandwidth WHERE user_id=?',(user_id,))
            elif data is not None:
                db.execute('INSERT INTO temporary_bandwidth(user_id,payload,expires_at) VALUES(?,?,?) ON CONFLICT(user_id) DO UPDATE SET payload=excluded.payload,expires_at=excluded.expires_at',
                           (user_id,json.dumps(limits,sort_keys=True),expires))
            self._bump_changed_users(db,before)
            row=db.execute('SELECT payload,expires_at FROM temporary_bandwidth WHERE user_id=?',(user_id,)).fetchone()
            return {'bandwidth':json.loads(row['payload']),'expires_at':row['expires_at']} if row else None

    def plans(self, *, versioned=False, db=None):
        from plan_admin import response
        with self._resource_transaction(db) as db:
            result=[self._plan_result(db,row) for row in db.execute('SELECT * FROM plans ORDER BY id')]
            return [response(plan) for plan in result] if versioned else result

    @staticmethod
    def _plan_result(db, row):
        result={'id':row['id'],'name':row['name'],'bandwidth':json.loads(row['bandwidth'])}
        policy=db.execute('SELECT payload FROM plan_connections WHERE plan_id=?',(row['id'],)).fetchone()
        if policy: result['connections']=json.loads(policy['payload'])
        return result

    def save_plan(self, data, plan_id=None, *, if_match=None, versioned=False, db=None):
        from plan_admin import validate,version,response
        validate(if_match)
        if plan_id is None and if_match is not None: raise Error('创建套餐不接受编辑版本')
        if not isinstance(data,dict) or set(data) not in ({'name','bandwidth'},{'name','bandwidth','connections'}):
            raise Error('套餐需要 name 和 bandwidth')
        connection_limits=None
        if 'connections' in data:
            try: connection_limits=connections(data['connections'])
            except ValueError as error: raise Error(str(error)) from None
        name=self.fields('users',{'name':data['name']})['name']
        try: limits=bandwidth(data['bandwidth'])
        except ValueError as error: raise Error(str(error)) from None
        with self._resource_transaction(db,write=True) as db:
            self._expire_bandwidth(db)
            encoded=json.dumps(limits,sort_keys=True)
            before={}
            if plan_id is None:
                plan_id=db.execute('INSERT INTO plans(name,bandwidth) VALUES(?,?)',(name,encoded)).lastrowid
            else:
                if db.execute('SELECT 1 FROM plans WHERE id=?',(plan_id,)).fetchone() is None:
                    raise Error('套餐不存在',404)
                if if_match is not None:
                    current=self._plan_result(db,db.execute('SELECT * FROM plans WHERE id=?',(plan_id,)).fetchone())
                    if version(current)!=if_match: raise Error('套餐配置已变化，请重新读取后再编辑',412)
                before={row[0]:self._user_execution_policy(db,row[0]) for row in db.execute('SELECT user_id FROM user_plans WHERE plan_id=?',(plan_id,))}
                db.execute('UPDATE plans SET name=?,bandwidth=? WHERE id=?',(name,encoded,plan_id))
            if connection_limits is not None:
                db.execute('INSERT INTO plan_connections(plan_id,payload) VALUES(?,?) ON CONFLICT(plan_id) DO UPDATE SET payload=excluded.payload',(plan_id,json.dumps(connection_limits,sort_keys=True)))
            self._bump_changed_users(db,before)
            result=self._plan_result(db,db.execute('SELECT * FROM plans WHERE id=?',(plan_id,)).fetchone())
            return response(result) if versioned else result

    def delete_plan(self, plan_id, *, if_match=None, db=None):
        from plan_admin import validate,version
        validate(if_match)
        with self._resource_transaction(db,write=True) as db:
            row=db.execute('SELECT * FROM plans WHERE id=?',(plan_id,)).fetchone()
            if row is None: raise Error('套餐不存在',404)
            if if_match is not None and version(self._plan_result(db,row))!=if_match:
                raise Error('套餐配置已变化，请重新读取后再确认删除',412)
            if db.execute('SELECT 1 FROM plan_grace_policies WHERE target_plan_id=?',(plan_id,)).fetchone():
                raise Error('套餐仍被用作到期降级目标，请先修改降级规则',409)
            if db.execute('SELECT 1 FROM user_plans WHERE plan_id=?',(plan_id,)).fetchone():
                raise Error('套餐仍绑定用户，请先迁移或解绑',409)
            if not db.execute('DELETE FROM plans WHERE id=?',(plan_id,)).rowcount:
                raise Error('套餐不存在',404)

    def assign_plan(self, user_id, data, *, db=None):
        if not isinstance(data,dict) or set(data)!={'plan_id'}:
            raise Error('需要 plan_id，null 表示解绑')
        plan_id=data['plan_id']
        if plan_id is not None and (type(plan_id) is not int or plan_id<1):
            raise Error('套餐编号无效')
        with self._resource_transaction(db, write=True) as db:
            self._bandwidth_policy(db,user_id)
            if plan_id is not None and db.execute('SELECT 1 FROM plans WHERE id=?',(plan_id,)).fetchone() is None:
                raise Error('套餐不存在',404)
            account=db.execute('SELECT plan_id FROM plan_accounts WHERE user_id=?',(user_id,)).fetchone()
            if account and account['plan_id']!=plan_id:
                raise Error('套餐子账号的套餐不能单独更换或解绑，请移除后重建',409)
            if plan_id is not None and db.execute('SELECT 1 FROM plan_accounts WHERE parent_id=? AND plan_id=?',(user_id,plan_id)).fetchone():
                raise Error('该用户已通过子账号持有此套餐',409)
            from plan_nodes import check
            check(db,plan_id,[r[0] for r in db.execute('SELECT node_id FROM node_users WHERE user_id=?',(user_id,))])
            before={user_id:self._user_execution_policy(db,user_id)}
            old_plan=db.execute('SELECT plan_id FROM user_plans WHERE user_id=?',(user_id,)).fetchone()
            if old_plan is None or old_plan['plan_id']!=plan_id:
                from plan_renewals import invalidate
                invalidate(db,user_id)
                db.execute('DELETE FROM plan_lifecycle WHERE user_id=?',(user_id,))
            if plan_id is None:
                db.execute('DELETE FROM user_plans WHERE user_id=?',(user_id,))
            else:
                db.execute('INSERT INTO user_plans(user_id,plan_id) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET plan_id=excluded.plan_id',(user_id,plan_id))
            self._bump_changed_users(db,before)
            return self._bandwidth_policy(db,user_id)

    def server_bandwidth(self, server_id, data=None, *, db=None):
        if data is not None:
            try: data=bandwidth(data)
            except ValueError as error: raise Error(str(error)) from None
        with self._resource_transaction(db,write=data is not None) as db:
            if db.execute('SELECT 1 FROM servers WHERE id=?',(server_id,)).fetchone() is None:
                raise Error('服务器不存在',404)
            old=db.execute('SELECT payload FROM server_bandwidth WHERE server_id=?',(server_id,)).fetchone()
            if data is None:
                return json.loads(old['payload']) if old else {'upload_bps':None,'download_bps':None}
            encoded=json.dumps(data,sort_keys=True)
            if old is None or old['payload']!=encoded:
                db.execute('INSERT INTO server_bandwidth(server_id,payload) VALUES(?,?) ON CONFLICT(server_id) DO UPDATE SET payload=excluded.payload',(server_id,encoded))
                self._bump(db,server_id)
            return data

    def _client_policy(self, db, client, node_id, enabled):
        from plan_lifecycle import view as lifecycle_view
        connection=self._effective_connections(db,client['user_id'])
        if any(value is not None for value in connection.values()): client['connections']=connection
        limits=self._effective_bandwidth(db,client['user_id'])
        if limits is not None: client['bandwidth']=limits
        from plan_node_bandwidth import override_for_user
        node_limits=override_for_user(db,client['user_id'],node_id)
        if node_limits is not None: client['node_bandwidth']=node_limits
        from plan_node_connections import override_for_user as connection_override
        node_connections=connection_override(db,client['user_id'],node_id)
        if node_connections is not None: client['node_connections']=node_connections
        auto=self._automatic_policy(db,client['user_id'])
        if auto['enabled']: client['automatic_bandwidth']=auto
        temporary=db.execute('SELECT expires_at FROM temporary_bandwidth WHERE user_id=?',(client['user_id'],)).fetchone()
        shared=db.execute('SELECT g.expires_at FROM shared_users u JOIN server_shares g ON g.id=u.share_id WHERE u.user_id=?',(client['user_id'],)).fetchone()
        validity=lifecycle_view(db,client['user_id'])
        expiry=[item['expires_at'] for item in (temporary,shared,validity) if item and item['expires_at'] is not None]
        if expiry and enabled:
            client['valid_until']=min(expiry)


    def _snapshot(self, db, server_id):
        from sharing import Shares
        Shares.expire(db)
        from plan_lifecycle import expire as expire_plans
        expire_plans(self,db)
        self._expire_bandwidth(db)
        deadlines=[]
        row=db.execute('SELECT s.name,c.revision FROM servers s JOIN server_configs c ON c.server_id=s.id WHERE s.id=?',(server_id,)).fetchone()
        nodes=[]
        for item in db.execute('SELECT n.id,n.name,c.payload FROM nodes n JOIN node_configs c ON c.node_id=n.id WHERE n.server_id=? ORDER BY n.id',(server_id,)):
            node={'id':item['id'],'name':item['name'],'config':json.loads(item['payload'])}
            if node['config'].get('protocol')=='vless':
                node['clients']=[{'user_id':user['user_id'],'uuid':user['credential']} for user in db.execute('SELECT user_id,credential FROM node_users WHERE node_id=? ORDER BY user_id',(item['id'],))]
                from plan_lifecycle import view as lifecycle_view
                node['clients']=[c for c in node['clients'] if lifecycle_view(db,c['user_id'])['status']=='active']
                from usage_cycles import boundary
                for client in node['clients']:
                    cycle=boundary(db,client['user_id'])
                    if cycle is not None and cycle[0] is not None:
                        client['cycle_id']=cycle[0]
                        from usage_migration import draining
                        if draining(db,cycle[0]):
                            client['_withheld']=True
                            continue
                        budget=db.execute('SELECT upload_bytes,download_bytes FROM usage_cycle_budgets WHERE cycle_id=?',(cycle[0],)).fetchone()
                        if budget is not None:
                            from total_usage import directions
                            used=directions(db,cycle[0])
                            if used[2] and any(budget[k] is not None for k in ('upload_bytes','download_bytes')):
                                # Missing historical direction is not free
                                # balance or fabricated per-direction usage.
                                client['_withheld']=True
                                continue
                            client['cycle_budget']={'upload_bytes':budget['upload_bytes'],'download_bytes':budget['download_bytes']}
                            client['cycle_usage']={'upload_bytes':used[0],'download_bytes':used[1]}
                node['clients']=[c for c in node['clients'] if not c.pop('_withheld',False)]
                for client in node['clients']:
                    self._client_policy(db,client,item['id'],node['config']['enabled'])
                    if 'valid_until' in client: deadlines.append(client['valid_until'])
            nodes.append(node)
        content={'schema':XRAY_SCHEMA if any('clients' in node for node in nodes) else SCHEMA,
                 'server_id':server_id,'server_name':row['name'],'revision':row['revision'],'nodes':nodes}
        limits=db.execute('SELECT payload FROM server_bandwidth WHERE server_id=?',(server_id,)).fetchone()
        if limits: content['bandwidth']=json.loads(limits['payload'])
        if deadlines: content['valid_until']=min(deadlines)
        return {**content,'digest':digest(content)}

    def configuration(self, token):
        from domain_failover import Failover
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            server_id=self._agent_id(db,token)
            Failover(self).tick(db,server_id)
            return self._snapshot(db,server_id)

    def acknowledge_configuration(self, token, data):
        if not isinstance(data,dict) or set(data) not in ({'revision','digest'},{'revision','digest','session_id'}) or type(data['revision']) is not int:
            raise Error('配置回执无效')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            server_id=self._agent_id(db,token)
            if 'session_id' in data: self._session_value(data['session_id'])
            self._check_session(db,server_id,data.get('session_id'))
            desired=self._snapshot(db,server_id)
            if data['revision']!=desired['revision'] or data['digest']!=desired['digest']:
                raise Error('配置已变化，请重新获取后回报',409)
            db.execute('UPDATE server_configs SET staged_revision=?,staged_digest=?,staged_at=? WHERE server_id=?',(data['revision'],data['digest'],time.time(),server_id))
            return {'accepted':True,'state':'staged','revision':data['revision']}

    def acknowledge_runtime(self, token, data):
        if not isinstance(data,dict) or set(data) not in ({'revision','digest'},{'revision','digest','session_id'}) or type(data['revision']) is not int:
            raise Error('执行回执无效')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            server_id=self._agent_id(db,token)
            if 'session_id' in data: self._session_value(data['session_id'])
            self._check_session(db,server_id,data.get('session_id'))
            desired=self._snapshot(db,server_id)
            staged=db.execute('SELECT staged_revision,staged_digest FROM server_configs WHERE server_id=?',(server_id,)).fetchone()
            if data['revision']!=desired['revision'] or data['digest']!=desired['digest'] or data['revision']!=staged['staged_revision'] or data['digest']!=staged['staged_digest']:
                raise Error('配置尚未暂存或已变化，请重新同步',409)
            db.execute('INSERT INTO runtime_receipts(server_id,revision,digest,reported_at) VALUES(?,?,?,?) ON CONFLICT(server_id) DO UPDATE SET revision=excluded.revision,digest=excluded.digest,reported_at=excluded.reported_at',
                       (server_id,data['revision'],data['digest'],time.time()))
            from usage_settlement import record_execution
            record_execution(db,desired,data.get('session_id'))
            return {'accepted':True,'state':'listening','revision':data['revision']}

    def agent_status(self):
        from agent_execution_status import aggregate
        with self.connect() as db:
            db.execute('BEGIN')
            now=time.time()
            rows=[]
            for row in db.execute('SELECT s.id AS server_id,s.name,a.last_seen,a.uptime,a.hostname,c.revision AS desired_revision,c.staged_revision,c.staged_at,r.revision AS applied_revision,r.reported_at AS applied_at FROM servers s LEFT JOIN agents a ON a.server_id=s.id LEFT JOIN server_configs c ON c.server_id=s.id LEFT JOIN runtime_receipts r ON r.server_id=s.id ORDER BY s.id'):
                online=(row['last_seen'] is not None
                        and 0 <= now-row['last_seen'] < ONLINE_WINDOW)
                listeners=(online
                    and row['applied_revision']==row['desired_revision']
                    and row['applied_at'] is not None
                    and 0 <= now-row['applied_at'] < ONLINE_WINDOW)
                agent=db.execute('SELECT * FROM agents WHERE server_id=?',
                                 (row['server_id'],)).fetchone()
                rows.append(dict(
                    row, online=online, listeners_current=listeners,
                    grant_execution=aggregate(
                        db, row['server_id'], agent, online, now)))
            return rows
