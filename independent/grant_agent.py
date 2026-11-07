"""Agent-side durable staging only; no listener installation or execution ACK.

The authenticated transport supplies the expected server/session.
A staged receipt must never be accepted as a runtime-applied receipt. Storage
keeps old reservations across expiry/restart; staging cannot mint more bytes.
"""
from contextlib import closing
from pathlib import Path
import json
import copy
import math
import sqlite3
import time
import uuid
from node_config import digest, validate_snapshot, XRAY_SCHEMA

SCHEMA='independent.total-grant.binding.v1'
FIELDS={'schema','grant_id','server_id','session_id','cycle_id','policy_id','plan_id',
        'revision','configuration_digest','node_id','node_config','client',
        'reserved_bytes','baseline_bytes','issued_at','valid_until'}
MAX_BYTES=2**63-1
FIRST='first_grant'
MULTI='multi_allocation'
LEDGER_KINDS=frozenset((FIRST,MULTI))
IDENTITY_FIELDS={'accepted','state','ledger_kind','ledger_id','binding_digest',
                 'ready_for_execution'}


def identifier(value):
    try: valid=type(value) is str and str(uuid.UUID(value))==value
    except (ValueError,TypeError,AttributeError): valid=False
    if not valid: raise ValueError('授予身份需要规范UUID')


def checksum(value):
    if type(value) is not str or len(value)!=64 or any(c not in '0123456789abcdef' for c in value):
        raise ValueError('授予摘要无效')


def validate(value,server_id,session_id,now=None):
    if not isinstance(value,dict) or not FIELDS<=set(value)<=FIELDS|{'server_bandwidth','digest','delivery_state','ready_for_execution'}:
        raise ValueError('授予配置字段无效')
    if value['schema']!=SCHEMA: raise ValueError('不支持此授予协议')
    if value.get('delivery_state','not_delivered')!='not_delivered' or value.get('ready_for_execution',False) is not False:
        raise ValueError('不能把执行回执当授予配置')
    for key in ('grant_id','session_id','cycle_id'): identifier(value[key])
    for key in ('server_id','policy_id','plan_id','revision','node_id'):
        if type(value[key]) is not int or not 1<=value[key]<=MAX_BYTES: raise ValueError('授予编号无效')
    if type(server_id) is not int or value['server_id']!=server_id or value['session_id']!=session_id:
        raise ValueError('授予不属于当前Agent会话')
    for key in ('reserved_bytes','baseline_bytes'):
        if type(value[key]) is not int or value[key]<(1 if key=='reserved_bytes' else 0) or (key=='reserved_bytes' and value[key]>MAX_BYTES):
            raise ValueError('授予字节范围无效')
    for key in ('issued_at','valid_until'):
        if type(value[key]) not in (int,float) or not math.isfinite(value[key]) or value[key]<=0:
            raise ValueError('授予期限无效')
    now=time.time() if now is None else now
    if type(now) not in (int,float) or not math.isfinite(now) or not value['issued_at']<=now<value['valid_until']:
        raise ValueError('授予窗口不可用')
    checksum(value['configuration_digest']); checksum(value.get('digest'))
    content={key:value[key] for key in FIELDS}
    if 'server_bandwidth' in value: content['server_bandwidth']=value['server_bandwidth']
    if digest(content)!=value['digest']: raise ValueError('授予配置摘要不匹配')
    client=value['client']
    if not isinstance(client,dict): raise ValueError('授予用户无效')
    snapshot={'schema':XRAY_SCHEMA,'server_id':server_id,'server_name':'grant-staging',
              'revision':value['revision'],'nodes':[{'id':value['node_id'],'name':'grant',
              'config':value['node_config'],'clients':[client]}]}
    if 'server_bandwidth' in value: snapshot['bandwidth']=value['server_bandwidth']
    if 'valid_until' in client: snapshot['valid_until']=client['valid_until']
    snapshot['digest']=digest(snapshot)
    validate_snapshot(snapshot)
    if 'cycle_id' in client and client['cycle_id']!=value['cycle_id']: raise ValueError('授予周期不匹配')
    if 'cycle_usage' in client and sum(client['cycle_usage'].values())!=value['baseline_bytes']:
        raise ValueError('授予方向基线不匹配')
    if 'valid_until' in client and client['valid_until']<value['valid_until']:
        raise ValueError('授予超过用户期限')
    if value['node_config'].get('protocol')!='vless' or value['node_config']['enabled'] is not True:
        raise ValueError('授予需要可用VLESS节点')
    return content


class GrantStageStore:
    def __init__(self,path,server_id,session_id):
        if type(server_id) is not int or not 1<=server_id<=MAX_BYTES: raise ValueError('Agent编号无效')
        identifier(session_id)
        self.path=Path(path); self.server_id=server_id; self.session_id=session_id

    @staticmethod
    def _columns(db,table):
        return tuple(row[1] for row in db.execute(f'PRAGMA table_info({table})'))

    @staticmethod
    def _create_v2(db,name='staged_grants'):
        db.execute(f'''CREATE TABLE {name}(
            ledger_kind TEXT NOT NULL CHECK(ledger_kind IN ('first_grant','multi_allocation')),
            grant_id TEXT NOT NULL,cycle_id TEXT NOT NULL,
            payload TEXT NOT NULL,digest TEXT NOT NULL,
            PRIMARY KEY(ledger_kind,grant_id))''')

    @staticmethod
    def _create_quarantine(db,name='quarantined_grants'):
        db.execute(f'''CREATE TABLE {name}(
            grant_id TEXT PRIMARY KEY,cycle_id TEXT NOT NULL,
            payload TEXT NOT NULL,digest TEXT NOT NULL,reason TEXT NOT NULL)''')

    def _owner(self,db):
        db.execute('CREATE TABLE IF NOT EXISTS grant_owner(server_id INTEGER NOT NULL)')
        owner=db.execute('SELECT server_id FROM grant_owner').fetchall()
        if not owner: db.execute('INSERT INTO grant_owner VALUES(?)',(self.server_id,))
        elif owner!=[(self.server_id,)]: raise ValueError('授予存储Agent身份变化')

    @staticmethod
    def _identity(value,content):
        if (type(value) is not dict or set(value)!=IDENTITY_FIELDS or
                value.get('accepted') is not True or
                value.get('state')!='ledger_resolved' or
                value.get('ready_for_execution') is not False or
                type(value.get('ledger_kind')) is not str or
                value.get('ledger_kind') not in LEDGER_KINDS or
                value.get('ledger_id')!=content['grant_id'] or
                value.get('binding_digest') is None):
            raise ValueError('授予账本身份回执无效')
        checksum(value['binding_digest'])
        return value['ledger_kind']

    def migrate(self,resolver=None):
        """Atomically upgrade an untyped stage table after out-of-lock resolution."""
        self.path.parent.mkdir(parents=True,exist_ok=True)
        with closing(sqlite3.connect(self.path,timeout=0.05)) as reader:
            columns=self._columns(reader,'staged_grants')
            if columns and 'ledger_kind' in columns:
                return
            legacy=reader.execute('''SELECT grant_id,cycle_id,payload,digest
                FROM staged_grants ORDER BY grant_id''').fetchall() if columns else []

        decisions=[]
        for row in legacy:
            identity=None
            if resolver is not None:
                try: identity=resolver(row[0],row[3])
                except Exception: identity=None
            try:
                kind=self._identity(identity,{'grant_id':row[0]}) if identity is not None else None
                if identity is not None and identity['binding_digest']!=row[3]: kind=None
            except (ValueError,TypeError): kind=None
            decisions.append((row,kind))

        with closing(sqlite3.connect(self.path,timeout=0.05)) as db:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('BEGIN IMMEDIATE')
            current=self._columns(db,'staged_grants')
            if current and 'ledger_kind' in current:
                db.rollback(); return
            if current!=columns:
                raise ValueError('授予暂存迁移期间结构已变化')
            check=db.execute('''SELECT grant_id,cycle_id,payload,digest
                FROM staged_grants ORDER BY grant_id''').fetchall() if current else []
            if check!=legacy:
                raise ValueError('授予暂存迁移期间数据已变化')
            self._owner(db)
            if not current:
                self._create_v2(db)
                self._create_quarantine(db)
            else:
                self._create_v2(db,'staged_grants_v2')
                self._create_quarantine(db,'quarantined_grants_v2')
                for row,kind in decisions:
                    if kind is None:
                        db.execute('INSERT INTO quarantined_grants_v2 VALUES(?,?,?,?,?)',
                                   (*row,'unresolved'))
                    else:
                        db.execute('INSERT INTO staged_grants_v2 VALUES(?,?,?,?,?)',
                                   (kind,*row))
                active=db.execute('''SELECT grant_id,cycle_id,payload,digest
                    FROM staged_grants_v2''').fetchall()
                quarantined=db.execute('''SELECT grant_id,cycle_id,payload,digest
                    FROM quarantined_grants_v2''').fetchall()
                if len(active)+len(quarantined)!=len(legacy) or sorted(active+quarantined)!=sorted(legacy):
                    raise ValueError('授予暂存迁移校验失败')
                db.execute('ALTER TABLE staged_grants RENAME TO staged_grants_v1')
                db.execute('ALTER TABLE staged_grants_v2 RENAME TO staged_grants')
                db.execute('ALTER TABLE quarantined_grants_v2 RENAME TO quarantined_grants')
                db.execute('DROP TABLE staged_grants_v1')
            db.execute('''CREATE INDEX IF NOT EXISTS staged_grants_cycle
                ON staged_grants(cycle_id)''')
            db.execute('''CREATE TABLE IF NOT EXISTS grant_stage_schema(
                version INTEGER PRIMARY KEY CHECK(version=2))''')
            marker=db.execute('SELECT version FROM grant_stage_schema').fetchall()
            if not marker: db.execute('INSERT INTO grant_stage_schema VALUES(2)')
            elif marker!=[(2,)]: raise ValueError('授予暂存结构版本无效')
            db.commit()

    def stage(self,value,identity=None):
        value=copy.deepcopy(value)
        content=validate(value,self.server_id,self.session_id)
        raw=json.dumps(content,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
        self.migrate()
        with closing(sqlite3.connect(self.path,timeout=0.05)) as db,db:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('BEGIN IMMEDIATE')
            self._owner(db)
            # Recheck after lock acquisition: validation must not spend the lease
            # waiting for a writer and then persist a now-expired grant.
            validate(value,self.server_id,self.session_id)
            if identity is None:
                matches=db.execute('''SELECT ledger_kind,cycle_id,payload,digest
                    FROM staged_grants WHERE grant_id=?''',(content['grant_id'],)).fetchall()
                if len(matches)!=1 or matches[0][1:]!=(content['cycle_id'],raw,value['digest']):
                    raise ValueError('新授予暂存需要精确账本身份')
                kind=matches[0][0]
            else:
                kind=self._identity(identity,content)
                if identity['binding_digest']!=value['digest']:
                    raise ValueError('授予账本摘要不匹配')
            other=db.execute('''SELECT 1 FROM staged_grants
                WHERE grant_id=? AND ledger_kind<>?''',(content['grant_id'],kind)).fetchone()
            if other is not None: raise ValueError('授予账本编号冲突')
            old=db.execute('''SELECT cycle_id,payload,digest FROM staged_grants
                WHERE ledger_kind=? AND grant_id=?''',(kind,content['grant_id'])).fetchone()
            if old:
                if old!=(content['cycle_id'],raw,value['digest']): raise ValueError('不能覆盖原授予暂存')
            else:
                db.execute('INSERT INTO staged_grants VALUES(?,?,?,?,?)',
                           (kind,content['grant_id'],content['cycle_id'],raw,value['digest']))
        return {'ledger_kind':kind,'grant_id':content['grant_id'],'digest':value['digest'],
                'state':'staged','ready_for_execution':False}
