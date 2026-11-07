"""Internal attributed ledger; no automatic Agent usage delivery yet.

An undelivered intent never authorizes metering. The applied table is reserved
for authenticated installation ACKs. Isolated ledger tests also seed it as a
fixture, not as proof of a real process executing this protocol.
"""
import json
import time
from core import Error
from usage_ledger import integer
from total_usage import directions

SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_first_total_applied(
 grant_id TEXT PRIMARY KEY REFERENCES usage_first_total_grants(grant_id),
 config_version INTEGER NOT NULL CHECK(config_version>0),
 config_digest TEXT NOT NULL,valid_until REAL NOT NULL);
CREATE TABLE IF NOT EXISTS usage_first_total_meter(
 grant_id TEXT NOT NULL REFERENCES usage_first_total_grants(grant_id),
 sequence INTEGER NOT NULL CHECK(sequence>0),
 upload_bytes INTEGER NOT NULL CHECK(upload_bytes>=0),
 download_bytes INTEGER NOT NULL CHECK(download_bytes>=0),received_at REAL NOT NULL,
 PRIMARY KEY(grant_id,sequence));
'''

def accounted(db,grant_id):
    row=db.execute('SELECT upload_bytes,download_bytes FROM usage_first_total_meter WHERE grant_id=? ORDER BY sequence DESC LIMIT 1',(grant_id,)).fetchone()
    return tuple(row) if row else (0,0)

def outstanding(db,cycle_id):
    from usage_accounting import FIRST,outstanding as source_outstanding
    return source_outstanding(db,cycle_id,FIRST)


def _execution(db,g,applied):
    configuration=db.execute('SELECT * FROM usage_first_total_configurations WHERE grant_id=?',(g['grant_id'],)).fetchone()
    installation=db.execute('SELECT fact FROM usage_first_total_installations WHERE grant_id=?',(g['grant_id'],)).fetchone()
    lifecycle=db.execute('SELECT * FROM usage_first_total_lifecycle WHERE grant_id=?',(g['grant_id'],)).fetchone()
    execution=db.execute('SELECT fact FROM usage_first_total_execution WHERE grant_id=?',(g['grant_id'],)).fetchone()
    if any(row is None for row in (configuration,installation,lifecycle,execution)):
        raise Error('授予缺少完整持久执行证据',409)
    try: fact=json.loads(execution['fact'])
    except (TypeError,ValueError): raise Error('授予执行证据损坏',409) from None
    from grant_execution import GrantExecution
    if GrantExecution._validate_fact(fact)!=execution['fact']:
        raise Error('授予执行证据不是规范正文',409)
    expected={key:value for key,value in fact.items() if key!='execution_id'}
    if (fact.get('grant_id')!=g['grant_id'] or fact.get('server_id')!=g['server_id']
            or fact.get('session_id')!=g['session_id'] or configuration['digest']!=fact.get('digest')
            or (applied['config_version'],applied['config_digest'],applied['valid_until'])
                !=(fact.get('revision'),fact.get('digest'),fact.get('valid_until'))
            or installation['fact']!=json.dumps(expected,sort_keys=True,separators=(',',':'))
            or (lifecycle['server_id'],lifecycle['session_id'],lifecycle['configuration_digest'])
                !=(g['server_id'],g['session_id'],configuration['digest'])
            or lifecycle['state'] not in ('installed','closed')):
        raise Error('授予执行证据与安装链不一致',409)
    return fact,lifecycle['state']

class GrantMeter:
    def __init__(self,registry): self.registry=registry

    def report(self,token,data):
        fields={'grant_id','policy_id','session_id','sequence','upload_bytes','download_bytes'}
        if not isinstance(data,dict) or set(data)!=fields: raise Error('授予计量字段无效')
        r=self.registry
        for k in ('grant_id','session_id'): r._session_value(data[k])
        for k in ('policy_id','sequence','upload_bytes','download_bytes'):
            if not integer(data[k],1 if k in ('policy_id','sequence') else 0): raise Error('授予计量需要有效整数字节')
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            sid=r._agent_id(db,token); r._check_session(db,sid,data['session_id'])
            g=db.execute('SELECT * FROM usage_first_total_grants WHERE grant_id=?',(data['grant_id'],)).fetchone()
            fingerprint=db.execute('SELECT token_hash FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
            if g is None or g['server_id']!=sid or g['token_hash']!=fingerprint or g['session_id']!=data['session_id']:
                raise Error('授予不属于此Agent会话',403)
            if g['policy_id']!=data['policy_id']: raise Error('授予策略版本不匹配',409)
            applied=db.execute('SELECT * FROM usage_first_total_applied WHERE grant_id=?',(g['grant_id'],)).fetchone()
            if applied is None: raise Error('授予尚未确认执行',409)
            fact,lifecycle_state=_execution(db,g,applied)
            digest=applied['config_digest']
            if len(digest)!=64 or any(c not in '0123456789abcdef' for c in digest) or not g['issued_at']<applied['valid_until']<=g['valid_until']:
                raise Error('授予执行记录无效',409)
            old=db.execute('SELECT * FROM usage_first_total_meter WHERE grant_id=? AND sequence=?',(g['grant_id'],data['sequence'])).fetchone()
            up,down=accounted(db,g['grant_id'])
            from usage_accounting import FIRST,require_source
            source=require_source(db,FIRST,g)
            if (int(source['upload_bytes']),int(source['download_bytes']))!=(up,down):
                raise Error('授予计量与归属来源不一致',409)
            if old:
                if any(old[k]!=data[k] for k in ('upload_bytes','download_bytes')): raise Error('授予回执编号冲突',409)
            else:
                if lifecycle_state!='installed': raise Error('授予生命周期已关闭',409)
                # A backwards wall-clock adjustment must not admit new usage
                # before this grant existed. Exact accepted retries above add
                # no usage and remain recoverable across clock adjustments.
                if not g['issued_at']<=time.time()<applied['valid_until']:
                    raise Error('授予计量窗口已结束或时钟异常',409)
                last=db.execute('SELECT max(sequence) FROM usage_first_total_meter WHERE grant_id=?',(g['grant_id'],)).fetchone()[0]
                if (last is not None and data['sequence']<=last) or data['upload_bytes']<up or data['download_bytes']<down:
                    raise Error('授予累计计数不可回退',409)
                if data['upload_bytes']+data['download_bytes']>g['reserved_bytes']: raise Error('授予双向累计超限',409)
                if directions(db,g['cycle_id'])[2]: raise Error('历史流量方向未确认',409)
                now=time.time()
                from usage_accounting import accept_first
                accept_first(db,g,data['sequence'],data['upload_bytes'],data['download_bytes'],fact['execution_id'],now)
                db.execute('INSERT INTO usage_first_total_meter VALUES(?,?,?,?,?)',(g['grant_id'],data['sequence'],data['upload_bytes'],data['download_bytes'],now))
                up,down=data['upload_bytes'],data['download_bytes']
            return dict(accepted=True,duplicate=old is not None,grant_id=g['grant_id'],policy_id=g['policy_id'],sequence=data['sequence'],
                        confirmed_bytes=up+down,held_bytes=g['reserved_bytes']-up-down,ready_for_execution=False)
