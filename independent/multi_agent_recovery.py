"""E3 fixture-only administrator recovery; never a production authorization API."""
from contextlib import closing
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import sqlite3
import time
import uuid

from core import Error
from node_config import digest
from grant_agent import identifier
from multi_agent_lifecycle import gated,encoded,KIND
from multi_agent_tail import sealed,unpack

READ='tail.audit.read'
PROPOSE='tail.recovery.propose'
COMMIT='tail.recovery.commit'
CAPTURE='tail.evidence.capture'
OBSERVE='tail.evidence.observe'
PERMISSIONS={READ,PROPOSE,COMMIT,CAPTURE,OBSERVE}
SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_multi_recovery_admins(
 actor_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,token_hash TEXT NOT NULL UNIQUE,
 active INTEGER NOT NULL CHECK(active IN (0,1)),permissions TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS usage_multi_fixture_instances(
 allocation_id TEXT PRIMARY KEY REFERENCES usage_multi_tail_authorities(allocation_id),
 execution_id TEXT NOT NULL,authority_digest TEXT NOT NULL,host_id TEXT NOT NULL,boot_id TEXT NOT NULL,
 instance_id TEXT NOT NULL,start_id TEXT NOT NULL,pid INTEGER NOT NULL,started_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS usage_multi_recovery_commitments(
 allocation_id TEXT PRIMARY KEY REFERENCES usage_multi_execution(allocation_id),
 record TEXT NOT NULL,bundle TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS usage_multi_recovery_proposals(
 proposal_id TEXT PRIMARY KEY,record TEXT NOT NULL,bundle TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS usage_multi_recovery_audits(
 audit_id TEXT PRIMARY KEY,proposal_id TEXT NOT NULL UNIQUE REFERENCES usage_multi_recovery_proposals(proposal_id),
 allocation_id TEXT NOT NULL UNIQUE REFERENCES usage_multi_execution(allocation_id),record TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS usage_multi_recovery_consumed(
 proposal_id TEXT PRIMARY KEY REFERENCES usage_multi_recovery_proposals(proposal_id),
 audit_id TEXT NOT NULL UNIQUE REFERENCES usage_multi_recovery_audits(audit_id),result TEXT NOT NULL);
'''
for table in ('usage_multi_fixture_instances','usage_multi_recovery_commitments','usage_multi_recovery_proposals',
              'usage_multi_recovery_audits','usage_multi_recovery_consumed'):
    for action in ('UPDATE','DELETE'):
        SCHEMA+=f"CREATE TRIGGER IF NOT EXISTS {table}_{action.lower()} BEFORE {action} ON {table} BEGIN SELECT RAISE(ABORT,'immutable recovery evidence'); END;\n"


def _hash(value):return hashlib.sha256(value.encode()).hexdigest()


def fixture_admin(harness,permissions,actor_id=None):
    """Harness owner provisioning only; not exposed by the application server."""
    if gated(harness.registry) is not harness:raise Error('非隔离身份夹具',403)
    if not set(permissions)<=PERMISSIONS:raise Error('恢复权限无效')
    actor_id=actor_id or str(uuid.uuid4());identifier(actor_id)
    token=secrets.token_urlsafe(32);session=str(uuid.uuid4())
    with harness.registry.connect() as db:
        db.execute('INSERT INTO usage_multi_recovery_admins VALUES(?,?,?,1,?) ON CONFLICT(actor_id) DO UPDATE SET session_id=excluded.session_id,token_hash=excluded.token_hash,active=1,permissions=excluded.permissions',
                   (actor_id,session,_hash(token),encoded(sorted(permissions))))
    return dict(actor_id=actor_id,session_id=session,token=token)


def admin(db,token,required):
    if type(token) is not str:raise Error('需要独立管理员身份',403)
    row=db.execute('SELECT * FROM usage_multi_recovery_admins WHERE token_hash=?',(_hash(token),)).fetchone()
    if row is None or row['active']!=1 or not set(required)<=set(json.loads(row['permissions'])):
        raise Error('当前管理员身份/独立恢复权限无效',403)
    return row


def target(db,ident):
    identifier(ident)
    if db.execute('SELECT 1 FROM usage_first_total_grants WHERE grant_id=?',(ident,)).fetchone():raise Error('双账本目标冲突',409)
    allocation=db.execute('SELECT * FROM usage_multi_agent_allocations WHERE allocation_id=?',(ident,)).fetchone()
    if allocation is None:raise Error('原allocation不存在',404)
    from multi_agent_meter import execution_chain
    from usage_accounting import require_source
    value,_,_=execution_chain(db,allocation)
    auth=value.get('tail_authority')
    if auth is None:raise Error('旧事实没有恢复授权',409)
    source=require_source(db,KIND,allocation)
    return allocation,auth,source


def snapshot(db):
    """Canonical full business state; proposal bookkeeping is deliberately excluded."""
    excluded={'usage_multi_recovery_proposals','usage_multi_recovery_audits','usage_multi_recovery_consumed'}
    schema=[tuple(r) for r in db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")]
    rows={}
    for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"):
        name=r[0]
        if name not in excluded:
            rows[name]=sorted([list(row) for row in db.execute('SELECT * FROM "'+name.replace('"','""')+'"')],key=encoded)
    return digest(dict(schema=schema,rows=rows))


def _local(gate,ident):
    store=gate.tail_stores.get(ident)
    if store is None:raise Error('缺原本地冻结证据',409)
    return store.frozen_bundle()


def _instance(db,gate,body,pack):
    from multi_agent_final import stopped
    stopped(gate,body,pack)
    row=db.execute('SELECT * FROM usage_multi_fixture_instances WHERE allocation_id=?',(body['ledger_id'],)).fetchone()
    runtime=gate.allocations.get(body['ledger_id'])
    if (row is None or runtime is None or row['execution_id']!=body['execution_id'] or row['authority_digest']!=body['authority_digest']
            or row['host_id']!=gate.fixture_host_id or row['boot_id']!=gate.fixture_boot_id
            or row['instance_id']!=runtime.fixture_instance_id or row['start_id']!=runtime.fixture_start_id
            or row['pid']!=runtime.fixture_process.pid or not row['started_at']<=body['stop_at']):
        raise Error('原host/boot/进程启动实例观察链缺失',409)
    return dict(row)


def _anchor(db,gate,auth,bundle):
    body=bundle['final'];pack=bundle['evidence']
    row=db.execute('SELECT * FROM usage_multi_recovery_commitments WHERE allocation_id=?',(body['ledger_id'],)).fetchone()
    if row is None:raise Error('缺原窗口master存证与独立观察',409)
    record=json.loads(row['record']);unpack(record)
    instance=_instance(db,gate,body,pack)
    if (row['bundle']!=encoded(bundle) or record['instance']!=instance
            or record['final_digest']!=body['body_digest'] or record['authority_digest']!=auth['body_digest']
            or record['execution_id']!=body['execution_id'] or record['ledger_id']!=body['ledger_id']
            or record['bundle_digest']!=digest(bundle) or record['fence_sequence']!=body['stop_fence_sequence']
            or record['stop_digest']!=body['runtime_stop_receipt_digest'] or record['ticket_digest']!=body['ticket_manifest_digest']
            or not body['freeze_completed_at']<=record['received_at']<=auth['settle_until']
            or record['observer_permissions']!=sorted([CAPTURE,OBSERVE])):
        raise Error('原窗口master存证/独立观察不匹配',409)
    return record


def _backup(gate,proposal_id):
    root=Path(gate.temp.name)/'recovery-private'
    if root.is_symlink():raise Error('私有备份目录不得为符号链接',409)
    root.mkdir(mode=0o700,exist_ok=True);root.chmod(0o700)
    path=root/(proposal_id+'.sqlite')
    fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(fd)
    try:
        with closing(sqlite3.connect(gate.path.as_uri()+'?mode=ro',uri=True)) as source,closing(sqlite3.connect(path)) as dest:
            source.backup(dest)
        with path.open('rb') as f:os.fsync(f.fileno())
        with closing(sqlite3.connect(path)) as db:state=snapshot(db)
        return dict(digest=hashlib.sha256(path.read_bytes()).hexdigest(),state_digest=state)
    except BaseException:
        path.unlink(missing_ok=True);raise


def _backup_valid(gate,proposal):
    root=Path(gate.temp.name)/'recovery-private';path=root/(proposal['proposal_id']+'.sqlite')
    if (root.is_symlink() or root.stat().st_mode&0o077 or not path.is_file() or path.is_symlink() or path.stat().st_mode&0o077
            or hashlib.sha256(path.read_bytes()).hexdigest()!=proposal['backup']['digest']):raise Error('私有一致性备份缺失/损坏',409)
    if proposal['backup']['state_digest']!=proposal['state_digest']:raise Error('备份不是原dry-run快照',409)


def public_summary(auth,body,source,liability,already=False):
    return dict(schema='independent.manual-tail-audit-v1',ledger_kind=KIND,ledger_id=body['ledger_id'],
        execution_id=body['execution_id'],final_digest=body['body_digest'],authority_digest=auth['body_digest'],
        accepted_sequence=source['accepted_sequence'],upload_delta=body['upload_bytes']-int(source['upload_bytes']),
        download_delta=body['download_bytes']-int(source['download_bytes']),expected_liability=liability,
        already_accepted=already,ready_for_execution=False)


def confirmation(proposal):
    return digest({k:proposal[k] for k in ('proposal_id','actor_id','session_id','operation','ledger_id','execution_id',
        'final_digest','state_digest','upload_delta','download_delta','expected_liability','challenge_digest','created_at','expires_at')})


class MultiAgentRecovery:
    def __init__(self,registry):self.registry=registry

    def capture(self,token,ident):
        gate=gated(self.registry)
        with gate.transition():
            with self.registry.connect() as db:
                db.execute('BEGIN');admin(db,token,[CAPTURE,OBSERVE])
            bundle=_local(gate,ident)
            with self.registry.connect() as db:
                db.execute('BEGIN IMMEDIATE');actor=admin(db,token,[CAPTURE,OBSERVE]);_,auth,_=target(db,ident)
                old=db.execute('SELECT 1 FROM usage_multi_recovery_commitments WHERE allocation_id=?',(ident,)).fetchone()
                if old:record=_anchor(db,gate,auth,bundle)
                else:
                    now=time.time();body=bundle['final'];pack=bundle['evidence']
                    if not body['freeze_completed_at']<=now<=auth['settle_until']:raise Error('不可窗口后补造存证',409)
                    instance=_instance(db,gate,body,pack)
                    record=sealed(dict(schema='independent.recovery-observation-v1',ledger_id=ident,execution_id=body['execution_id'],
                        authority_digest=auth['body_digest'],final_digest=body['body_digest'],bundle_digest=digest(bundle),
                        instance=instance,fence_sequence=body['stop_fence_sequence'],stop_digest=body['runtime_stop_receipt_digest'],
                        ticket_digest=body['ticket_manifest_digest'],received_at=now,observer_id=actor['actor_id'],
                        observer_session_digest=_hash(actor['session_id']),observer_permissions=sorted([CAPTURE,OBSERVE])))
                    db.execute('INSERT INTO usage_multi_recovery_commitments VALUES(?,?,?)',(ident,encoded(record),encoded(bundle)))
                return dict(ledger_id=ident,commitment_digest=record['body_digest'],received_at=record['received_at'],ready_for_execution=False)

    def inspect(self,token,ident):
        gate=gated(self.registry)
        with gate.transition(),self.registry.connect() as db:
            db.execute('BEGIN');admin(db,token,[READ]);a,auth,source=target(db,ident)
            bundle=_local(gate,ident)
            from usage_accounting import liability
            from multi_agent_final import history
            accepted=history(db,ident,auth) is not None
            result=public_summary(auth,bundle['final'],source,liability(db,a['cycle_id']),accepted)
            try:
                _anchor(db,gate,auth,bundle)
                if any(not t['successful'] for t in bundle['evidence']['tickets']):raise Error('unknown',409)
                result['recovery_state']='already_accepted' if accepted else 'eligible_for_dry_run'
            except Error:result['recovery_state']='already_accepted' if accepted else 'blocked_missing_or_unknown_evidence'
            return result

    def dry_run(self,token,ident,reason):
        gate=gated(self.registry)
        if type(reason) is not str or not reason.strip() or len(reason)>1000:raise Error('需要明确恢复理由')
        with gate.transition():
            with self.registry.connect() as db:
                db.execute('BEGIN');admin(db,token,[READ,PROPOSE,COMMIT])
            bundle=_local(gate,ident);body=bundle['final'];backup=None
            with self.registry.connect() as db:
                db.execute('BEGIN IMMEDIATE');actor=admin(db,token,[READ,PROPOSE,COMMIT]);a,auth,source=target(db,ident)
                from usage_accounting import liability
                from multi_agent_final import history
                accepted=history(db,ident,auth)
                summary=public_summary(auth,body,source,liability(db,a['cycle_id']),accepted is not None)
                if accepted:return dict(summary=summary,proposal=None)
                anchor=_anchor(db,gate,auth,bundle)
                if any(not t['successful'] for t in bundle['evidence']['tickets']):raise Error('未知字节不得人工确认',409)
                now=time.time()
                if not math.isfinite(now) or now<anchor['received_at']:raise Error('proposal时钟异常',409)
                state=snapshot(db);ident_proposal=str(uuid.uuid4());challenge=secrets.token_urlsafe(32)
                backup=_backup(gate,ident_proposal)
                if backup['state_digest']!=state:raise Error('一致性备份与dry-run状态不同',409)
                record=sealed(dict(proposal_id=ident_proposal,actor_id=actor['actor_id'],session_id=actor['session_id'],
                    operation='manual_recovery',ledger_id=ident,execution_id=body['execution_id'],final_digest=body['body_digest'],
                    state_digest=state,upload_delta=summary['upload_delta'],download_delta=summary['download_delta'],
                    expected_liability=summary['expected_liability'],challenge_digest=_hash(challenge),
                    created_at=now,expires_at=now+300,backup=backup,reason=reason,reason_digest=_hash(reason),
                    commitment_digest=anchor['body_digest']))
                db.execute('INSERT INTO usage_multi_recovery_proposals VALUES(?,?,?)',(ident_proposal,encoded(record),encoded(bundle)))
                return dict(summary=summary,proposal=dict(proposal_id=ident_proposal,challenge=challenge,confirmation_digest=confirmation(record),
                            created_at=now,expires_at=now+300))

    def _proposal(self,db,token,data):
        if type(data) is not dict or set(data)!={'proposal_id','challenge','confirmation_digest','confirmed'} or data['confirmed'] is not True:
            raise Error('恢复提交需要原明确确认')
        identifier(data['proposal_id']);actor=admin(db,token,[READ,PROPOSE,COMMIT])
        row=db.execute('SELECT * FROM usage_multi_recovery_proposals WHERE proposal_id=?',(data['proposal_id'],)).fetchone()
        if row is None:raise Error('proposal不存在',404)
        p=json.loads(row['record']);unpack(p)
        if (p['actor_id']!=actor['actor_id'] or p['session_id']!=actor['session_id'] or p['proposal_id']!=data['proposal_id']
                or row['record']!=encoded(p) or p['operation']!='manual_recovery' or _hash(p['reason'])!=p['reason_digest']
                or p['expires_at']!=p['created_at']+300 or type(data['challenge']) is not str
                or not secrets.compare_digest(p['challenge_digest'],_hash(data['challenge']))
                or data['confirmation_digest']!=confirmation(p)):raise Error('proposal操作者/原会话/确认不匹配',403)
        return p,json.loads(row['bundle'])

    def commit(self,token,data):
        gate=gated(self.registry)
        with gate.transition(),self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE');p,bundle=self._proposal(db,token,data)
            consumed=db.execute('SELECT * FROM usage_multi_recovery_consumed WHERE proposal_id=?',(p['proposal_id'],)).fetchone()
            a,auth,source=target(db,p['ledger_id'])
            from multi_agent_final import history,evidence,manual_receipt
            if consumed:
                prior=history(db,p['ledger_id'],auth)
                if prior is None or prior[0]!=bundle['final']:raise Error('已消费proposal缺原终态',409)
                return json.loads(consumed['result'])
            now=time.time()
            if not p['created_at']<=now<p['expires_at']:raise Error('未消费proposal已过期或时钟异常',409)
            if snapshot(db)!=p['state_digest']:raise Error('dry-run后状态已变化，请重新生成proposal',409)
            _backup_valid(gate,p)
            if _local(gate,p['ledger_id'])!=bundle:raise Error('原冻结正文或证据改变',409)
            anchor=_anchor(db,gate,auth,bundle);body=bundle['final'];pack=bundle['evidence']
            evidence(db,auth,body,pack)
            if any(not t['successful'] for t in pack['tickets']):raise Error('未知字节不得恢复',409)
            if anchor['body_digest']!=p['commitment_digest'] or p['final_digest']!=body['body_digest'] or p['execution_id']!=body['execution_id']:
                raise Error('proposal原目标/存证改变',409)
            from usage_accounting import liability,accept_attributed,require_source
            if (body['upload_bytes']-int(source['upload_bytes'])!=p['upload_delta']
                    or body['download_bytes']-int(source['download_bytes'])!=p['download_delta']
                    or liability(db,a['cycle_id'])!=p['expected_liability']):raise Error('proposal原增量/负债改变',409)
            if history(db,p['ledger_id'],auth) is not None:raise Error('已有final，重新只读核对',409)
            audit_id=str(uuid.uuid4())
            audit=sealed(dict(operation='manual_recovery',audit_id=audit_id,proposal_id=p['proposal_id'],ledger_id=p['ledger_id'],
                execution_id=p['execution_id'],final_digest=p['final_digest'],actor_id=p['actor_id'],
                actor_session_digest=_hash(p['session_id']),state_digest=p['state_digest'],backup_digest=p['backup']['digest'],
                commitment_digest=p['commitment_digest'],confirmation_digest=confirmation(p),reason_digest=p['reason_digest'],
                prior_sequence=source['accepted_sequence'],prior_upload=int(source['upload_bytes']),prior_download=int(source['download_bytes']),
                upload_delta=p['upload_delta'],download_delta=p['download_delta'],expected_liability=p['expected_liability'],accepted_at=now))
            receipt=manual_receipt(body,now,audit_id)
            result=dict(accepted=True,operation='manual_recovery',audit_id=audit_id,ledger_id=p['ledger_id'],
                        final_digest=p['final_digest'],upload_delta=p['upload_delta'],download_delta=p['download_delta'],ready_for_execution=False)
            accept_attributed(db,KIND,a,body['final_sequence'],body['upload_bytes'],body['download_bytes'],body['execution_id'],now)
            db.execute('INSERT INTO usage_multi_meter VALUES(?,?,?,?,?)',(p['ledger_id'],body['final_sequence'],body['upload_bytes'],body['download_bytes'],now))
            db.execute('INSERT INTO usage_multi_finals VALUES(?,?,?,?,?)',(p['ledger_id'],encoded(body),encoded(pack),encoded(receipt),now))
            db.execute('INSERT INTO usage_multi_terminals VALUES(?,?,?,NULL)',(p['ledger_id'],p['final_digest'],'accepted'))
            db.execute('INSERT INTO usage_multi_recovery_audits VALUES(?,?,?,?)',(audit_id,p['proposal_id'],p['ledger_id'],encoded(audit)))
            db.execute('INSERT INTO usage_multi_recovery_consumed VALUES(?,?,?)',(p['proposal_id'],audit_id,encoded(result)))
            require_source(db,KIND,a)
            if liability(db,a['cycle_id'])!=p['expected_liability']:raise Error('人工恢复负债不守恒',409)
            return result

    def result(self,token,data):
        gated(self.registry)
        with self.registry.connect() as db:
            db.execute('BEGIN');p,_=self._proposal(db,token,data);target(db,p['ledger_id'])
            row=db.execute('SELECT result FROM usage_multi_recovery_consumed WHERE proposal_id=?',(p['proposal_id'],)).fetchone()
            return json.loads(row['result']) if row else dict(accepted=False,ready_for_execution=False)


def validate_manual(db,auth,body,pack,receipt,at):
    """Historical decision integrity; revoking an admin never erases confirmed usage."""
    from multi_agent_final import manual_receipt
    try:
        row=db.execute('SELECT * FROM usage_multi_recovery_audits WHERE allocation_id=?',(body['ledger_id'],)).fetchone()
        if row is None:raise ValueError('audit')
        audit=json.loads(row['record']);unpack(audit)
        prop=db.execute('SELECT * FROM usage_multi_recovery_proposals WHERE proposal_id=?',(row['proposal_id'],)).fetchone()
        p=json.loads(prop['record']);unpack(p)
        used=db.execute('SELECT * FROM usage_multi_recovery_consumed WHERE proposal_id=?',(p['proposal_id'],)).fetchone()
        anchor=db.execute('SELECT * FROM usage_multi_recovery_commitments WHERE allocation_id=?',(body['ledger_id'],)).fetchone()
        observed=json.loads(anchor['record']);unpack(observed)
        bundle=dict(final=body,evidence=pack)
        result=json.loads(used['result'])
        if (audit['operation']!='manual_recovery' or audit['audit_id']!=row['audit_id'] or audit['proposal_id']!=p['proposal_id']
                or audit['ledger_id']!=body['ledger_id'] or audit['final_digest']!=body['body_digest'] or audit['execution_id']!=body['execution_id']
                or p['ledger_id']!=body['ledger_id'] or p['execution_id']!=body['execution_id'] or p['final_digest']!=body['body_digest']
                or audit['actor_id']!=p['actor_id'] or audit['actor_session_digest']!=_hash(p['session_id'])
                or audit['confirmation_digest']!=confirmation(p) or audit['accepted_at']!=at or not p['created_at']<=at<p['expires_at']
                or p['created_at']<observed['received_at'] or p['expires_at']!=p['created_at']+300 or p['operation']!='manual_recovery'
                or audit['reason_digest']!=p['reason_digest'] or _hash(p['reason'])!=p['reason_digest'] or observed['bundle_digest']!=digest(bundle)
                or observed['ledger_id']!=body['ledger_id'] or observed['execution_id']!=body['execution_id']
                or observed['stop_digest']!=body['runtime_stop_receipt_digest'] or observed['ticket_digest']!=body['ticket_manifest_digest']
                or observed['fence_sequence']!=body['stop_fence_sequence'] or observed['observer_permissions']!=sorted([CAPTURE,OBSERVE])
                or audit['state_digest']!=p['state_digest'] or audit['backup_digest']!=p['backup']['digest']
                or audit['commitment_digest']!=p['commitment_digest'] or p['commitment_digest']!=observed['body_digest']
                or anchor['bundle']!=encoded(bundle) or prop['bundle']!=encoded(bundle)
                or observed['final_digest']!=body['body_digest'] or observed['authority_digest']!=auth['body_digest']
                or not body['freeze_completed_at']<=observed['received_at']<=auth['settle_until']
                or audit['upload_delta']!=body['upload_bytes']-audit['prior_upload'] or audit['download_delta']!=body['download_bytes']-audit['prior_download']
                or (audit['prior_sequence'],audit['prior_upload'],audit['prior_download'])!=(body['last_ordinary_sequence'],body['last_ordinary_upload'],body['last_ordinary_download'])
                or (audit['upload_delta'],audit['download_delta'],audit['expected_liability'])!=(p['upload_delta'],p['download_delta'],p['expected_liability'])
                or used['audit_id']!=row['audit_id'] or result!=dict(accepted=True,operation='manual_recovery',audit_id=row['audit_id'],ledger_id=body['ledger_id'],
                    final_digest=body['body_digest'],upload_delta=p['upload_delta'],download_delta=p['download_delta'],ready_for_execution=False)
                or receipt!=manual_receipt(body,at,row['audit_id'])):raise ValueError('audit chain')
    except (TypeError,ValueError,KeyError,AttributeError) as exc:raise Error('人工恢复审计链损坏',409) from exc
