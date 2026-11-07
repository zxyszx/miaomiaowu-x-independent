"""Immutable identity periods and internal byte budgets.

Calendar scheduling and plan inheritance remain separate work. Aggregate
receipt guards are not distributed Agent allocations or grace execution.
"""
import time
import uuid
from core import Error
from total_policy import SCHEMA as TOTAL_POLICY_SCHEMA
from total_allocation import SCHEMA as TOTAL_ALLOCATION_SCHEMA
from total_usage import SCHEMA as TOTAL_USAGE_SCHEMA
from allocation_evidence import SCHEMA as ALLOCATION_EVIDENCE_SCHEMA
from usage_migration import SCHEMA as MIGRATION_SCHEMA
from first_grant import SCHEMA as FIRST_GRANT_SCHEMA
from multi_agent_allocation import SCHEMA as MULTI_AGENT_ALLOCATION_SCHEMA
from multi_agent_configuration import SCHEMA as MULTI_AGENT_CONFIGURATION_SCHEMA
from grant_meter import SCHEMA as GRANT_METER_SCHEMA
from grant_configuration import SCHEMA as GRANT_CONFIGURATION_SCHEMA
from multi_agent_lifecycle import SCHEMA as MULTI_EXECUTION_SCHEMA
from multi_agent_meter import SCHEMA as MULTI_METER_SCHEMA
from multi_agent_tail import SCHEMA as MULTI_TAIL_SCHEMA
from multi_agent_final import SCHEMA as MULTI_FINAL_SCHEMA
from multi_agent_recovery import SCHEMA as MULTI_RECOVERY_SCHEMA

SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_cycles(
 id TEXT PRIMARY KEY,user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 starts_at REAL NOT NULL,ends_at REAL NOT NULL,expired INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS usage_cycles_user_time ON usage_cycles(user_id,starts_at,ends_at);
CREATE TABLE IF NOT EXISTS usage_cycle_budgets(
 cycle_id TEXT PRIMARY KEY REFERENCES usage_cycles(id) ON DELETE CASCADE,
 upload_bytes INTEGER, download_bytes INTEGER);
CREATE TABLE IF NOT EXISTS usage_cycle_total_budgets(
 cycle_id TEXT PRIMARY KEY REFERENCES usage_cycles(id) ON DELETE CASCADE,
 total_bytes INTEGER CHECK(total_bytes IS NULL OR total_bytes>=0));
CREATE TABLE IF NOT EXISTS usage_cycle_state(
 user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,active_id TEXT);
'''+TOTAL_POLICY_SCHEMA+TOTAL_ALLOCATION_SCHEMA+ALLOCATION_EVIDENCE_SCHEMA+TOTAL_USAGE_SCHEMA+MIGRATION_SCHEMA+FIRST_GRANT_SCHEMA+MULTI_AGENT_ALLOCATION_SCHEMA+MULTI_AGENT_CONFIGURATION_SCHEMA+GRANT_METER_SCHEMA+GRANT_CONFIGURATION_SCHEMA+MULTI_EXECUTION_SCHEMA+MULTI_METER_SCHEMA+MULTI_TAIL_SCHEMA+MULTI_FINAL_SCHEMA+MULTI_RECOVERY_SCHEMA


def boundary(db,user_id,now=None):
    """None means unconfigured; (None,None) means configured but no open period."""
    if not db.execute('SELECT 1 FROM usage_cycles WHERE user_id=?',(user_id,)).fetchone(): return None
    now=time.time() if now is None else now
    row=db.execute('SELECT id,ends_at FROM usage_cycles WHERE user_id=? AND expired=0 AND starts_at<=? AND ends_at>?',
                   (user_id,now,now)).fetchone()
    return (row['id'],row['ends_at']) if row else (None,None)


def refresh(registry,db):
    """Persist expired periods and bump each affected host once per refresh."""
    now=time.time()
    db.execute('UPDATE usage_cycles SET expired=1 WHERE expired=0 AND ends_at<=?',(now,))
    affected=set()
    for row in db.execute('SELECT DISTINCT user_id FROM usage_cycles').fetchall():
        uid=row['user_id']; active=boundary(db,uid,now)[0]
        previous=db.execute('SELECT active_id FROM usage_cycle_state WHERE user_id=?',(uid,)).fetchone()
        if previous is not None and previous['active_id']==active: continue
        db.execute('INSERT INTO usage_cycle_state VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET active_id=excluded.active_id',(uid,active))
        affected.update(r[0] for r in db.execute('SELECT DISTINCT n.server_id FROM nodes n JOIN node_users u ON u.node_id=n.id WHERE u.user_id=?',(uid,)))
    for sid in affected: registry._bump(db,sid)


def bump_budget_configuration(registry,db,cycle_ids):
    """Invalidate active hosts on stored budget or accepted-usage changes.

    Aggregate caps remain internal until their Agent wire policy is integrated.
    """
    affected=set(); now=time.time()
    for ident in cycle_ids:
        affected.update(row[0] for row in db.execute('''SELECT DISTINCT n.server_id
            FROM usage_cycles c
            JOIN node_users u ON u.user_id=c.user_id JOIN nodes n ON n.id=u.node_id
            WHERE c.id=? AND c.expired=0 AND c.starts_at<=? AND c.ends_at>?
            AND (EXISTS(SELECT 1 FROM usage_cycle_budgets WHERE cycle_id=c.id)
                 OR EXISTS(SELECT 1 FROM usage_cycle_total_budgets WHERE cycle_id=c.id))''',
            (ident,now,now)))
    for sid in affected: registry._bump(db,sid)


class UsageCycles:
    def __init__(self,registry): self.registry=registry

    def create(self,user_id,data):
        if not isinstance(data,dict) or set(data)!={'starts_at','ends_at'}:
            raise Error('周期需要 starts_at 和 ends_at')
        start,end=data['starts_at'],data['ends_at']
        if (any(type(x) not in (int,float) or not 0<=x<=253402300799 for x in (start,end)) or start>=end):
            raise Error('周期起止时间无效')
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if not db.execute('SELECT 1 FROM user_plans WHERE user_id=?',(user_id,)).fetchone():
                raise Error('用户没有绑定套餐',404)
            if db.execute('SELECT 1 FROM shared_users WHERE user_id=?',(user_id,)).fetchone():
                raise Error('委托身份不能设置所有者周期',409)
            if end<=time.time(): raise Error('不能创建已结束的周期')
            if db.execute('SELECT 1 FROM usage_cycles WHERE user_id=? AND starts_at<? AND ends_at>?',(user_id,end,start)).fetchone():
                raise Error('周期与已有记录重叠',409)
            ident=str(uuid.uuid4())
            db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',(ident,user_id,start,end))
            from usage_accounting import initialize_cycle
            initialize_cycle(db,ident)
            refresh(self.registry,db)
            return dict(id=ident,user_id=user_id,starts_at=start,ends_at=end)

    def set_budget(self, cycle_id, data):
        if not isinstance(data,dict) or set(data)!={'upload_bytes','download_bytes'} or any(type(data[k]) is not int or data[k]<0 or data[k]>2**63-1 for k in data):
            raise Error('周期额度需要非负整数上传和下载上限')
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if not db.execute('SELECT 1 FROM usage_cycles WHERE id=?',(cycle_id,)).fetchone(): raise Error('周期不存在',404)
            old=db.execute('SELECT upload_bytes,download_bytes FROM usage_cycle_budgets WHERE cycle_id=?',(cycle_id,)).fetchone()
            db.execute('INSERT INTO usage_cycle_budgets VALUES(?,?,?) ON CONFLICT(cycle_id) DO UPDATE SET upload_bytes=excluded.upload_bytes,download_bytes=excluded.download_bytes',(cycle_id,data['upload_bytes'],data['download_bytes']))
            if old is None or tuple(old)!=(data['upload_bytes'],data['download_bytes']):
                bump_budget_configuration(self.registry,db,{cycle_id})
            return dict(cycle_id=cycle_id,**data)

    def set_total_budget(self,cycle_id,total_bytes):
        """Internal byte cap, not a plan GB field or an Agent allocation.

        None is unlimited, zero denies new positive deltas. No public endpoint
        until execution configuration and distributed allocation are integrated.
        """
        if total_bytes is not None and (type(total_bytes) is not int or not 0<=total_bytes<=2**63-1):
            raise Error('周期合计额度需要非负整数字节或不限')
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if not db.execute('SELECT 1 FROM usage_cycles WHERE id=?',(cycle_id,)).fetchone():
                raise Error('周期不存在',404)
            old=db.execute('SELECT total_bytes FROM usage_cycle_total_budgets WHERE cycle_id=?',(cycle_id,)).fetchone()
            from total_policy import current
            current(db,cycle_id) # preserve an upgrade baseline before replacement
            db.execute('INSERT INTO usage_cycle_total_budgets VALUES(?,?) ON CONFLICT(cycle_id) DO UPDATE SET total_bytes=excluded.total_bytes',
                       (cycle_id,total_bytes))
            if old is None or old['total_bytes']!=total_bytes:
                db.execute("INSERT INTO usage_total_policy_versions(cycle_id,total_bytes,observed_at,origin) VALUES(?,?,?,'change')",
                           (cycle_id,total_bytes,time.time()))
                bump_budget_configuration(self.registry,db,{cycle_id})
            return dict(cycle_id=cycle_id,total_bytes=total_bytes)

    def total_budget(self,cycle_id):
        with self.registry.connect() as db:
            row=db.execute('SELECT cycle_id,total_bytes FROM usage_cycle_total_budgets WHERE cycle_id=?',(cycle_id,)).fetchone()
            return dict(row) if row else None

    def budget(self, cycle_id):
        with self.registry.connect() as db:
            row=db.execute('SELECT cycle_id,upload_bytes,download_bytes FROM usage_cycle_budgets WHERE cycle_id=?',(cycle_id,)).fetchone()
            return dict(row) if row else None

    def list(self,user_id):
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if not db.execute('SELECT 1 FROM users WHERE id=?',(user_id,)).fetchone(): raise Error('用户不存在',404)
            refresh(self.registry,db)
            return [dict(r) for r in db.execute('SELECT * FROM usage_cycles WHERE user_id=? ORDER BY starts_at,id',(user_id,))]
