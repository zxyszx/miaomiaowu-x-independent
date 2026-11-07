"""Independent validity of a bound plan identity, never a vendor entitlement."""
import time
from core import Error

SCHEMA='''
CREATE TABLE IF NOT EXISTS plan_lifecycle(
 user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 enabled INTEGER NOT NULL, expires_at REAL, active INTEGER NOT NULL);
'''


def policy(db,user_id):
    row=db.execute('SELECT enabled,expires_at,active FROM plan_lifecycle WHERE user_id=?',(user_id,)).fetchone()
    return tuple(row) if row else None


def expire(registry,db):
    from usage_cycles import refresh
    refresh(registry,db)
    from plan_grace import observe_due
    now=time.time()
    observe_due(db,now)
    rows=db.execute('SELECT user_id FROM plan_lifecycle WHERE active=1 AND expires_at<=?',(now,)).fetchall()
    affected=set()
    for row in rows:
        uid=row['user_id']
        db.execute('UPDATE plan_lifecycle SET active=0 WHERE user_id=?',(uid,))
        affected.update(r[0] for r in db.execute('SELECT DISTINCT n.server_id FROM nodes n JOIN node_users u ON u.node_id=n.id WHERE u.user_id=?',(uid,)))
    for sid in affected: registry._bump(db,sid)


def view(db,user_id):
    row=policy(db,user_id)
    enabled,end,allowed=row if row else (True,None,True)
    active=bool(enabled) and bool(allowed) and (end is None or end>time.time())
    from usage_cycles import boundary
    cycle=boundary(db,user_id)
    if cycle is not None:
        if cycle[0] is None: active=False
        else: end=cycle[1] if end is None else min(end,cycle[1])
    return {'enabled':bool(enabled),'expires_at':end,'status':'disabled' if not enabled else ('active' if active else 'expired')}


def validate(data):
    if not isinstance(data,dict) or set(data)!={'enabled','expires_at'} or type(data['enabled']) is not bool:
        raise Error('套餐有效期需要 enabled 布尔值和 expires_at')
    end=data['expires_at']
    if end is not None and (type(end) not in (int,float) or not 0<end<=253402300799):
        raise Error('到期时间应为有效Unix秒数或null表示长期')


def save(registry,db,user_id,data):
    end=data['expires_at']
    old=policy(db,user_id)
    current=(int(data['enabled']),end,int(data['enabled'] and (end is None or end>time.time())))
    if old!=current:
        db.execute('''INSERT INTO plan_lifecycle VALUES(?,?,?,?) ON CONFLICT(user_id)
            DO UPDATE SET enabled=excluded.enabled,expires_at=excluded.expires_at,active=excluded.active''',(user_id,*current))
        for row in db.execute('SELECT DISTINCT n.server_id FROM nodes n JOIN node_users u ON u.node_id=n.id WHERE u.user_id=?',(user_id,)).fetchall():
            registry._bump(db,row[0])


class Lifecycle:
    def __init__(self,registry): self.registry=registry

    def manage(self,user_id,data=None,*,db=None):
        if data is not None: validate(data)
        with self.registry._resource_transaction(db,write=True) as db:
            if not db.execute('SELECT 1 FROM user_plans WHERE user_id=?',(user_id,)).fetchone():
                raise Error('用户没有绑定套餐',404)
            if db.execute('SELECT 1 FROM shared_users WHERE user_id=?',(user_id,)).fetchone():
                raise Error('委托身份不能设置所有者套餐有效期',409)
            expire(self.registry,db)
            if data is not None:
                old=policy(db,user_id)
                if old is None or old[:2]!=(int(data['enabled']),data['expires_at']):
                    from plan_renewals import invalidate
                    invalidate(db,user_id)
                save(self.registry,db,user_id,data)
            return view(db,user_id)
