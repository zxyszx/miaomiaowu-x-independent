"""Administrator-staged renewal approval; no payment or panel login integration."""
import time
from core import Error
from plan_lifecycle import policy,save,validate

SCHEMA='''
CREATE TABLE IF NOT EXISTS plan_renewals(
 id INTEGER PRIMARY KEY,
 user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 plan_id INTEGER NOT NULL,
 previous_enabled INTEGER NOT NULL,
 previous_expiry REAL NOT NULL,
 expires_at REAL NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending',
 created_at REAL NOT NULL,
 decided_at REAL);
CREATE UNIQUE INDEX IF NOT EXISTS plan_renewal_pending ON plan_renewals(user_id) WHERE status='pending';
'''


def invalidate(db,user_id):
    db.execute("UPDATE plan_renewals SET status='invalidated',decided_at=? WHERE user_id=? AND status='pending'",(time.time(),user_id))


def binding(db,user_id):
    row=db.execute('SELECT plan_id FROM user_plans WHERE user_id=?',(user_id,)).fetchone()
    if row is None: raise Error('用户没有绑定套餐',404)
    if db.execute('SELECT 1 FROM shared_users WHERE user_id=?',(user_id,)).fetchone():
        raise Error('委托身份不能申请所有者套餐续期',409)
    return row[0]


def result(row):
    return {key:row[key] for key in ('id','user_id','plan_id','expires_at','status','created_at','decided_at')}


class Renewals:
    def __init__(self,registry): self.registry=registry

    def list(self,user_id):
        with self.registry.connect() as db:
            if not db.execute('SELECT 1 FROM users WHERE id=?',(user_id,)).fetchone(): raise Error('用户不存在',404)
            return [result(row) for row in db.execute('SELECT * FROM plan_renewals WHERE user_id=? ORDER BY id DESC',(user_id,))]

    def create(self,user_id,data):
        if not isinstance(data,dict) or set(data)!={'expires_at'}: raise Error('续期申请需要 expires_at')
        end=data['expires_at']; validate({'enabled':True,'expires_at':end})
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            # A writer may wait for another transaction. Validate against the
            # time after acquiring the lock, never the pre-wait request time.
            now=time.time()
            if end is None or end<=now: raise Error('续期截止时间必须在未来')
            pid=binding(db,user_id); old=policy(db,user_id)
            if old is None or old[1] is None: raise Error('长期套餐无需续期，请先设置有效期',409)
            if end<=old[1]: raise Error('续期截止时间必须晚于原截止时间')
            if db.execute("SELECT 1 FROM plan_renewals WHERE user_id=? AND status='pending'",(user_id,)).fetchone():
                raise Error('已有待审核的续期申请',409)
            ident=db.execute('''INSERT INTO plan_renewals(user_id,plan_id,previous_enabled,previous_expiry,expires_at,created_at)
                VALUES(?,?,?,?,?,?)''',(user_id,pid,old[0],old[1],end,now)).lastrowid
            return result(db.execute('SELECT * FROM plan_renewals WHERE id=?',(ident,)).fetchone())

    def decide(self,ident,data):
        if not isinstance(data,dict) or set(data)!={'decision'} or data['decision'] not in ('approve','reject'):
            raise Error('审核决定应为 approve 或 reject')
        status='approved' if data['decision']=='approve' else 'rejected'
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT * FROM plan_renewals WHERE id=?',(ident,)).fetchone()
            if row is None: raise Error('续期申请不存在',404)
            if row['status']!='pending':
                if row['status']!=status: raise Error('申请已处理，不能更改审核结果',409)
                return result(row)
            if status=='approved':
                pid=binding(db,row['user_id']); old=policy(db,row['user_id'])
                if pid!=row['plan_id'] or old is None or old[:2]!=(row['previous_enabled'],row['previous_expiry']):
                    raise Error('套餐或有效期已改变，请重新申请',409)
                if row['expires_at']<=time.time(): raise Error('申请截止时间已过，请重新申请',409)
                # Preserve a manual disable; renewal does not silently enable the identity.
                save(self.registry,db,row['user_id'],dict(enabled=bool(old[0]),expires_at=row['expires_at']))
            db.execute('UPDATE plan_renewals SET status=?,decided_at=? WHERE id=?',(status,time.time(),ident))
            return result(db.execute('SELECT * FROM plan_renewals WHERE id=?',(ident,)).fetchone())
