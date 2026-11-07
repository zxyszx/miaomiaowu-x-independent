"""Primary subscription identity and presentation order, separate from execution policy."""
from core import Error
from accounts import Accounts

SCHEMA='''
CREATE TABLE IF NOT EXISTS plan_order(
 parent_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 position INTEGER NOT NULL,
 PRIMARY KEY(parent_id,user_id), UNIQUE(parent_id,position));
'''


class PlanOrder:
    def __init__(self,registry): self.registry=registry

    def manage(self,parent_id,data=None):
        if data is not None:
            if not isinstance(data,dict) or set(data)!={'user_ids'} or not isinstance(data['user_ids'],list):
                raise Error('套餐顺序需要 user_ids 列表，第一项为主套餐身份')
            ids=data['user_ids']
            if any(type(uid) is not int or uid<1 for uid in ids) or len(set(ids))!=len(ids):
                raise Error('套餐身份编号必须是互不重复的正整数')
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE'); Accounts.parent(db,parent_id)
            # Legacy credentials remain on the parent; picking a child as primary never rebinds it.
            members={row['user_id']:row['plan_id'] for row in db.execute('''
                SELECT user_id,plan_id FROM user_plans WHERE user_id=?
                UNION ALL SELECT user_id,plan_id FROM plan_accounts WHERE parent_id=?''',(parent_id,parent_id))}
            if data is not None:
                if set(ids)!=set(members): raise Error('套餐身份已变化，请刷新后提交完整顺序',409)
                db.execute('DELETE FROM plan_order WHERE parent_id=?',(parent_id,))
                db.executemany('INSERT INTO plan_order VALUES(?,?,?)',[(parent_id,uid,pos) for pos,uid in enumerate(ids)])
            saved=[row[0] for row in db.execute('SELECT user_id FROM plan_order WHERE parent_id=? ORDER BY position',(parent_id,)) if row[0] in members]
            # Deleted/unbound identities are absent; newly bound identities append deterministically.
            ordered=saved+sorted(set(members)-set(saved),key=lambda uid:(uid!=parent_id,uid))
            return {'parent_id':parent_id,'primary_user_id':ordered[0] if ordered else None,
                    'items':[{'user_id':uid,'plan_id':members[uid],'is_main_account':uid==parent_id,
                              'is_primary':index==0} for index,uid in enumerate(ordered)]}
