"""Local durable quota reservations; never a substitute for a usage receipt.

Commit before transport write. A crash or write failure may burn reserved bytes,
but restarting against an old master baseline cannot grant them a second time.
"""
from contextlib import closing
from pathlib import Path
import sqlite3
from payload_meter import CycleQuota

# Reservations execute before transport.write on the event loop. Keep FULL
# durability and atomic reservations, but never wait seconds for another writer.
PAYLOAD_LOCK_TIMEOUT=0.05


class QuotaStore:
    def __init__(self,path,server_id):
        self.path=Path(path)
        self.server_id=server_id

    def update(self,key,used,limits,direction=None,count=0,*,total_limit=None):
        if total_limit is not None and (type(total_limit) is not int or total_limit<0):
            raise ValueError('周期合计流量额度无效')
        self.path.parent.mkdir(parents=True,exist_ok=True)
        try:
            with closing(sqlite3.connect(self.path,timeout=PAYLOAD_LOCK_TIMEOUT)) as db, db:
                db.execute('PRAGMA synchronous=FULL')
                db.execute('BEGIN IMMEDIATE')
                db.execute('CREATE TABLE IF NOT EXISTS owner(server_id INTEGER NOT NULL)')
                owner=db.execute('SELECT server_id FROM owner').fetchall()
                if not owner: db.execute('INSERT INTO owner VALUES(?)',(self.server_id,))
                elif owner!=[(self.server_id,)]: raise ValueError('周期额度存储服务器身份变化')
                db.execute('''CREATE TABLE IF NOT EXISTS quotas(
                    user_id INTEGER NOT NULL,cycle_id TEXT NOT NULL,
                    upload INTEGER NOT NULL CHECK(upload>=0),
                    download INTEGER NOT NULL CHECK(download>=0),
                    PRIMARY KEY(user_id,cycle_id))''')
                row=db.execute('SELECT upload,download FROM quotas WHERE user_id=? AND cycle_id=?',key).fetchone()
                totals=[max(local,remote) for local,remote in zip(used,row or (0,0))]
                allowed=0
                if direction is not None:
                    limit=limits[direction]
                    allowed=count if limit is None else min(count,max(0,limit-totals[direction]))
                    # One combined allowance, not a separate copy per direction.
                    # Both local high waters (including uncertain writes) are
                    # read and reserved inside this same SQLite write lock.
                    if total_limit is not None:
                        allowed=min(allowed,max(0,total_limit-sum(totals)))
                    totals[direction]+=allowed
                db.execute('INSERT OR REPLACE INTO quotas VALUES(?,?,?,?)',(*key,*totals))
            return allowed,totals
        except (sqlite3.Error,OverflowError) as error:
            raise OSError('周期额度持久存储失败，停止载荷转发') from error


class PersistentCycleQuota(CycleQuota):
    def __init__(self,store,key):
        super().__init__()
        self.store=store
        self.key=key
        self.limits=(None,None)

    def reconcile(self,upload_limit=None,download_limit=None,upload_used=0,download_used=0,*,total_limit=None):
        # Validate before touching storage and preserve the in-memory high water.
        super().reconcile(upload_limit,download_limit,upload_used,download_used,total_limit=total_limit)
        self.limits=(upload_limit,download_limit)
        _,totals=self.store.update(self.key,self.used,self.limits,total_limit=self.total_limit)
        super().reconcile(*self.limits,*totals,total_limit=self.total_limit)
        return self

    def reserve(self,direction,count):
        if type(direction) is not int or direction not in (0,1): raise ValueError('流量方向无效')
        if type(count) is not int or count<0: raise ValueError('流量字节数无效')
        allowed,totals=self.store.update(self.key,self.used,self.limits,direction,count,total_limit=self.total_limit)
        super().reconcile(*self.limits,*totals,total_limit=self.total_limit)
        return allowed
