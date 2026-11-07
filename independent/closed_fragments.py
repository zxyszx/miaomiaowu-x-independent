"""Internal, immutable fragmentation of existing local close evidence.

No HTTP dispatch or master acceptance. Hashes bind content, not authentication
or proof that a remote executor stopped. Unknown writes remain a hard barrier.
"""
import hashlib
import json
from usage_ledger import integer,validate


def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)


def digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


class ClosedFragments:
    def __init__(self,box):
        self.box=box
        with box.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS closed_fragment_manifests(
                    session TEXT PRIMARY KEY,body TEXT NOT NULL,digest TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS closed_fragment_parts(
                    session TEXT NOT NULL,part INTEGER NOT NULL,body TEXT NOT NULL,
                    digest TEXT NOT NULL,PRIMARY KEY(session,part));''')

    def _expected(self,db,session):
        self.box._session_value(session)
        row=db.execute('SELECT body,digest FROM closed_epochs WHERE session=?',(session,)).fetchone()
        if row is None: raise ValueError('缺少本地实际关闭检查点')
        body=json.loads(row['body'])
        if (not isinstance(body,dict) or
            set(body)!={'format','server_id','session_id','items','uncertain_writes'} or
            type(body['format']) is not int or body['format']!=1 or
            not integer(body['server_id'],1) or body['server_id']!=self.box.server_id or
            body['session_id']!=session or digest(body)!=row['digest']):
            raise ValueError('本地关闭检查点身份或摘要不一致')
        if body['uncertain_writes']!=[]:
            raise ValueError('未知写入不能拆分为最终证据')
        items=body['items']
        if not isinstance(items,list) or not items: raise ValueError('关闭检查点没有身份')
        keys=[]; parts=[]
        total=(len(items)+63)//64
        for offset in range(0,len(items),64):
            batch=items[offset:offset+64]
            validate(dict(session_id=session,sequence=1,items=batch),self.box)
            keys.extend((i['node_id'],i['user_id'],i['credential']) for i in batch)
            part=dict(format=1,server_id=self.box.server_id,session_id=session,
                      checkpoint_digest=row['digest'],index=offset//64,total=total,items=batch)
            parts.append(dict(body=part,digest=digest(part)))
        # Per-page validation alone cannot detect a repeated identity on a
        # different page. The source is already ordered by close_epoch().
        if keys!=sorted(set(keys)): raise ValueError('关闭身份跨分片重复或乱序')
        manifest=dict(format=1,server_id=self.box.server_id,session_id=session,
                      checkpoint_digest=row['digest'],item_count=len(items),
                      parts=[dict(index=i,count=len(p['body']['items']),digest=p['digest'])
                             for i,p in enumerate(parts)])
        return dict(manifest=manifest,digest=digest(manifest)),parts

    def _load(self,db,session,expected,parts):
        row=db.execute('SELECT body,digest FROM closed_fragment_manifests WHERE session=?',(session,)).fetchone()
        stored=db.execute('SELECT part,body,digest FROM closed_fragment_parts WHERE session=? ORDER BY part',
                          (session,)).fetchall()
        if row is None:
            if stored: raise ValueError('分片缺少完整清单')
            return None
        if (row['body']!=encoded(expected['manifest']) or row['digest']!=expected['digest'] or
            len(stored)!=len(parts)):
            raise ValueError('分片清单缺失或内容变化')
        for index,(saved,part) in enumerate(zip(stored,parts)):
            if (saved['part']!=index or saved['body']!=encoded(part['body']) or
                saved['digest']!=part['digest']):
                raise ValueError('分片缺失、混入其他会话或内容变化')
        return expected

    def prepare(self,session):
        with self.box.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            expected,parts=self._expected(db,session)
            old=self._load(db,session,expected,parts)
            if old is not None: return old
            db.execute('INSERT INTO closed_fragment_manifests VALUES(?,?,?)',
                       (session,encoded(expected['manifest']),expected['digest']))
            db.executemany('INSERT INTO closed_fragment_parts VALUES(?,?,?,?)',
                           [(session,i,encoded(p['body']),p['digest']) for i,p in enumerate(parts)])
            return expected

    def manifest(self,session):
        with self.box.connect() as db:
            db.execute('BEGIN')
            expected,parts=self._expected(db,session)
            return self._load(db,session,expected,parts)

    def part(self,session,index):
        if type(index) is not int or index<0: raise ValueError('分片编号无效')
        with self.box.connect() as db:
            db.execute('BEGIN')
            expected,parts=self._expected(db,session)
            if self._load(db,session,expected,parts) is None or index>=len(parts):
                raise ValueError('分片不存在')
            return parts[index]
