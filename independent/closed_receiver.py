"""Authenticated fragment collector; Agent automatic dispatch is not enabled.

Partial receipts never seal an identity or create confirmed/billable usage.
Completion reuses the ordinary final-evidence acceptance inside one transaction.
"""
import json
import re
import time
from core import Error
from usage_ledger import integer,validate
from closed_fragments import encoded,digest

SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_closed_manifests(
 cycle_id TEXT NOT NULL REFERENCES usage_cycle_drains(cycle_id) ON DELETE CASCADE,
 server_id INTEGER NOT NULL,session_id TEXT NOT NULL,
 body TEXT NOT NULL,digest TEXT NOT NULL,
 PRIMARY KEY(cycle_id,server_id,session_id));
CREATE TABLE IF NOT EXISTS usage_closed_parts(
 cycle_id TEXT NOT NULL REFERENCES usage_cycle_drains(cycle_id) ON DELETE CASCADE,
 server_id INTEGER NOT NULL,session_id TEXT NOT NULL,part INTEGER NOT NULL,
 body TEXT NOT NULL,digest TEXT NOT NULL,
 PRIMARY KEY(cycle_id,server_id,session_id,part));
'''


def is_digest(value):
    return isinstance(value,str) and re.fullmatch('[0-9a-f]{64}',value) is not None


class ClosedReceiver:
    def __init__(self,registry): self.registry=registry

    def accept(self,cycle_id,token,envelope,fragment):
        if not isinstance(cycle_id,str) or not 1<=len(cycle_id)<=200:
            raise Error('周期编号无效')
        if not isinstance(envelope,dict) or set(envelope)!={'manifest','digest'}:
            raise Error('分片清单字段无效')
        m=envelope['manifest']
        if not isinstance(m,dict) or set(m)!={'format','server_id','session_id','checkpoint_digest','item_count','parts'}:
            raise Error('分片清单内容无效')
        if (type(m['format']) is not int or m['format']!=1 or not integer(m['server_id'],1) or
            not is_digest(m['checkpoint_digest']) or not integer(m['item_count'],1) or
            not isinstance(m['parts'],list) or not 1<=len(m['parts'])<=4096):
            raise Error('分片清单范围无效')
        self.registry._session_value(m['session_id'])
        if envelope['digest']!=digest(m): raise Error('分片清单摘要不符')
        count=0
        for index,p in enumerate(m['parts']):
            if (not isinstance(p,dict) or set(p)!={'index','count','digest'} or
                type(p['index']) is not int or p['index']!=index or
                not integer(p['count'],1) or p['count']>64 or
                (index<len(m['parts'])-1 and p['count']!=64) or not is_digest(p['digest'])):
                raise Error('分片顺序或数量无效')
            count+=p['count']
        if count!=m['item_count']: raise Error('清单身份总数不符')
        if not isinstance(fragment,dict) or set(fragment)!={'body','digest'}:
            raise Error('分片字段无效')
        b=fragment['body']
        if (not isinstance(b,dict) or set(b)!={'format','server_id','session_id','checkpoint_digest','index','total','items'} or
            type(b['format']) is not int or b['format']!=1 or
            type(b['server_id']) is not int or b['server_id']!=m['server_id'] or
            b['session_id']!=m['session_id'] or b['checkpoint_digest']!=m['checkpoint_digest'] or
            type(b['total']) is not int or b['total']!=len(m['parts']) or
            type(b['index']) is not int or not 0<=b['index']<len(m['parts'])):
            raise Error('分片身份或位置不符')
        validate(dict(session_id=b['session_id'],sequence=1,items=b['items']),self.registry)
        spec=m['parts'][b['index']]
        if (len(b['items'])!=spec['count'] or fragment['digest']!=spec['digest'] or
            digest(b)!=fragment['digest']): raise Error('分片内容与清单不符')
        r=self.registry; key=(cycle_id,m['server_id'],m['session_id'])
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            sid=r._agent_id(db,token)
            if sid!=m['server_id']: raise Error('分片不属于此服务器',403)
            capture=db.execute('SELECT 1 FROM usage_drain_inventory_captures WHERE cycle_id=?',(cycle_id,)).fetchone()
            frontier=db.execute('SELECT token_hash,issued_at,settle_until FROM usage_drain_inventory WHERE cycle_id=? AND server_id=? AND session_id=?',key).fetchall()
            if capture is None or not frontier: raise Error('缺少原排空身份清单',409)
            fingerprint=db.execute('SELECT token_hash FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
            if any(row['token_hash']!=fingerprint for row in frontier):
                raise Error('原执行凭据已撤销',403)
            accepted=db.execute('SELECT digest FROM usage_drain_checkpoints WHERE cycle_id=? AND server_id=? AND session_id=?',key).fetchone()
            if accepted and accepted[0]!=m['checkpoint_digest']:
                raise Error('最终检查点内容冲突',409)
            now=time.time()
            if not accepted and any(not row['issued_at']<=now<=row['settle_until'] for row in frontier):
                raise Error('原执行最终结算窗口已结束',403)
            old=db.execute('SELECT body,digest FROM usage_closed_manifests WHERE cycle_id=? AND server_id=? AND session_id=?',key).fetchone()
            if old and (old['body']!=encoded(m) or old['digest']!=envelope['digest']):
                raise Error('分片清单冲突',409)
            if old is None:
                db.execute('INSERT INTO usage_closed_manifests VALUES(?,?,?,?,?)',(*key,encoded(m),envelope['digest']))
            oldpart=db.execute('SELECT body,digest FROM usage_closed_parts WHERE cycle_id=? AND server_id=? AND session_id=? AND part=?',(*key,b['index'])).fetchone()
            if oldpart and (oldpart['body']!=encoded(b) or oldpart['digest']!=fragment['digest']):
                raise Error('已接收分片内容冲突',409)
            if oldpart is None:
                db.execute('INSERT INTO usage_closed_parts VALUES(?,?,?,?,?,?)',(*key,b['index'],encoded(b),fragment['digest']))
            rows=db.execute('SELECT part,body,digest FROM usage_closed_parts WHERE cycle_id=? AND server_id=? AND session_id=? ORDER BY part',key).fetchall()
            # Validate every stored piece even while incomplete. A malformed or
            # overlapping earlier fragment cannot be hidden until completion.
            items=[]; seen=set()
            for row in rows:
                index=row['part']
                if not 0<=index<len(m['parts']): raise Error('存储分片位置无效',409)
                part=json.loads(row['body'])
                if (row['digest']!=m['parts'][index]['digest'] or digest(part)!=row['digest'] or
                    part.get('index')!=index or part.get('session_id')!=m['session_id'] or
                    part.get('server_id')!=sid or part.get('checkpoint_digest')!=m['checkpoint_digest'] or
                    part.get('total')!=len(m['parts']) or len(part.get('items',[]))!=m['parts'][index]['count']):
                    raise Error('存储分片不完整或内容变化',409)
                validate(dict(session_id=m['session_id'],sequence=1,items=part['items']),r)
                for item in part['items']:
                    identity=(item['node_id'],item['user_id'],item['credential'])
                    if identity in seen: raise Error('跨分片身份重复',409)
                    seen.add(identity); items.append(item)
            if len(rows)<len(m['parts']):
                return dict(accepted=True,complete=False,server_id=sid,cycle_id=cycle_id,
                            session_id=m['session_id'],manifest_digest=envelope['digest'],
                            part=b['index'],ready_for_execution=False)
            if [(i['node_id'],i['user_id'],i['credential']) for i in items]!=sorted(seen):
                raise Error('完整分片身份顺序不符',409)
            body=dict(format=1,server_id=sid,session_id=m['session_id'],items=items,uncertain_writes=[])
            if digest(body)!=m['checkpoint_digest']: raise Error('完整检查点摘要不符',409)
            from usage_migration import UsageMigration
            result=UsageMigration(r)._accept_closed(db,cycle_id,token,body,m['checkpoint_digest'])
            return dict(result,complete=True,manifest_digest=envelope['digest'],part=b['index'])
