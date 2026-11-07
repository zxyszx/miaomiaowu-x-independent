"""Authenticated cumulative usage ingestion foundation, not a quota enforcer.

Counters are payload bytes supplied by an independent Agent. This module does
not measure sockets; runtime metering and durable Agent delivery remain separate.
"""
import hashlib
import json
import math
import time
from core import Error

MAX_COUNTER = 2**63 - 1
SCHEMA = '''
CREATE TABLE IF NOT EXISTS usage_reports(
    server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL, sequence INTEGER NOT NULL, digest TEXT NOT NULL,
    received_at REAL NOT NULL, PRIMARY KEY(server_id,session_id));
CREATE TABLE IF NOT EXISTS usage_counters(
    server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL,
    node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    credential TEXT NOT NULL, upload_bytes INTEGER NOT NULL, download_bytes INTEGER NOT NULL,
    PRIMARY KEY(server_id,session_id,node_id,user_id,credential));
CREATE TABLE IF NOT EXISTS usage_deltas(
    server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL,sequence INTEGER NOT NULL,
    node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    credential TEXT NOT NULL,cycle_id TEXT REFERENCES usage_cycles(id) ON DELETE SET NULL,received_at REAL NOT NULL,
    upload_bytes INTEGER NOT NULL,download_bytes INTEGER NOT NULL,
    PRIMARY KEY(server_id,session_id,sequence,node_id,user_id,credential));
CREATE INDEX IF NOT EXISTS usage_deltas_user_time ON usage_deltas(user_id,received_at);
'''


def integer(value, minimum=0):
    return type(value) is int and minimum <= value <= MAX_COUNTER


def validate(data, registry):
    if not isinstance(data, dict) or set(data) != {'session_id','sequence','items'}:
        raise Error('流量回执字段无效')
    registry._session_value(data['session_id'])
    if not integer(data['sequence'], 1): raise Error('流量回执序号无效')
    if not isinstance(data['items'], list) or not 1 <= len(data['items']) <= 64:
        raise Error('流量回执需要1–64条累计计数')
    seen=set()
    for item in data['items']:
        if not isinstance(item,dict) or set(item) != {'node_id','user_id','credential','upload_bytes','download_bytes'}:
            raise Error('流量计数字段无效')
        if not integer(item['node_id'],1) or not integer(item['user_id'],1): raise Error('流量资源编号无效')
        registry._session_value(item['credential'])
        if not integer(item['upload_bytes']) or not integer(item['download_bytes']): raise Error('流量计数需要非负整数')
        key=(item['node_id'],item['user_id'],item['credential'])
        if key in seen: raise Error('流量回执包含重复资源')
        seen.add(key)
    canonical=dict(data, items=sorted(data['items'],key=lambda x:(x['node_id'],x['user_id'],x['credential'])))
    return hashlib.sha256(json.dumps(canonical,sort_keys=True,separators=(',',':')).encode()).hexdigest()


class UsageLedger:
    def __init__(self, registry): self.registry=registry

    def report(self, token, data):
        return self._report(token,data)

    def settle(self, token, data):
        return self._report(token,data,settlement=True)

    def _report(self, token, data, settlement=False):
        r=self.registry
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            sid=r._agent_id(db,token)
            fingerprint=validate(data,r)
            session=data['session_id']; sequence=data['sequence']
            if not settlement: r._check_session(db,sid,session)
            previous=db.execute('SELECT sequence,digest FROM usage_reports WHERE server_id=? AND session_id=?',(sid,session)).fetchone()
            if settlement:
                from usage_settlement import authorize
                executed_cycles=authorize(db,sid,data,retry=bool(previous and sequence==previous['sequence'] and fingerprint==previous['digest']))
            reply=dict(accepted=True,sequence=sequence,server_id=sid,session_id=session)
            if previous:
                if sequence==previous['sequence'] and fingerprint==previous['digest']:
                    return dict(reply,duplicate=True)
                if sequence<=previous['sequence']: raise Error('流量回执已过期或内容冲突',409)
            received_at=time.time(); changed_cycles=set()
            for item in data['items']:
                from usage_migration import require_open_counter
                require_open_counter(db,sid,session,item)
                nid,uid,credential=(item[k] for k in ('node_id','user_id','credential'))
                grant=db.execute('SELECT 1 FROM node_users u JOIN nodes n ON n.id=u.node_id WHERE n.server_id=? AND u.node_id=? AND u.user_id=? AND u.credential=?',
                                 (sid,nid,uid,credential)).fetchone()
                if grant is None and not settlement: raise Error('流量回执不属于此服务器的当前授权',403)
                key=(sid,session,nid,uid,credential)
                old=db.execute('SELECT upload_bytes,download_bytes FROM usage_counters WHERE server_id=? AND session_id=? AND node_id=? AND user_id=? AND credential=?',key).fetchone()
                upload,download=item['upload_bytes'],item['download_bytes']
                if old and (upload<old['upload_bytes'] or download<old['download_bytes']):
                    raise Error('同一会话累计流量不可回退',409)
                delta_up=upload-(old['upload_bytes'] if old else 0)
                delta_down=download-(old['download_bytes'] if old else 0)
                if old is None and not (upload or download):
                    # Zero is still a new accounting identity. Do not let a
                    # post-drain session bypass the migration fence merely
                    # because it has not reported positive bytes yet.
                    if settlement:
                        initial_cycle=executed_cycles[(nid,uid,credential)]
                    else:
                        initial=db.execute('SELECT id FROM usage_cycles WHERE user_id=? AND starts_at<=? AND ends_at>?',
                                           (uid,received_at,received_at)).fetchone()
                        initial_cycle=initial['id'] if initial else None
                    from usage_migration import require_unsealed_growth, require_legacy_tail
                    require_unsealed_growth(db,initial_cycle)
                    if not settlement:
                        require_legacy_tail(db,sid,session,item,initial_cycle)
                if delta_up or delta_down:
                    if settlement:
                        cycle_id=executed_cycles[(nid,uid,credential)]
                        cycle={'id':cycle_id} if cycle_id is not None else None
                    else:
                        cycle=db.execute('SELECT id FROM usage_cycles WHERE user_id=? AND starts_at<=? AND ends_at>?',(uid,received_at,received_at)).fetchone()
                        from usage_migration import require_legacy_tail
                        require_legacy_tail(db,sid,session,item,cycle['id'] if cycle else None)
                    if cycle is not None:
                        from usage_migration import require_unsealed_growth
                        require_unsealed_growth(db,cycle['id'])
                        budget=db.execute('SELECT upload_bytes,download_bytes FROM usage_cycle_budgets WHERE cycle_id=?',(cycle['id'],)).fetchone()
                        total=db.execute('SELECT total_bytes FROM usage_cycle_total_budgets WHERE cycle_id=?',(cycle['id'],)).fetchone()
                        if budget is not None or (total is not None and total['total_bytes'] is not None):
                            from total_usage import directions
                            used=directions(db,cycle['id'])
                            if budget is not None and used[2] and any(budget[k] is not None for k in ('upload_bytes','download_bytes')):
                                raise Error('历史流量方向未确认，暂停有限方向额度结算',409)
                            if budget is not None and ((budget['upload_bytes'] is not None and used[0]+delta_up>budget['upload_bytes']) or (budget['download_bytes'] is not None and used[1]+delta_down>budget['download_bytes'])):
                                from execution_directions import permits_tail
                                if not settlement or not permits_tail(db,sid,session,item,cycle['id'],budget,used,(delta_up,delta_down),received_at):
                                    raise Error('周期流量额度已耗尽',409)
                            if total is not None and total['total_bytes'] is not None:
                                from total_usage import current as confirmed_total
                                if confirmed_total(db,cycle['id'])+delta_up+delta_down>total['total_bytes']:
                                    raise Error('周期合计流量额度已耗尽',409)
                                if not settlement:
                                    from total_allocation import require_legacy_capacity
                                    require_legacy_capacity(db,cycle['id'],total['total_bytes'],confirmed_total(db,cycle['id']),delta_up+delta_down)
                    if cycle is not None:
                        from usage_accounting import accept_legacy
                        accept_legacy(db,cycle['id'],server_id=sid,session_id=session,
                            sequence=sequence,node_id=nid,user_id=uid,credential=credential,
                            upload=delta_up,download=delta_down,accepted_at=received_at)
                    db.execute('INSERT INTO usage_deltas(server_id,session_id,sequence,node_id,user_id,credential,cycle_id,received_at,upload_bytes,download_bytes) VALUES(?,?,?,?,?,?,?,?,?,?)',
                               (sid,session,sequence,nid,uid,credential,cycle['id'] if cycle else None,received_at,delta_up,delta_down))
                    if cycle is not None: changed_cycles.add(cycle['id'])
                db.execute('INSERT INTO usage_counters VALUES(?,?,?,?,?,?,?) ON CONFLICT(server_id,session_id,node_id,user_id,credential) DO UPDATE SET upload_bytes=excluded.upload_bytes,download_bytes=excluded.download_bytes',
                           (*key,upload,download))
            db.execute('INSERT INTO usage_reports VALUES(?,?,?,?,?) ON CONFLICT(server_id,session_id) DO UPDATE SET sequence=excluded.sequence,digest=excluded.digest,received_at=excluded.received_at',
                       (sid,session,sequence,fingerprint,received_at))
            from usage_cycles import bump_budget_configuration
            bump_budget_configuration(r,db,changed_cycles)
            return dict(reply,duplicate=False)

    def user_period(self, uid, start, end):
        """Receipt-time window [start,end); not a traffic occurrence-time quota.

        Old cumulative counters have no timestamps. Expose their unattributed
        amount instead of assigning them to the current period during upgrade.
        Aggregate in Python to preserve sums beyond SQLite's integer limit.
        """
        if (any(type(x) not in (int,float) or x<0 or x>MAX_COUNTER or not math.isfinite(x) for x in (start,end))
                or start>=end): raise Error('流量周期需要有效的起止时间')
        with self.registry.connect() as db:
            db.execute('BEGIN')
            if db.execute('SELECT 1 FROM users WHERE id=?',(uid,)).fetchone() is None:
                raise Error('用户不存在',404)
            nodes={}; attributed_up=attributed_down=0
            for row in db.execute('SELECT * FROM usage_deltas WHERE user_id=?',(uid,)):
                attributed_up+=row['upload_bytes']; attributed_down+=row['download_bytes']
                if not start<=row['received_at']<end: continue
                key=(row['server_id'],row['node_id'])
                item=nodes.setdefault(key,dict(server_id=key[0],node_id=key[1],upload_bytes=0,download_bytes=0))
                item['upload_bytes']+=row['upload_bytes']; item['download_bytes']+=row['download_bytes']
            counters=db.execute('SELECT upload_bytes,download_bytes FROM usage_counters WHERE user_id=?',(uid,)).fetchall()
            missing_up=sum(r['upload_bytes'] for r in counters)-attributed_up
            missing_down=sum(r['download_bytes'] for r in counters)-attributed_down
            items=[nodes[key] for key in sorted(nodes)]
            return dict(user_id=uid,start=start,end=end,basis='received_at',
                        upload_bytes=sum(x['upload_bytes'] for x in items),
                        download_bytes=sum(x['download_bytes'] for x in items),nodes=items,
                        unattributed_upload_bytes=missing_up,unattributed_download_bytes=missing_down)

    def user(self, uid):
        with self.registry.connect() as db:
            db.execute('BEGIN')
            if db.execute('SELECT 1 FROM users WHERE id=?',(uid,)).fetchone() is None: raise Error('用户不存在',404)
            nodes={}
            for row in db.execute('SELECT server_id,node_id,upload_bytes,download_bytes FROM usage_counters WHERE user_id=?',(uid,)):
                key=(row['server_id'],row['node_id'])
                item=nodes.setdefault(key,dict(server_id=key[0],node_id=key[1],upload_bytes=0,download_bytes=0))
                for field in ('upload_bytes','download_bytes'): item[field]+=row[field]
            items=[nodes[key] for key in sorted(nodes)]
            return dict(user_id=uid,upload_bytes=sum(x['upload_bytes'] for x in items),
                        download_bytes=sum(x['download_bytes'] for x in items),nodes=items)
