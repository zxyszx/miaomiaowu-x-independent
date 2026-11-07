"""Persistent discovery hints for already closed, fully acknowledged epochs.

No runtime shutdown, refunds, or policy activation. One bounded page per call;
final evidence remains separately authenticated by the existing delivery route.
"""
import json
from closed_delivery import ClosedDelivery


class ClosedDiscovery:
    def __init__(self,box): self.box=box

    def ready(self,now,excluded=()):
        self.box._clock(now)
        # Materialize immutable fragments before advertising a large epoch.
        # Discovery remains only a hint; the worker will still require the
        # same closed/tail-ACK gates before sending any part.
        from closed_fragments import ClosedFragments
        fragments=ClosedFragments(self.box)
        with self.box.connect() as db:
            large=[r['session'] for r in db.execute(
                'SELECT session FROM closed_epochs WHERE session NOT IN '
                '(SELECT session FROM closed_discoveries)')]
        for session in large:
            with self.box.connect() as db:
                row=db.execute('SELECT body FROM closed_epochs WHERE session=?',(session,)).fetchone()
            if row is not None:
                try:
                    if len(json.loads(row['body']).get('items',[]))>64:
                        fragments.prepare(session)
                except (ValueError,TypeError,json.JSONDecodeError):
                    pass
        with self.box.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            for final in db.execute('SELECT * FROM closed_epochs c WHERE NOT EXISTS '
                                    '(SELECT 1 FROM closed_discoveries d WHERE d.session=c.session) '
                                    'ORDER BY session').fetchall():
                session=final['session']; body=json.loads(final['body'])
                if session in excluded or not 1<=len(body['items']): continue
                if not ClosedDelivery.confirmed(db,session,body): continue
                # Existing explicit jobs already know a cycle. Start their
                # periodic discovery later, without repeating manual scheduling.
                known=db.execute('SELECT 1 FROM closed_deliveries WHERE session=?',(session,)).fetchone()
                db.execute("INSERT OR IGNORE INTO closed_discoveries VALUES(?,?,'pending',NULL,0,?)",
                           (session,final['digest'],now+60 if known else now))
            rows=db.execute("SELECT d.*,c.body,c.digest AS final_digest FROM closed_discoveries d JOIN closed_epochs c USING(session) WHERE status='pending' AND next_attempt<=? ORDER BY next_attempt,session",(now,)).fetchall()
            for row in rows:
                if row['session'] in excluded or row['digest']!=row['final_digest']: continue
                body=json.loads(row['body'])
                if not ClosedDelivery.confirmed(db,row['session'],body): continue
                after=json.loads(row['after_key']) if row['after_key'] else None
                return dict(session=row['session'],digest=row['digest'],
                            request=dict(sessions=[row['session']],after=after))
        return None

    def _match(self,db,job):
        row=db.execute('SELECT d.*,c.body,c.digest AS final_digest FROM closed_discoveries d JOIN closed_epochs c USING(session) WHERE session=?',
                       (job['session'],)).fetchone()
        after=json.dumps(job['request']['after'],separators=(',',':')) if job['request']['after'] is not None else None
        if (row is None or row['status']!='pending' or row['digest']!=job['digest'] or
            row['final_digest']!=job['digest'] or row['after_key']!=after or
            job['request']['sessions']!=[job['session']] or
            not ClosedDelivery.confirmed(db,job['session'],json.loads(row['body']))):
            raise ValueError('排空查询与本地关闭证据不一致')
        return row

    def accept(self,job,reply,now):
        self.box._clock(now)
        fields={'server_id','sessions','drains','next_after','ready_for_execution'}
        if (not isinstance(reply,dict) or set(reply)!=fields or
            type(reply['server_id']) is not int or reply['server_id']!=self.box.server_id or
            reply['sessions']!=job['request']['sessions'] or reply['ready_for_execution'] is not False or
            not isinstance(reply['drains'],list) or len(reply['drains'])>64):
            raise ValueError('排空查询响应身份无效')
        keys=[]
        for item in reply['drains']:
            if (not isinstance(item,dict) or set(item)!={'cycle_id','session_id'} or
                item['session_id']!=job['session'] or not isinstance(item['cycle_id'],str) or
                not 0<len(item['cycle_id'])<=200):
                raise ValueError('排空查询周期无效')
            keys.append([item['cycle_id'],item['session_id']])
        if (keys!=sorted(keys) or len({tuple(k) for k in keys})!=len(keys) or
            (job['request']['after'] is not None and any(k<=job['request']['after'] for k in keys))):
            raise ValueError('排空查询分页顺序无效')
        next_key=reply['next_after']
        if next_key is not None and (len(keys)!=64 or next_key!=keys[-1]):
            raise ValueError('排空查询下一页无效')
        after=json.dumps(next_key,separators=(',',':')) if next_key is not None else None
        from fragment_delivery import FragmentDelivery
        from closed_fragments import ClosedFragments
        fragments=FragmentDelivery(self.box)
        with self.box.connect() as source:
            source.execute('BEGIN')
            final_row=source.execute('SELECT body FROM closed_epochs WHERE session=?',(job['session'],)).fetchone()
            final=json.loads(final_row['body'])
        large=len(final['items'])>64
        manifest=ClosedFragments(self.box).manifest(job['session']) if large else None
        with self.box.connect() as db:
            db.execute('BEGIN IMMEDIATE'); self._match(db,job)
            for cycle,session in keys:
                old=db.execute('SELECT digest FROM closed_deliveries WHERE cycle=? AND session=?',(cycle,session)).fetchone()
                fragment_old=db.execute('SELECT manifest_digest FROM closed_fragment_deliveries WHERE cycle=? AND session=?',(cycle,session)).fetchone()
                if fragment_old and (not large or fragment_old[0]!=manifest['digest']): raise ValueError('最终分片清单已变化')
                if old and old['digest']!=job['digest']: raise ValueError('最终检查点已变化')
                if large:
                    # The discovery cursor and the first fragment cursor move
                    # together in this transaction; a failed page cannot
                    # leave a half-enqueued large cycle.
                    db.execute("INSERT OR IGNORE INTO closed_fragment_deliveries VALUES(?,?,?,0,'pending',0,0)",
                               (cycle,session,manifest['digest']))
                else:
                    # Never reopen a blocked delivery, including after token errors.
                    db.execute("INSERT OR IGNORE INTO closed_deliveries VALUES(?,?,?,'pending',0,0)",
                               (cycle,session,job['digest']))
            db.execute('UPDATE closed_discoveries SET after_key=?,attempts=0,next_attempt=? WHERE session=?',
                       (after,now+(5 if after else 60),job['session']))

    def defer(self,job,now,*,revoked=False):
        self.box._clock(now)
        with self.box.connect() as db:
            db.execute('BEGIN IMMEDIATE'); row=self._match(db,job)
            attempts=row['attempts']+1
            db.execute('UPDATE closed_discoveries SET status=?,attempts=?,next_attempt=? WHERE session=?',
                       ('blocked' if revoked else 'pending',attempts,now+min(60,5*2**min(attempts-1,4)),job['session']))
