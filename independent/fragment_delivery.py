"""Persistent one-fragment delivery cursor for explicitly known drain cycles.

No automatic discovery, network calls or runtime closure. Every ready/ACK step
rechecks original local evidence and all frozen tail receipts before progressing.
"""
from closed_fragments import ClosedFragments
from closed_delivery import ClosedDelivery
from closed_transport import fragment_receipt


class FragmentDelivery:
    def __init__(self,box):
        self.box=box;self.fragments=ClosedFragments(box)
        with box.connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS closed_fragment_deliveries(
                cycle TEXT NOT NULL,session TEXT NOT NULL,manifest_digest TEXT NOT NULL,
                part INTEGER NOT NULL,status TEXT NOT NULL,attempts INTEGER NOT NULL,
                next_attempt REAL NOT NULL,PRIMARY KEY(cycle,session))''')

    def enqueue(self,cycle,session):
        if not isinstance(cycle,str) or not 1<=len(cycle)<=200:
            raise ValueError('最终分片周期无效')
        envelope=self.fragments.prepare(session)
        if len(envelope['manifest']['parts'])>4096:
            raise ValueError('最终分片清单过大')
        with self.box.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT manifest_digest FROM closed_fragment_deliveries WHERE cycle=? AND session=?',
                           (cycle,session)).fetchone()
            if old and old[0]!=envelope['digest']:raise ValueError('最终分片清单已变化')
            # Never reopen accepted or blocked work on repeated discovery.
            db.execute("INSERT OR IGNORE INTO closed_fragment_deliveries VALUES(?,?,?,0,'pending',0,0)",
                       (cycle,session,envelope['digest']))

    def _job(self,cycle,session,part):
        envelope=self.fragments.manifest(session)
        if envelope is None:raise ValueError('最终分片清单缺失')
        return dict(cycle_id=cycle,envelope=envelope,fragment=self.fragments.part(session,part))

    def _match(self,db,job):
        m=job['envelope']['manifest'];session=m['session_id']
        row=db.execute('SELECT * FROM closed_fragment_deliveries WHERE cycle=? AND session=?',
                       (job['cycle_id'],session)).fetchone()
        final=db.execute('SELECT body,digest FROM closed_epochs WHERE session=?',(session,)).fetchone()
        import json
        if (row is None or row['status']!='pending' or row['manifest_digest']!=job['envelope']['digest'] or
            row['part']!=job['fragment']['body']['index'] or final is None or
            final['digest']!=m['checkpoint_digest'] or m['server_id']!=self.box.server_id or
            not ClosedDelivery.confirmed(db,session,json.loads(final['body']))):
            raise ValueError('最终分片与原关闭证据或确认状态不符')
        return row

    def ready(self,now,excluded=()):
        self.box._clock(now)
        with self.box.connect() as db:
            rows=db.execute("SELECT * FROM closed_fragment_deliveries WHERE status='pending' AND next_attempt<=? ORDER BY next_attempt,cycle,session",(now,)).fetchall()
        for row in rows:
            if row['session'] in excluded:continue
            job=self._job(row['cycle'],row['session'],row['part'])
            with self.box.connect() as db:
                db.execute('BEGIN')
                # Concurrent cursor advancement yields a fresh ready lookup;
                # do not issue an already superseded request.
                current=db.execute('SELECT * FROM closed_fragment_deliveries WHERE cycle=? AND session=?',
                                   (row['cycle'],row['session'])).fetchone()
                if current is None or tuple(current)!=tuple(row):continue
                try:self._match(db,job)
                except ValueError:continue
                return job
        return None

    def acknowledge(self,job,receipt):
        # Verify immutable source outside the update transaction, then bind its
        # same digest and closure/ACK gates inside the write transaction.
        m=job['envelope']['manifest'];session=m['session_id'];part=job['fragment']['body']['index']
        if self._job(job['cycle_id'],session,part)!=job:raise ValueError('最终分片待发内容变化')
        fragment_receipt(job['cycle_id'],job['envelope'],job['fragment'],receipt)
        last=part==len(m['parts'])-1
        if receipt['complete']!=last:raise ValueError('最终分片完成状态与持久进度不符')
        with self.box.connect() as db:
            db.execute('BEGIN IMMEDIATE');self._match(db,job)
            db.execute('''UPDATE closed_fragment_deliveries SET part=?,status=?,attempts=0,next_attempt=0
                          WHERE cycle=? AND session=?''',
                       (part if last else part+1,'accepted' if last else 'pending',job['cycle_id'],session))

    def defer(self,job,now,*,revoked=False):
        self.box._clock(now)
        m=job['envelope']['manifest'];session=m['session_id'];part=job['fragment']['body']['index']
        if self._job(job['cycle_id'],session,part)!=job:raise ValueError('最终分片待发内容变化')
        with self.box.connect() as db:
            db.execute('BEGIN IMMEDIATE');row=self._match(db,job)
            attempts=row['attempts']+1
            db.execute('''UPDATE closed_fragment_deliveries SET status=?,attempts=?,next_attempt=?
                          WHERE cycle=? AND session=?''',
                       ('blocked' if revoked else 'pending',attempts,now+min(60,5*2**min(attempts-1,4)),
                        job['cycle_id'],session))
