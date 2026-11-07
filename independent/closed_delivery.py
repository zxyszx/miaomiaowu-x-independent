"""Durable final evidence delivery for explicitly identified drain cycles.

Never discovers/activates a policy or closes a runtime. Only previously closed
Agent epochs may be scheduled; final usage receipts must be confirmed first.
"""
import json
from usage_ledger import validate


class ClosedDelivery:
    def __init__(self,box): self.box=box

    @staticmethod
    def confirmed(db,session,body):
        """A local close record alone never proves that the tail was ACKed."""
        if any(db.execute('SELECT 1 FROM '+table+' WHERE session=?',(session,)).fetchone()
               for table in ('pending','isolation','quarantine','failures','write_intents')):
            return False
        if body['uncertain_writes']: return False
        if db.execute('SELECT 1 FROM retired WHERE session=?',(session,)).fetchone():
            return True
        for item in body['items']:
            row=db.execute('SELECT * FROM totals WHERE session=? AND node=? AND user=? AND credential=?',
                           (session,item['node_id'],item['user_id'],item['credential'])).fetchone()
            if (row is None or row['acked_sequence']==0 or
                (row['upload'],row['download'],row['acked_upload'],row['acked_download'])!=
                (item['upload_bytes'],item['download_bytes'])*2):
                return False
        return True

    def enqueue(self,cycle,session):
        if not isinstance(cycle,str) or not cycle or len(cycle)>200:
            raise ValueError('最终检查点周期无效')
        final=self.box.closed_epoch(session)
        if final is None: raise ValueError('执行尚无关闭检查点')
        body=final['checkpoint']
        validate(dict(session_id=session,sequence=1,items=body['items']),self.box)
        if body['uncertain_writes']: raise ValueError('未知写入不能作为最终检查点投递')
        with self.box.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT digest FROM closed_deliveries WHERE cycle=? AND session=?',(cycle,session)).fetchone()
            if old and old[0]!=final['digest']: raise ValueError('最终检查点已变化')
            db.execute("INSERT OR IGNORE INTO closed_deliveries VALUES(?,?,?,'pending',0,0)",
                       (cycle,session,final['digest']))

    def ready(self,now):
        self.box._clock(now)
        with self.box.connect() as db:
            db.execute('BEGIN')
            rows=db.execute("SELECT * FROM closed_deliveries WHERE status='pending' AND next_attempt<=? ORDER BY next_attempt,cycle,session",(now,)).fetchall()
            for job in rows:
                session=job['session']
                final=db.execute('SELECT body,digest FROM closed_epochs WHERE session=?',(session,)).fetchone()
                if final is None or final['digest']!=job['digest']: continue
                body=json.loads(final['body'])
                if not self.confirmed(db,session,body): continue
                return dict(cycle_id=job['cycle'],checkpoint=body,digest=job['digest'])
        return None

    def _match(self,db,job):
        row=db.execute('SELECT d.*,c.body FROM closed_deliveries d JOIN closed_epochs c USING(session) WHERE cycle=? AND session=?',
                       (job['cycle_id'],job['checkpoint']['session_id'])).fetchone()
        if (row is None or row['digest']!=job['digest'] or json.loads(row['body'])!=job['checkpoint']):
            raise ValueError('最终检查点投递身份不一致')
        return row

    def acknowledge(self,job,receipt):
        expected=dict(cycle_id=job['cycle_id'],session_id=job['checkpoint']['session_id'],
                      server_id=self.box.server_id,digest=job['digest'])
        if (not isinstance(receipt,dict) or receipt.get('accepted') is not True or
            type(receipt.get('server_id')) is not int or any(receipt.get(k)!=v for k,v in expected.items())):
            raise ValueError('最终检查点确认与待发证据不一致')
        with self.box.connect() as db:
            db.execute('BEGIN IMMEDIATE'); self._match(db,job)
            db.execute("UPDATE closed_deliveries SET status='accepted' WHERE cycle=? AND session=?",
                       (job['cycle_id'],job['checkpoint']['session_id']))

    def defer(self,job,now,*,revoked=False):
        self.box._clock(now)
        with self.box.connect() as db:
            db.execute('BEGIN IMMEDIATE'); row=self._match(db,job)
            if row['status']!='pending': return
            attempts=row['attempts']+1
            db.execute('UPDATE closed_deliveries SET status=?,attempts=?,next_attempt=? WHERE cycle=? AND session=?',
                       ('blocked' if revoked else 'pending',attempts,now+min(60,5*2**min(attempts-1,4)),
                        job['cycle_id'],job['checkpoint']['session_id']))
