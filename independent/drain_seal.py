"""Conservative closure of captured legacy evidence, never protocol activation.

Accepted checkpoints are authenticated Agent declarations after actual runtime
closure. Their digests alone cannot prove a remote process has stopped. This
gate neither reconstructs deleted pre-drain history nor releases reservations.
"""
import hashlib
import json
import time
from core import Error


def identity(row):
    return tuple(row[k] for k in ('server_id','session_id','node_id','user_id','credential'))


def inspect(db,cycle,*,revalidate=False):
    if not db.in_transaction: raise RuntimeError('drain inspection requires a transaction')
    if not db.execute('SELECT 1 FROM usage_cycle_drains WHERE cycle_id=?',(cycle,)).fetchone():
        raise Error('周期尚未排空',409)
    prior=db.execute('SELECT body,digest FROM usage_drain_seals WHERE cycle_id=?',(cycle,)).fetchone()
    if prior:
        try:
            body=json.loads(prior['body'])
            encoded=json.dumps(body,sort_keys=True,separators=(',',':'))
            valid=(hashlib.sha256(encoded.encode()).hexdigest()==prior['digest']
                   and body['cycle_id']==cycle
                   and body['state']=='captured_legacy_evidence_sealed'
                   and body['ready_for_execution'] is False and body['blockers']==[])
        except (ValueError,TypeError,KeyError):
            valid=False
        if not valid: raise Error('排空封存证据完整性校验失败',409)
        if not revalidate: return body,prior['digest']
    blockers=set()
    captured=db.execute('SELECT 1 FROM usage_drain_inventory_captures WHERE cycle_id=?',(cycle,)).fetchone()
    if not captured: blockers.add('frontier_not_captured')
    frontier=db.execute('SELECT * FROM usage_drain_inventory WHERE cycle_id=? ORDER BY server_id,session_id,node_id,user_id,credential',(cycle,)).fetchall()
    if not frontier: blockers.add('empty_frontier')
    expected={identity(row):row for row in frontier}
    sessions={(row['server_id'],row['session_id']) for row in frontier}
    proofs={}
    for row in db.execute('SELECT * FROM usage_drain_checkpoints WHERE cycle_id=? ORDER BY server_id,session_id',(cycle,)):
        proofs[(row['server_id'],row['session_id'])]=row
    if set(proofs)!=sessions: blockers.add('missing_or_extra_checkpoints')
    finals={}; evidence=[]
    for key,row in proofs.items():
        # Stored bodies are scoped by accept_closed; the digest authenticates
        # the original whole-session envelope, not this scoped representation.
        body=json.loads(row['body'])
        if body['uncertain_writes'] or (body['server_id'],body['session_id'])!=key:
            blockers.add('invalid_checkpoint')
        scoped={}
        for item in body['items']:
            item_key=(*key,item['node_id'],item['user_id'],item['credential'])
            values=(item['upload_bytes'],item['download_bytes'])
            if item_key in scoped or any(type(v) is not int or v<0 for v in values):
                blockers.add('invalid_checkpoint')
            scoped[item_key]=values
        wanted={k for k in expected if k[:2]==key}
        if set(scoped)!=wanted: blockers.add('checkpoint_identity_mismatch')
        finals.update(scoped)
        evidence.append(dict(server_id=key[0],session_id=key[1],digest=row['digest']))
    up=down=0
    for key,row in expected.items():
        if key not in finals: continue
        values=finals[key]; up+=values[0]; down+=values[1]
        if any(row[k] is not None and values[i]<row[k] for i,k in enumerate(('upload_bytes','download_bytes'))):
            blockers.add('below_captured_high_water')
        current=db.execute('SELECT upload_bytes,download_bytes FROM usage_counters WHERE server_id=? AND session_id=? AND node_id=? AND user_id=? AND credential=?',key).fetchone()
        # Accepted proof survives supported node/server cleanup. Absence before
        # acceptance cannot reach here: accept_closed requires exact counters.
        if current and tuple(current)!=values: blockers.add('counter_changed_after_checkpoint')
    for row in db.execute('SELECT * FROM usage_execution_grants WHERE cycle_id=?',(cycle,)):
        if identity(row) not in expected: blockers.add('uncaptured_execution')
    total=db.execute('SELECT total_bytes FROM usage_cycle_confirmed_totals WHERE cycle_id=?',(cycle,)).fetchone()
    directions=db.execute('SELECT upload_bytes,download_bytes,unknown_bytes FROM usage_cycle_confirmed_directions WHERE cycle_id=?',(cycle,)).fetchone()
    if total is None or directions is None:
        blockers.add('missing_confirmed_baseline')
    else:
        counts=tuple(int(x) for x in directions)
        if min(counts)<0 or int(total[0])!=sum(counts): blockers.add('inconsistent_confirmed_baseline')
        if counts[2]!=0: blockers.add('unknown_confirmed_direction')
        if counts[:2]!=(up,down) or int(total[0])!=up+down:
            blockers.add('unaccounted_confirmed_usage')
    from total_allocation import held
    held_bytes=held(db,cycle)
    if held_bytes: blockers.add('unresolved_allocations')
    return dict(cycle_id=cycle,state='draining',ready_for_execution=False,
                captured_identities=len(expected),accepted_sessions=len(proofs),
                expected_sessions=len(sessions),final_upload_bytes=up,final_download_bytes=down,
                held_bytes=held_bytes,blockers=sorted(blockers),evidence=evidence),None


class DrainSeal:
    def __init__(self,registry): self.registry=registry

    def status(self,cycle):
        with self.registry.connect() as db:
            db.execute('BEGIN')
            body,digest=inspect(db,cycle)
            return dict(body,digest=digest,evidence_complete=not body['blockers'])

    def seal(self,cycle):
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            body,digest=inspect(db,cycle)
            if digest: return dict(body,digest=digest,duplicate=True)
            if body['blockers']: raise Error('排空证据尚未收齐或存在未归属用量',409)
            body=dict(body,state='captured_legacy_evidence_sealed')
            encoded=json.dumps(body,sort_keys=True,separators=(',',':'))
            digest=hashlib.sha256(encoded.encode()).hexdigest()
            db.execute('INSERT INTO usage_drain_seals VALUES(?,?,?,?)',(cycle,encoded,digest,time.time()))
            return dict(body,digest=digest,duplicate=False)
