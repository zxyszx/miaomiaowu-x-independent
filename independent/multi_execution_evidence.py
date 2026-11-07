"""Private historical multi evidence, never authentication or execution permission.

Uses only the caller's existing SQLite transaction. No bind, migration, gate,
maintenance or runtime calls. Accounting source/event/meter/confirmed validation
remains with require_source in the original authenticated callers; this reader
must not call it recursively or replace that separate integrity check.
"""
import json

from core import Error
from usage_accounting import MULTI
from multi_agent_lifecycle import encoded,chain


def _read_multi_execution(db,kind,ident,*,expected_allocation=None):
    """Validate original durable facts without rebinding a moving cycle baseline."""
    if not db.in_transaction:
        raise RuntimeError('execution evidence requires caller transaction')
    if type(kind) is not str or kind!=MULTI:
        raise Error('multi execution evidence requires exact ledger kind',409)
    from grant_agent import identifier
    try:
        identifier(ident)
    except (ValueError,TypeError) as exc:
        raise Error('invalid multi execution evidence identity') from exc
    if db.execute('SELECT 1 FROM usage_first_total_grants WHERE grant_id=?',(ident,)).fetchone():
        raise Error('multi execution evidence ledger collision',409)
    allocation=db.execute('SELECT * FROM usage_multi_agent_allocations WHERE allocation_id=?',(ident,)).fetchone()
    if allocation is None:
        raise Error('original multi allocation missing',404)
    if expected_allocation is not None and dict(expected_allocation)!=dict(allocation):
        raise Error('original multi allocation identity conflict',409)
    config=db.execute('SELECT * FROM usage_multi_agent_configurations WHERE allocation_id=?',(ident,)).fetchone()
    ref=db.execute('SELECT * FROM usage_multi_configuration_refs WHERE allocation_id=?',(ident,)).fetchone()
    execution=db.execute('SELECT fact FROM usage_multi_execution WHERE allocation_id=?',(ident,)).fetchone()
    life=db.execute('SELECT state FROM usage_multi_lifecycle WHERE allocation_id=?',(ident,)).fetchone()
    if any(row is None for row in (config,ref,execution,life)):
        raise Error('multi计量缺少持久执行链',409)
    if life['state'] not in ('execution_recorded','closing','blocked','closed'):
        raise Error('multi执行生命周期与持久记录冲突',409)
    try:
        from grant_agent import validate
        from node_config import digest
        from multi_agent_installation import fact_for
        content=json.loads(config['payload'])
        if digest(content)!=config['digest']: raise ValueError('binding digest')
        value=dict(content,digest=config['digest'])
        validate(value,allocation['server_id'],allocation['session_id'],now=allocation['issued_at'])
        from multi_agent_tail import attach
        value=attach(db,value)
        if life['state']=='closed' and (not value.get('tail_authority') or not db.execute(
                "SELECT 1 FROM usage_multi_terminals WHERE allocation_id=? AND state='closed'",(ident,)).fetchone()):
            raise ValueError('closed without terminal')
        for key in ('cycle_id','policy_id','server_id','session_id','node_id',
                    'reserved_bytes','issued_at'):
            if value[key]!=allocation[key]: raise ValueError('allocation identity')
        if (value['grant_id']!=ident or value['valid_until']!=allocation['valid_until']
                or value['baseline_bytes']!=int(allocation['baseline_bytes'])
                or value['client']['user_id']!=allocation['user_id']
                or value['client']['uuid']!=allocation['credential']
                or (ref['digest'],ref['token_hash'])!=(config['digest'],allocation['token_hash'])):
            raise ValueError('binding identity')
        fact=json.loads(execution['fact'])
        if encoded(fact_for(value,fact['execution_id']))!=execution['fact']:
            raise ValueError('execution fact')
        if chain(db,ident,value)!=execution['fact']: raise ValueError('installation chain')
        first=db.execute('SELECT * FROM usage_first_total_grants WHERE grant_id=?',
                         (allocation['first_grant_id'],)).fetchone()
        from drain_seal import inspect
        _,seal_digest=inspect(db,allocation['cycle_id'])
        if (first is None or first['cycle_id']!=allocation['cycle_id']
                or first['user_id']!=allocation['user_id']
                or first['seal_digest']!=allocation['seal_digest']
                or seal_digest!=allocation['seal_digest']):
            raise ValueError('ancestry')
    except (ValueError,TypeError,KeyError,AttributeError) as exc:
        raise Error('multi持久执行链损坏',409) from exc
    return value,fact,life['state']

