"""Durable confirmed cycle totals, independent of deletable node detail rows.

Accepted legacy deltas and the separate applied-grant ledger enter here.
Unapplied intents and tagged reservation claims are not confirmed usage.
Decimal TEXT preserves Python sums beyond SQLite int64.
"""
SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_cycle_confirmed_totals(
 cycle_id TEXT PRIMARY KEY REFERENCES usage_cycles(id) ON DELETE CASCADE,
 total_bytes TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS usage_cycle_confirmed_directions(
 cycle_id TEXT PRIMARY KEY REFERENCES usage_cycles(id) ON DELETE CASCADE,
 upload_bytes TEXT NOT NULL,download_bytes TEXT NOT NULL,unknown_bytes TEXT NOT NULL);
'''


def current(db,cycle_id):
    if not db.in_transaction: raise RuntimeError('confirmed totals require a transaction')
    from usage_accounting import require_cycle
    values=require_cycle(db,cycle_id)
    row=db.execute('SELECT total_bytes FROM usage_cycle_confirmed_totals WHERE cycle_id=?',(cycle_id,)).fetchone()
    known=int(row[0])
    if known<0 or known!=sum(values): raise RuntimeError('invalid confirmed total')
    return known


def add(db,cycle_id,count):
    if type(count) is not int or count<0: raise ValueError('confirmed delta must be a nonnegative integer')
    raise RuntimeError('confirmed growth requires an accounting event')


def preserve_nodes(db,node_ids):
    """Capture upgrade baselines before any supported node deletion path."""
    for nid in node_ids:
        for row in db.execute('SELECT DISTINCT cycle_id FROM usage_deltas WHERE node_id=? AND cycle_id IS NOT NULL',(nid,)).fetchall():
            directions(db,row[0])


def directions(db,cycle_id):
    """Known directions plus an explicit, unassigned historical remainder.

    Never split a surviving aggregate arbitrarily after detail was deleted.
    Callers must withhold finite directional execution if unknown is positive.
    """
    from usage_accounting import require_cycle
    return require_cycle(db,cycle_id)


def add_directions(db,cycle_id,upload,download):
    if any(type(x) is not int or x<0 for x in (upload,download)):
        raise ValueError('confirmed directions must be nonnegative integers')
    raise RuntimeError('confirmed growth requires an accounting event')
