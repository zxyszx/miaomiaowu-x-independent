"""Additive cycle accounting shared by legacy and attributed grant usage."""
import hashlib
import json
import time

from core import Error


VERSION = 'attributed-accounting-v1'
FIRST = 'first_grant'
MULTI = 'multi_allocation'
SOURCE_KINDS = frozenset((FIRST, MULTI))

SCHEMA = '''
CREATE TABLE IF NOT EXISTS usage_cycle_accounting_anchors(
 cycle_id TEXT PRIMARY KEY REFERENCES usage_cycles(id) ON DELETE RESTRICT,
 accounting_version TEXT NOT NULL,
 upload_bytes TEXT NOT NULL,download_bytes TEXT NOT NULL,unknown_bytes TEXT NOT NULL,
 initialized_at REAL NOT NULL,state TEXT NOT NULL CHECK(state IN ('active','blocked')),
 block_reason TEXT);
CREATE TABLE IF NOT EXISTS usage_attributed_sources(
 ledger_kind TEXT NOT NULL CHECK(ledger_kind IN ('first_grant','multi_allocation')),
 ledger_id TEXT NOT NULL,cycle_id TEXT NOT NULL
   REFERENCES usage_cycle_accounting_anchors(cycle_id) ON DELETE RESTRICT,
 reserved_bytes TEXT NOT NULL,
 initial_sequence INTEGER NOT NULL CHECK(initial_sequence>=0),
 accepted_sequence INTEGER NOT NULL CHECK(accepted_sequence>=initial_sequence),
 initial_upload_bytes TEXT NOT NULL,initial_download_bytes TEXT NOT NULL,
 upload_bytes TEXT NOT NULL,download_bytes TEXT NOT NULL,
 execution_id TEXT,identity_digest TEXT NOT NULL,created_at REAL NOT NULL,
 PRIMARY KEY(ledger_kind,ledger_id));
CREATE INDEX IF NOT EXISTS usage_attributed_source_cycle
 ON usage_attributed_sources(cycle_id,ledger_kind,ledger_id);
CREATE TABLE IF NOT EXISTS usage_cycle_accounting_events(
 event_id TEXT PRIMARY KEY,cycle_id TEXT NOT NULL
   REFERENCES usage_cycle_accounting_anchors(cycle_id) ON DELETE RESTRICT,
 event_kind TEXT NOT NULL CHECK(event_kind IN ('legacy','first_grant','multi_allocation')),
 ledger_kind TEXT,ledger_id TEXT,sequence INTEGER NOT NULL CHECK(sequence>0),
 server_id INTEGER,session_id TEXT,node_id INTEGER,user_id INTEGER,credential TEXT,
 upload_delta TEXT NOT NULL,download_delta TEXT NOT NULL,accepted_at REAL NOT NULL,
 FOREIGN KEY(ledger_kind,ledger_id) REFERENCES usage_attributed_sources(ledger_kind,ledger_id)
   ON DELETE RESTRICT,
 CHECK((event_kind='legacy' AND ledger_kind IS NULL AND ledger_id IS NULL
        AND server_id IS NOT NULL AND session_id IS NOT NULL AND node_id IS NOT NULL
        AND user_id IS NOT NULL AND credential IS NOT NULL)
    OR (event_kind IN ('first_grant','multi_allocation')
        AND ledger_kind=event_kind AND ledger_id IS NOT NULL)));
CREATE UNIQUE INDEX IF NOT EXISTS usage_accounting_source_event
 ON usage_cycle_accounting_events(event_kind,ledger_id,sequence)
 WHERE event_kind IN ('first_grant','multi_allocation');
CREATE UNIQUE INDEX IF NOT EXISTS usage_accounting_legacy_event
 ON usage_cycle_accounting_events(
   event_kind,server_id,session_id,sequence,node_id,user_id,credential)
 WHERE event_kind='legacy';
'''


def marker(db):
    return db.execute('SELECT 1 FROM schema_migrations WHERE name=?',
                      (VERSION,)).fetchone() is not None


def _number(value, label):
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise Error('计量账本字段损坏：' + label, 409) from exc
    if result < 0:
        raise Error('计量账本字段损坏：' + label, 409)
    return result


def _digest(kind, row):
    identifier = 'grant_id' if kind == FIRST else 'allocation_id'
    keys = (identifier, 'cycle_id', 'policy_id', 'server_id', 'session_id',
            'token_hash', 'node_id', 'user_id', 'credential', 'reserved_bytes',
            'issued_at', 'valid_until')
    body = dict(ledger_kind=kind, **{key: row[key] for key in keys})
    return hashlib.sha256(json.dumps(
        body, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _source_values(kind, row):
    identifier = row['grant_id'] if kind == FIRST else row['allocation_id']
    return identifier, row['cycle_id'], int(row['reserved_bytes']), _digest(kind, row)


def _legacy_directions(db, cycle_id):
    total_row = db.execute(
        'SELECT total_bytes FROM usage_cycle_confirmed_totals WHERE cycle_id=?',
        (cycle_id,)).fetchone()
    detail_up = detail_down = 0
    for row in db.execute(
            'SELECT upload_bytes,download_bytes FROM usage_deltas WHERE cycle_id=?',
            (cycle_id,)):
        detail_up += _number(row['upload_bytes'], 'legacy upload')
        detail_down += _number(row['download_bytes'], 'legacy download')
    total = detail_up + detail_down if total_row is None else _number(
        total_row['total_bytes'], 'confirmed total')
    direction_row = db.execute('''SELECT upload_bytes,download_bytes,unknown_bytes
        FROM usage_cycle_confirmed_directions WHERE cycle_id=?''',
        (cycle_id,)).fetchone()
    if direction_row is None:
        unknown = total - detail_up - detail_down
        if unknown < 0:
            raise Error('历史方向明细超过确认合计，迁移已阻断', 409)
        return total, (detail_up, detail_down, unknown)
    values = tuple(_number(direction_row[key], 'confirmed direction')
                   for key in ('upload_bytes', 'download_bytes', 'unknown_bytes'))
    if sum(values) != total:
        raise Error('确认方向与合计不一致，迁移已阻断', 409)
    return total, values


def _latest_meter(db, grant_id, kind=FIRST):
    table,identifier=('usage_first_total_meter','grant_id') if kind==FIRST else ('usage_multi_meter','allocation_id')
    rows = db.execute('''SELECT sequence,upload_bytes,download_bytes
        FROM '''+table+' WHERE '+identifier+'=? ORDER BY sequence',
        (grant_id,)).fetchall()
    last_sequence = 0
    up = down = 0
    for row in rows:
        current = (_number(row['upload_bytes'], 'first upload'),
                   _number(row['download_bytes'], 'first download'))
        if row['sequence'] <= last_sequence or current[0] < up or current[1] < down:
            raise Error('首次授予历史累计回退，迁移已阻断', 409)
        last_sequence, up, down = row['sequence'], current[0], current[1]
    return last_sequence, up, down


def _insert_source(db, kind, row, upload, download, *, initial_sequence=0,
                   execution_id=None, now=None):
    ledger_id, cycle_id, reserved, identity = _source_values(kind, row)
    if upload < 0 or download < 0 or upload + download > reserved:
        raise Error('归属来源累计超过原预留，迁移已阻断', 409)
    db.execute('''INSERT INTO usage_attributed_sources(
        ledger_kind,ledger_id,cycle_id,reserved_bytes,initial_sequence,accepted_sequence,
        initial_upload_bytes,initial_download_bytes,upload_bytes,download_bytes,
        execution_id,identity_digest,created_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (kind, ledger_id, cycle_id, str(reserved), initial_sequence,initial_sequence,
         str(upload), str(download), str(upload), str(download), execution_id,
         identity, time.time() if now is None else now))


def migrate(db):
    """Atomically seed every pre-marker cycle and source, then write marker."""
    if not db.in_transaction:
        raise RuntimeError('accounting migration requires a transaction')
    if db.execute('PRAGMA foreign_keys').fetchone()[0] != 1:
        raise RuntimeError('accounting migration requires foreign keys')
    source_columns={row['name'] for row in db.execute(
        'PRAGMA table_info(usage_attributed_sources)')}
    baseline_columns={'initial_sequence','accepted_sequence',
                      'initial_upload_bytes','initial_download_bytes'}
    if marker(db) and not baseline_columns<=source_columns:
        raise Error('归属来源缺少不可变迁移基准，已阻断',409)
    if marker(db):
        return False
    if db.execute('SELECT 1 FROM usage_multi_meter LIMIT 1').fetchone():
        raise Error('后续计量已存在但迁移marker缺失，已阻断',409)
    if not baseline_columns<=source_columns:
        raise Error('计量迁移表结构不完整，已阻断',409)
    if any(db.execute('SELECT 1 FROM ' + table + ' LIMIT 1').fetchone()
           for table in ('usage_cycle_accounting_anchors',
                         'usage_attributed_sources',
                         'usage_cycle_accounting_events')):
        raise Error('计量迁移存在未标记的部分证据', 409)
    now = time.time()
    attributed_by_cycle = {}
    for cycle in db.execute('SELECT id FROM usage_cycles ORDER BY id').fetchall():
        cycle_id = cycle['id']
        total, values = _legacy_directions(db, cycle_id)
        if db.execute('SELECT 1 FROM usage_cycle_confirmed_totals WHERE cycle_id=?',
                      (cycle_id,)).fetchone() is None:
            db.execute('INSERT INTO usage_cycle_confirmed_totals VALUES(?,?)',
                       (cycle_id, str(total)))
        if db.execute('SELECT 1 FROM usage_cycle_confirmed_directions WHERE cycle_id=?',
                      (cycle_id,)).fetchone() is None:
            db.execute('INSERT INTO usage_cycle_confirmed_directions VALUES(?,?,?,?)',
                       (cycle_id, *(str(value) for value in values)))
        db.execute('''INSERT INTO usage_cycle_accounting_anchors
            VALUES(?,?,?,?,?,?,?,NULL)''',
            (cycle_id, VERSION, *(str(value) for value in values), now, 'active'))
        attributed_by_cycle[cycle_id] = 0
    for grant in db.execute(
            'SELECT * FROM usage_first_total_grants ORDER BY grant_id').fetchall():
        sequence, up, down = _latest_meter(db, grant['grant_id'])
        execution = db.execute(
            'SELECT fact FROM usage_first_total_execution WHERE grant_id=?',
            (grant['grant_id'],)).fetchone()
        execution_id = None
        if execution is not None:
            try:
                execution_id = json.loads(execution['fact'])['execution_id']
            except (TypeError, ValueError, KeyError) as exc:
                raise Error('首次授予执行证据损坏，迁移已阻断', 409) from exc
        _insert_source(db, FIRST, grant, up, down, initial_sequence=sequence,
                       execution_id=execution_id, now=now)
        attributed_by_cycle[grant['cycle_id']] += up + down
    for allocation in db.execute(
            'SELECT * FROM usage_multi_agent_allocations ORDER BY allocation_id').fetchall():
        _insert_source(db, MULTI, allocation, 0, 0, now=now)
    for cycle_id, attributed in attributed_by_cycle.items():
        confirmed = _number(db.execute('''SELECT total_bytes
            FROM usage_cycle_confirmed_totals WHERE cycle_id=?''',
            (cycle_id,)).fetchone()[0], 'confirmed total')
        if confirmed < attributed:
            raise Error('确认合计低于既有归属累计，迁移已阻断', 409)
    db.execute('INSERT INTO schema_migrations(name) VALUES(?)', (VERSION,))
    return True


def initialize_cycle(db, cycle_id, *, now=None):
    if not db.in_transaction:
        raise RuntimeError('cycle accounting initialization requires a transaction')
    if not marker(db):
        raise Error('计量账本尚未完成迁移', 409)
    if db.execute('SELECT 1 FROM usage_cycles WHERE id=?', (cycle_id,)).fetchone() is None:
        raise Error('周期不存在', 404)
    timestamp = time.time() if now is None else now
    db.execute('INSERT INTO usage_cycle_confirmed_totals VALUES(?,?)', (cycle_id, '0'))
    db.execute('INSERT INTO usage_cycle_confirmed_directions VALUES(?,?,?,?)',
               (cycle_id, '0', '0', '0'))
    db.execute('''INSERT INTO usage_cycle_accounting_anchors
        VALUES(?,?,?,?,?,?,?,NULL)''',
        (cycle_id, VERSION, '0', '0', '0', timestamp, 'active'))


def _events(db, cycle_id):
    up = down = 0
    for row in db.execute('''SELECT upload_delta,download_delta
        FROM usage_cycle_accounting_events WHERE cycle_id=?''', (cycle_id,)):
        up += _number(row['upload_delta'], 'event upload')
        down += _number(row['download_delta'], 'event download')
    return up, down


def _validate_source_history(db,source):
    kind=source['ledger_kind'];ledger_id=source['ledger_id']
    if kind==MULTI:
        from multi_agent_tail import meter_history
        meter_history(db,ledger_id)
    initial_sequence=_number(source['initial_sequence'],'source initial sequence')
    accepted_sequence=_number(source['accepted_sequence'],'source accepted sequence')
    initial_up=_number(source['initial_upload_bytes'],'source initial upload')
    initial_down=_number(source['initial_download_bytes'],'source initial download')
    current_up=_number(source['upload_bytes'],'source upload')
    current_down=_number(source['download_bytes'],'source download')
    reserved=_number(source['reserved_bytes'],'source reserved')
    if initial_up+initial_down>reserved or current_up+current_down>reserved:
        raise Error('归属来源超过原预留，已阻断',409)
    events=db.execute('''SELECT cycle_id,sequence,upload_delta,download_delta
        FROM usage_cycle_accounting_events
        WHERE event_kind=? AND ledger_kind=? AND ledger_id=? ORDER BY sequence''',
        (kind,kind,ledger_id)).fetchall()
    if any(row['cycle_id']!=source['cycle_id'] for row in events):
        raise Error('归属事件周期冲突，已阻断',409)
    if kind==MULTI:
        if initial_sequence or initial_up or initial_down:
            raise Error('后续配额初始基准必须为零，已阻断',409)
    table,identifier=('usage_first_total_meter','grant_id') if kind==FIRST else ('usage_multi_meter','allocation_id')

    baseline=db.execute('''SELECT upload_bytes,download_bytes
        FROM '''+table+' WHERE '+identifier+'=? AND sequence=?',
        (ledger_id,initial_sequence)).fetchone() if initial_sequence else None
    if (initial_sequence and (baseline is None
            or (_number(baseline['upload_bytes'],'initial meter upload'),
                _number(baseline['download_bytes'],'initial meter download'))
               !=(initial_up,initial_down))):
        raise Error('首次授予迁移基准与计量冲突，已阻断',409)
    meters=db.execute('''SELECT sequence,upload_bytes,download_bytes
        FROM '''+table+' WHERE '+identifier+'=? AND sequence>? ORDER BY sequence',
        (ledger_id,initial_sequence)).fetchall()
    if len(events)!=len(meters):
        raise Error('首次授予事件与计量记录缺失，已阻断',409)
    up,down=initial_up,initial_down
    for event,meter in zip(events,meters):
        if event['sequence']!=meter['sequence'] or event['sequence']<=initial_sequence:
            raise Error('首次授予事件序列冲突，已阻断',409)
        up+=_number(event['upload_delta'],'source event upload')
        down+=_number(event['download_delta'],'source event download')
        if (up,down)!=(_number(meter['upload_bytes'],'meter upload'),
                       _number(meter['download_bytes'],'meter download')):
            raise Error('首次授予事件与累计计量冲突，已阻断',409)
    latest_sequence,meter_up,meter_down=_latest_meter(db,ledger_id,kind)
    if ((up,down)!=(current_up,current_down)
            or (meter_up,meter_down)!=(current_up,current_down)
            or latest_sequence!=accepted_sequence
            or accepted_sequence!=(meters[-1]['sequence'] if meters else initial_sequence)):
        raise Error('归属来源、事件与计量高水位冲突，已阻断',409)
    execution_table='usage_first_total_execution' if kind==FIRST else 'usage_multi_execution'
    execution=db.execute('SELECT fact FROM '+execution_table+' WHERE '+identifier+'=?',
                         (ledger_id,)).fetchone()
    execution_id=None
    if execution is not None:
        try: execution_id=json.loads(execution['fact'])['execution_id']
        except (TypeError,ValueError,KeyError) as exc:
            raise Error('首次授予执行证据损坏，已阻断',409) from exc
    if source['execution_id']!=execution_id and source['execution_id'] is not None:
        raise Error('归属来源执行编号冲突，已阻断',409)
    if events and (source['execution_id'] is None or source['execution_id']!=execution_id):
        raise Error('归属事件缺少精确执行证据，已阻断',409)


def require_cycle(db, cycle_id, *, sources=True):
    """Return authoritative directions or fail closed without repairing damage."""
    if not db.in_transaction:
        raise RuntimeError('accounting integrity requires a transaction')
    if not marker(db):
        raise Error('计量账本尚未完成迁移', 409)
    if db.execute('SELECT 1 FROM usage_cycles WHERE id=?', (cycle_id,)).fetchone() is None:
        raise Error('周期不存在', 404)
    total = db.execute(
        'SELECT total_bytes FROM usage_cycle_confirmed_totals WHERE cycle_id=?',
        (cycle_id,)).fetchone()
    directions = db.execute('''SELECT upload_bytes,download_bytes,unknown_bytes
        FROM usage_cycle_confirmed_directions WHERE cycle_id=?''',
        (cycle_id,)).fetchone()
    anchor = db.execute(
        'SELECT * FROM usage_cycle_accounting_anchors WHERE cycle_id=?',
        (cycle_id,)).fetchone()
    if total is None or directions is None or anchor is None:
        raise Error('周期计量证据缺失，已阻断', 409)
    if anchor['accounting_version'] != VERSION or anchor['state'] != 'active':
        raise Error('周期计量证据已阻断', 409)
    values = tuple(_number(directions[key], 'confirmed direction')
                   for key in ('upload_bytes', 'download_bytes', 'unknown_bytes'))
    confirmed = _number(total['total_bytes'], 'confirmed total')
    base = tuple(_number(anchor[key], 'accounting anchor')
                 for key in ('upload_bytes', 'download_bytes', 'unknown_bytes'))
    event_up, event_down = _events(db, cycle_id)
    if (sum(values) != confirmed or values !=
            (base[0] + event_up, base[1] + event_down, base[2])):
        raise Error('周期计量对账损坏，已阻断', 409)
    if sources:
        for kind, table in ((FIRST, 'usage_first_total_grants'),
                            (MULTI, 'usage_multi_agent_allocations')):
            for row in db.execute(
                    f'SELECT * FROM {table} WHERE cycle_id=?', (cycle_id,)).fetchall():
                require_source(db, kind, row, check_cycle=False)
        for source in db.execute(
                'SELECT * FROM usage_attributed_sources WHERE cycle_id=?',
                (cycle_id,)):
            table='usage_first_total_grants' if source['ledger_kind']==FIRST else 'usage_multi_agent_allocations'
            identifier='grant_id' if source['ledger_kind']==FIRST else 'allocation_id'
            parent=db.execute(f'SELECT * FROM {table} WHERE {identifier}=?',
                              (source['ledger_id'],)).fetchone()
            if parent is None or parent['cycle_id']!=cycle_id:
                raise Error('归属来源父证据缺失或周期冲突，已阻断',409)
            require_source(db,source['ledger_kind'],parent,check_cycle=False)
    return values


def require_source(db, kind, row, *, check_cycle=True):
    if kind not in SOURCE_KINDS:
        raise ValueError('invalid attributed source kind')
    ledger_id, cycle_id, reserved, identity = _source_values(kind, row)
    if check_cycle:
        require_cycle(db, cycle_id, sources=False)
    source = db.execute('''SELECT * FROM usage_attributed_sources
        WHERE ledger_kind=? AND ledger_id=?''', (kind, ledger_id)).fetchone()
    if (source is None or source['cycle_id'] != cycle_id
            or _number(source['reserved_bytes'], 'source reserved') != reserved
            or source['identity_digest'] != identity):
        raise Error('归属来源证据缺失或冲突，已阻断', 409)
    _validate_source_history(db,source)
    return source


def create_source(db, kind, row):
    require_cycle(db, row['cycle_id'], sources=False)
    _insert_source(db, kind, row, 0, 0)


def outstanding(db, cycle_id, kind=None):
    require_cycle(db, cycle_id)
    total = 0
    query = 'SELECT * FROM usage_attributed_sources WHERE cycle_id=?'
    params = [cycle_id]
    if kind is not None:
        query += ' AND ledger_kind=?'
        params.append(kind)
    for row in db.execute(query, params):
        total += (_number(row['reserved_bytes'], 'source reserved')
                  - _number(row['upload_bytes'], 'source upload')
                  - _number(row['download_bytes'], 'source download'))
    return total


def liability(db, cycle_id):
    values = require_cycle(db, cycle_id)
    legacy = sum(_number(row[0], 'legacy hold') for row in db.execute(
        'SELECT reserved_bytes FROM usage_total_allocations WHERE cycle_id=?',
        (cycle_id,)))
    return sum(values) + legacy + outstanding(db, cycle_id)


def _event_id(parts):
    return hashlib.sha256(json.dumps(
        parts, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _advance_confirmed(db, cycle_id, upload, download, values):
    total = sum(values) + upload + download
    db.execute('UPDATE usage_cycle_confirmed_totals SET total_bytes=? WHERE cycle_id=?',
               (str(total), cycle_id))
    db.execute('''UPDATE usage_cycle_confirmed_directions
        SET upload_bytes=?,download_bytes=? WHERE cycle_id=?''',
        (str(values[0] + upload), str(values[1] + download), cycle_id))


def accept_legacy(db, cycle_id, *, server_id, session_id, sequence, node_id,
                  user_id, credential, upload, download, accepted_at):
    values=require_cycle(db, cycle_id)
    parts = dict(kind='legacy', server_id=server_id, session_id=session_id,
                 sequence=sequence, node_id=node_id, user_id=user_id,
                 credential=credential)
    db.execute('''INSERT INTO usage_cycle_accounting_events
        VALUES(?,?,?,NULL,NULL,?,?,?,?,?,?,?,?,?)''',
        (_event_id(parts), cycle_id, 'legacy', sequence, server_id, session_id,
         node_id, user_id, credential, str(upload), str(download), accepted_at))
    _advance_confirmed(db, cycle_id, upload, download, values)


def accept_first(db, grant, sequence, upload, download, execution_id, accepted_at):
    return accept_attributed(db,FIRST,grant,sequence,upload,download,execution_id,accepted_at)


def accept_attributed(db, kind, grant, sequence, upload, download, execution_id, accepted_at):
    source = require_source(db, kind, grant)
    values = require_cycle(db, grant['cycle_id'])
    old_up = _number(source['upload_bytes'], 'source upload')
    old_down = _number(source['download_bytes'], 'source download')
    if upload < old_up or download < old_down:
        raise Error('授予累计计数不可回退', 409)
    reserved = _number(source['reserved_bytes'], 'source reserved')
    if sequence<=source['accepted_sequence']:
        raise Error('归属来源序列不可回退',409)
    if upload + download > reserved:
        raise Error('授予双向累计超限', 409)
    if source['execution_id'] is not None and source['execution_id'] != execution_id:
        raise Error('授予执行编号与归属来源冲突', 409)
    ident=grant['grant_id' if kind==FIRST else 'allocation_id']
    parts = dict(kind=kind, ledger_id=ident, sequence=sequence)
    db.execute('''INSERT INTO usage_cycle_accounting_events
        VALUES(?,?,?,?,?,?,NULL,NULL,NULL,NULL,NULL,?,?,?)''',
        (_event_id(parts), grant['cycle_id'], kind, kind, ident,
         sequence, str(upload - old_up), str(download - old_down), accepted_at))
    db.execute('''UPDATE usage_attributed_sources
        SET accepted_sequence=?,upload_bytes=?,download_bytes=?,execution_id=?
        WHERE ledger_kind=? AND ledger_id=?''',
        (sequence,str(upload),str(download),execution_id,kind,ident))
    _advance_confirmed(db, grant['cycle_id'], upload - old_up, download - old_down, values)
