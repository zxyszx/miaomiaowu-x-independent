"""Historical local installation facts; never liveness or a master execution ACK.

Runtime.install records only after checking its started processes. Reopening
this journal cannot activate listeners or authorize master metering.
"""
from contextlib import closing
import copy
import json
import sqlite3
import time
from grant_agent import validate, checksum


def record(stage, value, runtime_digest, protocol):
    value = copy.deepcopy(value)
    content = validate(value, stage.server_id, stage.session_id)
    checksum(runtime_digest)
    expected = 'reality' if 'reality' in content['node_config'] else 'vless'
    if protocol != expected:
        raise ValueError('安装记录协议与授予不一致')
    # No private keys or client credentials in the receipt journal.
    fact = dict(grant_id=value['grant_id'], digest=value['digest'],
                runtime_digest=runtime_digest, protocol=protocol,
                server_id=stage.server_id, session_id=stage.session_id,
                revision=value['revision'], valid_until=value['valid_until'])
    raw = json.dumps(fact, sort_keys=True, separators=(',', ':'), allow_nan=False)
    staged = json.dumps(content, sort_keys=True, separators=(',', ':'),
                        ensure_ascii=False, allow_nan=False)
    with closing(sqlite3.connect(stage.path, timeout=0.05)) as db, db:
        db.execute('PRAGMA synchronous=FULL')
        db.execute('BEGIN IMMEDIATE')
        validate(value, stage.server_id, stage.session_id)
        owner = db.execute('SELECT server_id FROM grant_owner').fetchall()
        rows = db.execute('''SELECT ledger_kind,payload,digest
            FROM staged_grants WHERE grant_id=?''',(value['grant_id'],)).fetchall()
        expected_stage = [('first_grant',staged,value['digest'])]
        if owner != [(stage.server_id,)] or rows != expected_stage:
            raise ValueError('安装记录缺少原授予暂存')
        db.execute('''CREATE TABLE IF NOT EXISTS local_grant_installations(
            grant_id TEXT PRIMARY KEY, fact TEXT NOT NULL,
            first_installed_at REAL NOT NULL)''')
        old = db.execute('SELECT fact FROM local_grant_installations WHERE grant_id=?',
                         (value['grant_id'],)).fetchone()
        if old is not None and old[0] != raw:
            raise ValueError('不能覆盖原授予安装记录')
        if old is None:
            db.execute('INSERT INTO local_grant_installations VALUES(?,?,?)',
                       (value['grant_id'], raw, time.time()))
    return fact
