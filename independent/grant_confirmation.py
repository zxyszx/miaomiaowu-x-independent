"""Explicit installation claim and durable exact master receipt; no mode activation."""
from contextlib import closing
import copy
import json
import sqlite3
from grant_agent import validate
from grant_ack import fact_for,receipt


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)


def installed(stage,value):
    # Read-only avoids silently recreating a lost journal.
    with closing(sqlite3.connect(stage.path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        row=db.execute('SELECT fact FROM local_grant_installations WHERE grant_id=?',
                       (value['grant_id'],)).fetchone()
    expected=fact_for(value)
    if row is None or row[0]!=canonical(expected):
        raise ValueError('缺少匹配的持久安装记录')
    return expected


def remember_ack(stage, value, response):
    expected = receipt(installed(stage, value))
    raw = canonical(expected)
    if canonical(response) != raw:
        raise ValueError('安装确认记录不匹配')
    with closing(sqlite3.connect(stage.path, timeout=0.05)) as db, db:
        db.execute('PRAGMA synchronous=FULL')
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT fact FROM local_grant_installations WHERE grant_id=?',
                         (value['grant_id'],)).fetchone()
        if row is None or row[0] != canonical(expected['installation']):
            raise ValueError('安装确认缺少原安装记录')
        db.execute('CREATE TABLE IF NOT EXISTS local_grant_master_acks('
                   'grant_id TEXT PRIMARY KEY, receipt TEXT NOT NULL)')
        old = db.execute('SELECT receipt FROM local_grant_master_acks WHERE grant_id=?',
                         (value['grant_id'],)).fetchone()
        if old is not None and old[0] != raw:
            raise ValueError('不能覆盖原安装确认')
        if old is None:
            db.execute('INSERT INTO local_grant_master_acks VALUES(?,?)',
                       (value['grant_id'], raw))


def acknowledged(stage, value):
    expected = receipt(installed(stage, value))
    with closing(sqlite3.connect(stage.path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                            "AND name='local_grant_master_acks'").fetchone()
        row = db.execute('SELECT receipt FROM local_grant_master_acks WHERE grant_id=?',
                         (value['grant_id'],)).fetchone() if exists else None
    if row is None or row[0] != canonical(expected):
        raise ValueError('尚无匹配的主控安装确认')
    return expected


def live(runtime):
    cores=([runtime.front,runtime.paced.core] if hasattr(runtime,'front') else [runtime.core])
    if any(not core.process or core.process.returncode is not None for core in cores):
        raise OSError('授予运行时已停止')


async def confirm_installed(runtime,base,token):
    """One request under lifecycle lock; failures close actual listeners.

    This is an authenticated Agent claim, not remote process attestation.
    The caller must explicitly negotiate STAGING and INSTALLATION first.
    """
    from agent import request
    storage=runtime.paced._storage if hasattr(runtime,'paced') else runtime._storage
    async with runtime.lifecycle:
        try:
            value=copy.deepcopy(runtime.grant)
            validate(value,runtime.stage.server_id,runtime.stage.session_id)
            live(runtime)
            fact=await storage(installed,runtime.stage,value)
            validate(value,runtime.stage.server_id,runtime.stage.session_id)
            live(runtime)
            response=await storage(request,base,token,'/agent/grants/installed',fact)
            if canonical(response)!=canonical(receipt(fact)):
                raise ValueError('安装回执响应身份或状态不匹配')
            validate(value,runtime.stage.server_id,runtime.stage.session_id)
            live(runtime)
            await storage(remember_ack,runtime.stage,value,response)
            validate(value,runtime.stage.server_id,runtime.stage.session_id)
            live(runtime)
            return response
        except BaseException:
            await runtime._close()
            raise
