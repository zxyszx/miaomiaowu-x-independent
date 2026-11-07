"""Explicit live-runtime execution audit confirmation, never mode activation.

The durable execution ID identifies one grant audit attempt, not a process PID
or remote attestation. Reopening the same immutable grant reuses that ID.
"""
from contextlib import closing
import copy
import json
import math
import sqlite3
import uuid

from grant_agent import validate
from grant_confirmation import acknowledged, canonical, installed, live


def prepare(stage, value):
    expected = installed(stage, value)
    ack = acknowledged(stage, value)
    with closing(sqlite3.connect(stage.path, timeout=0.05)) as db, db:
        db.execute('PRAGMA synchronous=FULL')
        db.execute('BEGIN IMMEDIATE')
        original = db.execute('SELECT fact FROM local_grant_installations WHERE grant_id=?',
                              (value['grant_id'],)).fetchone()
        confirmed = db.execute('SELECT receipt FROM local_grant_master_acks WHERE grant_id=?',
                               (value['grant_id'],)).fetchone()
        if (original is None or original[0] != canonical(expected)
                or confirmed is None or confirmed[0] != canonical(ack)):
            raise ValueError('执行声明缺少原安装确认')
        db.execute('CREATE TABLE IF NOT EXISTS local_grant_execution_claims('
                   'grant_id TEXT PRIMARY KEY, fact TEXT NOT NULL, receipt TEXT)')
        old = db.execute('SELECT fact FROM local_grant_execution_claims WHERE grant_id=?',
                         (value['grant_id'],)).fetchone()
        if old is not None:
            fact = json.loads(old[0])
            from grant_execution import GrantExecution
            GrantExecution._validate_fact(fact)
            if {k: v for k, v in fact.items() if k != 'execution_id'} != expected:
                raise ValueError('执行声明与原安装事实冲突')
            return fact
        fact = dict(expected, execution_id=str(uuid.uuid4()))
        db.execute('INSERT INTO local_grant_execution_claims VALUES(?,?,NULL)',
                   (value['grant_id'], canonical(fact)))
        return fact


def remember(stage, fact, response):
    if (type(response) is not dict or set(response) != {
            'accepted', 'state', 'execution', 'recorded_at', 'ready_for_execution'}
            or response['accepted'] is not True
            or response['ready_for_execution'] is not False
            or response['state'] != 'execution_recorded'
            or canonical(response['execution']) != canonical(fact)
            or type(response['recorded_at']) not in (int, float)
            or not math.isfinite(response['recorded_at'])
            or response['recorded_at'] < 0):
        raise ValueError('执行回执响应身份或状态不匹配')
    raw = canonical(response)
    with closing(sqlite3.connect(stage.path, timeout=0.05)) as db, db:
        db.execute('PRAGMA synchronous=FULL')
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT fact,receipt FROM local_grant_execution_claims WHERE grant_id=?',
                         (fact['grant_id'],)).fetchone()
        if row is None or row[0] != canonical(fact):
            raise ValueError('执行确认缺少原持久声明')
        if row[1] is not None and row[1] != raw:
            raise ValueError('不能覆盖原执行确认')
        db.execute('UPDATE local_grant_execution_claims SET receipt=? WHERE grant_id=?',
                   (raw, fact['grant_id']))


async def confirm_execution(runtime, base, token):
    """One authenticated request, with durable identity before transmission.

    Master lifecycle must already be installed. Failure closes listeners but
    preserves the claim, quota and unknown writes for an exact explicit retry.
    """
    from agent import request
    storage = runtime.paced._storage if hasattr(runtime, 'paced') else runtime._storage
    async with runtime.lifecycle:
        try:
            value = copy.deepcopy(runtime.grant)
            def check():
                validate(value, runtime.stage.server_id, runtime.stage.session_id)
                live(runtime)
            check()
            fact = await storage(prepare, runtime.stage, value)
            check()
            response = await storage(request, base, token,
                                     '/agent/grants/execution', {'fact': fact})
            check()
            await storage(remember, runtime.stage, fact, response)
            check()
            return response
        except BaseException:
            await runtime._close()
            raise
