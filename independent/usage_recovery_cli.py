"""Local owner-only inspection and explicit retry of quarantined usage reports.

No network request, permission repair, quota release or unknown-write adjudication.
Run against a trusted local state directory; this is not a web authorization API.
"""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import time

from usage_ledger import integer, validate
from usage_outbox import UsageOutbox


class ExistingOutbox(UsageOutbox):
    """Open existing state only; inspection must never migrate or observe it."""
    def __init__(self,path,server_id):
        if not integer(server_id,1): raise ValueError('服务器编号无效')
        original=Path(path).absolute()
        info=original.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077
                or info.st_uid!=os.geteuid()):
            raise ValueError('队列必须是当前用户拥有的私有普通文件')
        self.path=original.resolve(strict=True); self.server_id=server_id

    @contextmanager
    def connect(self,timeout=10,*,readonly=False):
        db=sqlite3.connect(self.path.as_uri()+('?mode=ro' if readonly else '?mode=rw'),
                           uri=True,timeout=timeout)
        db.row_factory=sqlite3.Row
        try:
            db.execute('PRAGMA synchronous=FULL')
            if readonly: db.execute('PRAGMA query_only=ON')
            with db: yield db
        finally: db.close()


def inspect(box,session=None,sequence=None):
    if session is not None: box._session_value(session)
    if sequence is not None and not integer(sequence,1): raise ValueError('序号无效')
    result=[]
    with box.connect(readonly=True) as db:
        db.execute('BEGIN')
        identity=db.execute('SELECT server_id FROM identity').fetchall()
        if len(identity)!=1 or identity[0][0]!=box.server_id:
            raise ValueError('流量队列服务器身份不一致')
        for row in db.execute('''SELECT f.session,f.sequence,f.body FROM failures f
                WHERE f.reason='identity_rejected'
                AND EXISTS(SELECT 1 FROM quarantine q WHERE q.session=f.session AND q.sequence=f.sequence)
                ORDER BY f.session,f.sequence'''):
            if session is not None and row['session']!=session: continue
            if sequence is not None and row['sequence']!=sequence: continue
            report=json.loads(row['body']); digest=validate(report,box)
            if len(report['items'])!=1: raise ValueError('隔离报告无效')
            item=report['items'][0]
            result.append((report,dict(session=row['session'],sequence=row['sequence'],
                node=item['node_id'],user=item['user_id'],
                credential_fingerprint=hashlib.sha256(item['credential'].encode()).hexdigest(),
                upload_bytes=item['upload_bytes'],download_bytes=item['download_bytes'],
                report_digest=digest)))
    return result


def retry(box,session,sequence,digest,reason):
    records=inspect(box,session,sequence)
    if len(records)!=1 or records[0][1]['report_digest']!=digest:
        raise ValueError('隔离报告已变化，请重新检查')
    # retry_quarantined rechecks the exact frozen body and current quarantine
    # inside BEGIN IMMEDIATE, so an intervening retry/rejection cannot slip through.
    box.retry_quarantined(records[0][0],time.time(),operator=f'uid:{os.geteuid()}',reason=reason)
    return dict(server_id=box.server_id,session=session,sequence=sequence,status='queued_for_retry')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',required=True)
    parser.add_argument('--server-id',type=int,required=True)
    sub=parser.add_subparsers(dest='action',required=True)
    sub.add_parser('inspect')
    command=sub.add_parser('retry')
    command.add_argument('--session',required=True)
    command.add_argument('--sequence',type=int,required=True)
    command.add_argument('--report-digest',required=True)
    command.add_argument('--reason',required=True)
    args=parser.parse_args()
    try:
        box=ExistingOutbox(args.database,args.server_id)
        output=([r[1] for r in inspect(box)] if args.action=='inspect' else
                retry(box,args.session,args.sequence,args.report_digest,args.reason))
        print(json.dumps(output,ensure_ascii=False,sort_keys=True))
        return 0
    except Exception as error:
        # Do not print report bodies, UUID credentials, DB paths or SQL errors.
        print(json.dumps(dict(error='local_recovery_failed',type=type(error).__name__)))
        return 1


if __name__=='__main__': raise SystemExit(main())
