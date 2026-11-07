"""Read-only local export of uncertain writes and checkpoint resolution evidence.

This is an evidence snapshot, not a settlement receipt, signed proof, database
backup, or authorization to remove records or adjust quota/billing.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile


def fingerprint(credential):
    if not isinstance(credential,str): raise ValueError('审计凭据格式无效')
    return hashlib.sha256(credential.encode()).hexdigest()


def write_records(output,rows):
    count=0
    for row in rows:
        item=dict(row)
        item['credential_fingerprint']=fingerprint(item.pop('credential'))
        if count: output.write(',\n')
        json.dump(item,output,ensure_ascii=False,sort_keys=True,allow_nan=False)
        count+=1
    return count


def export_evidence(database,server_id,destination):
    """Export one SQLite read transaction; exclusively publish a private file.

    Does not instantiate UsageOutbox (which migrates/observes on opening).
    Incomplete files are temporary and removed; an existing destination,
    including a symlink, is never replaced. Existing evidence is never pruned.
    """
    if type(server_id) is not int or server_id<1:
        raise ValueError('流量服务器编号无效')
    source=Path(database).resolve(strict=True)
    destination=Path(destination).absolute()
    temporary=None
    try:
        # URI escaping protects filenames containing ?, #, or Unicode. mode=ro
        # plus query_only prevents migration, observation and data mutations.
        db=sqlite3.connect(source.as_uri()+'?mode=ro',uri=True,timeout=.05)
        try:
            db.row_factory=sqlite3.Row
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            identity=db.execute('SELECT server_id FROM identity').fetchall()
            if len(identity)!=1 or identity[0][0]!=server_id:
                raise ValueError('流量队列服务器身份不一致')
            fd,name=tempfile.mkstemp(prefix='.usage-evidence-',dir=destination.parent)
            temporary=Path(name)
            with os.fdopen(fd,'w',encoding='utf-8',newline='\n') as output:
                output.write('{"format":"independent-usage-evidence","schema":1,')
                output.write('"server_id":'+str(server_id)+',"exported_at":')
                json.dump(datetime.now(timezone.utc).isoformat(),output)
                output.write(',"unresolved":[\n')
                unresolved=write_records(output,db.execute('''
                    SELECT w.session,w.node,w.user,w.credential,
                        COALESCE(t.upload,0) AS committed_upload,
                        COALESCE(t.download,0) AS committed_download,
                        w.upload AS possible_upload,w.download AS possible_download,
                        r.lower_upload AS observed_lower_upload,
                        r.lower_download AS observed_lower_download
                    FROM write_intents w LEFT JOIN totals t ON
                        w.session=t.session AND w.node=t.node AND w.user=t.user AND w.credential=t.credential
                    LEFT JOIN recovered_writes r ON
                        w.session=r.session AND w.node=r.node AND w.user=r.user AND w.credential=r.credential
                        AND w.upload=r.target_upload AND w.download=r.target_download
                    ORDER BY w.session,w.node,w.user,w.credential'''))
                output.write('],"resolutions":[\n')
                resolved=write_records(output,db.execute('''SELECT session,node,user,credential,
                    lower_upload,lower_download,target_upload,target_download,
                    checkpoint_upload,checkpoint_download,recorded_at,
                    'successful_checkpoint' AS source FROM write_resolutions
                    ORDER BY session,node,user,credential,target_upload,target_download'''))
                output.write(']}\n')
                output.flush(); os.fsync(output.fileno())
            db.rollback()
        finally:
            db.close()
        # Hard-link publication is atomic and fails if destination already
        # exists, unlike replace/rename. Both names are on the same filesystem.
        os.link(temporary,destination)
        temporary.unlink(); temporary=None
        directory=os.open(destination.parent,os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(directory)
        finally: os.close(directory)
        digest=hashlib.sha256()
        with destination.open('rb') as saved:
            for block in iter(lambda:saved.read(65536),b''): digest.update(block)
        return dict(server_id=server_id,unresolved=unresolved,resolutions=resolved,
                    sha256=digest.hexdigest())
    finally:
        if temporary is not None: temporary.unlink(missing_ok=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',required=True)
    parser.add_argument('--server-id',type=int,required=True)
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    try:
        result=export_evidence(args.database,args.server_id,args.output)
    except (OSError,sqlite3.Error,ValueError) as error:
        parser.exit(1,'导出失败（'+type(error).__name__+'）；原队列未修改，已有目标文件不会覆盖。\n')
    print(json.dumps(result,sort_keys=True))


if __name__=='__main__': main()
