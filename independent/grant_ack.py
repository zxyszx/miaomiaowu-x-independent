"""Authenticated installation ACK, distinct from staging and usage delivery.

This records an authenticated Agent claim, not remote attestation or perpetual
liveness. The explicit client checks live runtimes and durable installation
before sending it. Automatic installation and grant usage delivery remain off.
"""
import json
import math
from core import Error
from grant_capabilities import INSTALLATION, protocols
from grant_configuration import GrantConfiguration
from grant_snapshot import snapshot_for

SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_first_total_installations(
 grant_id TEXT PRIMARY KEY, fact TEXT NOT NULL);
'''
FIELDS={'grant_id','digest','runtime_digest','protocol','server_id','session_id',
        'revision','valid_until'}


def fact_for(value):
    return dict(grant_id=value['grant_id'],digest=value['digest'],
        runtime_digest=snapshot_for(value)['digest'],
        protocol='reality' if 'reality' in value['node_config'] else 'vless',
        server_id=value['server_id'],session_id=value['session_id'],
        revision=value['revision'],valid_until=value['valid_until'])


def receipt(fact):
    return dict(accepted=True,state='installation_recorded',installation=fact,
                ready_for_execution=False)


class GrantAcknowledgement:
    def __init__(self,registry): self.registry=registry

    def accept(self,token,fact):
        r=self.registry
        if type(fact) is not dict or set(fact)!=FIELDS:
            raise Error('安装回执字段无效')
        r._session_value(fact['grant_id']);r._session_value(fact['session_id'])
        if (type(fact['server_id']) is not int or type(fact['revision']) is not int
                or type(fact['valid_until']) not in (int,float)
                or (type(fact['valid_until']) is float and not math.isfinite(fact['valid_until']))):
            raise Error('安装回执字段类型无效')
        for key in ('digest','runtime_digest'):
            if type(fact[key]) is not str or len(fact[key])!=64 or any(c not in '0123456789abcdef' for c in fact[key]):
                raise Error('安装回执摘要无效')
        if fact['protocol'] not in ('vless','reality'): raise Error('安装协议无效')
        encoded=json.dumps(fact,sort_keys=True,separators=(',',':'),allow_nan=False)
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            sid=r._agent_id(db,token)
            r._check_session(db,sid,fact['session_id'])
            g=db.execute('SELECT * FROM usage_first_total_grants WHERE grant_id=?',(fact['grant_id'],)).fetchone()
            fingerprint=db.execute('SELECT token_hash FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
            if (g is None or g['server_id']!=sid or fact['server_id']!=sid
                    or g['session_id']!=fact['session_id'] or g['token_hash']!=fingerprint):
                raise Error('安装回执身份不匹配',403)
            cap=db.execute('SELECT * FROM grant_capabilities WHERE server_id=?',(sid,)).fetchone()
            if (cap is None or cap['session_id']!=fact['session_id'] or cap['token_hash']!=fingerprint
                    or INSTALLATION not in protocols(json.loads(cap['offer']))):
                raise Error('当前会话未协商安装回执协议',409)
            old=db.execute('SELECT fact FROM usage_first_total_installations WHERE grant_id=?',(g['grant_id'],)).fetchone()
            applied=db.execute('SELECT * FROM usage_first_total_applied WHERE grant_id=?',(g['grant_id'],)).fetchone()
            if old is not None:
                # Historical retry neither extends the lease nor regrants bytes.
                # Authenticate again, but do not rebind the now consumed baseline.
                if (old[0]!=encoded or applied is None or applied['config_version']!=fact['revision']
                        or applied['config_digest']!=fact['digest'] or applied['valid_until']!=fact['valid_until']):
                    raise Error('安装回执与原记录冲突',409)
                return receipt(fact)
            if applied is not None: raise Error('缺少原安装回执，不能补造',409)
            if db.execute('SELECT 1 FROM usage_first_total_configurations WHERE grant_id=?',(g['grant_id'],)).fetchone() is None:
                raise Error('授予尚未领取配置',409)
            value=GrantConfiguration(r)._bind(db,token,g['grant_id'],
                session_id=fact['session_id'],require_staging=True)
            expected=fact_for(value)
            # Canonical equality rejects bool/number and alternate encoding tricks.
            if encoded!=json.dumps(expected,sort_keys=True,separators=(',',':'),allow_nan=False):
                raise Error('安装配置摘要或策略不匹配',409)
            db.execute('INSERT INTO usage_first_total_installations VALUES(?,?)',(g['grant_id'],encoded))
            db.execute('INSERT INTO usage_first_total_applied VALUES(?,?,?,?)',
                (g['grant_id'],fact['revision'],fact['digest'],fact['valid_until']))
            return receipt(fact)
