"""Explicit, session-scoped protocol negotiation, never execution evidence.

Durable staging and explicit installation claims are implemented. Automatic
execution versions are never selected. Existing Agents do not auto-advertise
these capabilities, and negotiation cannot write an applied grant.
"""
import hashlib
import json
from core import Error

SCHEMA='''
CREATE TABLE IF NOT EXISTS grant_capabilities(
 server_id INTEGER PRIMARY KEY REFERENCES agents(server_id) ON DELETE CASCADE,
 session_id TEXT NOT NULL,token_hash TEXT NOT NULL,offer TEXT NOT NULL);
'''
STAGING='independent.total-grant.staging.v1'
STAGE_IDENTITY='independent.total-grant.stage-identity.v1'
INSTALLATION='independent.total-grant.installation.v1'
SUPPORTED=(STAGING,STAGE_IDENTITY,INSTALLATION)


def protocols(value):
    if type(value) is not list or len(value)>8:
        raise Error('授予协议列表无效')
    if any(type(v) is not str or not 1<=len(v)<=96 or
           any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789.-' for c in v) for v in value):
        raise Error('授予协议名称无效')
    if len(set(value))!=len(value): raise Error('授予协议不可重复')
    return sorted(value)


def receipt(server_id,session,offer,*,isolated=False):
    supported=SUPPORTED
    if isolated:
        from multi_agent_lifecycle import MULTI_EXECUTION
        from multi_agent_tail import TAIL
        supported=(*SUPPORTED,MULTI_EXECUTION,TAIL)
    return dict(accepted=True,server_id=server_id,session_id=session,
                offered=offer,selected=[p for p in offer if p in supported],
                execution_ready=False)


class GrantCapabilities:
    def __init__(self,registry): self.registry=registry

    def negotiate(self,token,data):
        if type(data) is not dict or set(data)!={'session_id','protocols'}:
            raise Error('授予协议协商字段无效')
        r=self.registry; session=r._session_value(data['session_id'])
        offer=protocols(data['protocols']); raw=json.dumps(offer,separators=(',',':'))
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            sid=r._agent_id(db,token); r._check_session(db,sid,session)
            fingerprint=hashlib.sha256(token.encode()).hexdigest()
            old=db.execute('SELECT * FROM grant_capabilities WHERE server_id=?',(sid,)).fetchone()
            if old is not None and old['session_id']==session and old['token_hash']==fingerprint:
                if old['offer']!=raw: raise Error('当前会话协议不可变，请启动新会话',409)
            else:
                db.execute('INSERT OR REPLACE INTO grant_capabilities VALUES(?,?,?,?)',
                           (sid,session,fingerprint,raw))
            from multi_agent_test_runtime import gate_for
            return receipt(sid,session,offer,isolated=gate_for(r) is not None)


def negotiate(base,token,server_id,session,offered):
    """One explicit request; caller supplies actual implemented capabilities."""
    from agent import request
    if type(server_id) is not int or server_id<=0: raise ValueError('Agent编号无效')
    offer=protocols(offered)
    expected=receipt(server_id,session,offer)
    response=request(base,token,'/agent/grants/capabilities',dict(session_id=session,protocols=offer))
    # Exact keys/types: bool must not masquerade as an integer server identity.
    if (type(response) is not dict or set(response)!=set(expected) or
        type(response.get('server_id')) is not int or response.get('accepted') is not True or
        response.get('execution_ready') is not False or response!=expected):
        raise ValueError('主控授予协议回执不匹配')
    return response
