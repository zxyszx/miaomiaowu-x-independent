"""Atomic content CAS; all databases and credentials are temporary."""
import concurrent.futures
import json
import threading
import urllib.error
import urllib.request
import unittest
from unittest import mock
import http.client
from urllib.parse import urlsplit
from core import Registry, Error
from plan_node_bandwidth import PlanNodeBandwidth
from plan_nodes import PlanNodes
import test_core
import test_xray
from test_accounts import plan
from test_plan_node_bandwidth import rates

class ConditionalHTTPTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown

    def fixture(self):
        r=self.server.registry
        sid=r.save('servers',{'name':'temporary'})['id']
        nid=r.save('nodes',dict(name='temporary',server_id=sid))['id']
        r.configure_node(nid,test_xray.VLESS);pid=plan(r)
        self.path=f'/api/plans/{pid}/nodes/{nid}/bandwidth'
        return r,sid,pid,nid

    def call(self,method='GET',data=None,version=None,token=None):
        headers={'Authorization':'Bearer '+(self.server.token if token is None else token)}
        if version is not None:headers['If-Match']=version
        req=urllib.request.Request(self.base+self.path,method=method,headers=headers,
            data=json.dumps(data).encode() if data is not None else None)
        try:
            with self.opener.open(req,timeout=5) as response:return response.status,json.load(response)
        except urllib.error.HTTPError as error:return error.code,json.load(error)

    def test_simultaneous_puts_have_one_winner_and_persist(self):
        r,sid,pid,nid=self.fixture()
        uid=r.save('users',{'name':'executing'})['id'];r.assign_plan(uid,{'plan_id':pid});r.authorize_node_user(nid,uid)
        with r.connect() as db:before=db.execute('SELECT revision FROM server_configs WHERE server_id=?',(sid,)).fetchone()[0]
        version=self.call()[1]['version'];gate=threading.Barrier(2)
        def write(rate):gate.wait();return self.call('PUT',rates(rate),version)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(write,[125000,250000]))
        self.assertEqual(sorted(status for status,_ in results),[200,412])
        winner=next(body for status,body in results if status==200)
        self.assertEqual(self.call()[1],winner)
        self.assertEqual(PlanNodeBandwidth(Registry(r.path)).manage(pid,nid,versioned=True),winner)
        with r.connect() as db:self.assertEqual(db.execute('SELECT revision FROM server_configs WHERE server_id=?',(sid,)).fetchone()[0],before+1)

    def test_put_delete_race_and_stale_zero_revision(self):
        r,sid,pid,nid=self.fixture();self.call('PUT',rates(1));version=self.call()[1]['version'];gate=threading.Barrier(2)
        def write(method):gate.wait();return self.call(method,rates(2) if method=='PUT' else None,version)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(write,['PUT','DELETE']))
        self.assertEqual(sorted(s for s,_ in results),[200,412])
        before=self.call()[1]
        with r.connect() as db:revision=db.execute('SELECT revision FROM server_configs WHERE server_id=?',(sid,)).fetchone()[0]
        self.assertEqual(self.call('DELETE',version=version)[0],412)
        self.assertEqual(self.call()[1],before)
        with r.connect() as db:self.assertEqual(db.execute('SELECT revision FROM server_configs WHERE server_id=?',(sid,)).fetchone()[0],revision)

    def test_legacy_write_content_aba_and_idempotence(self):
        r,sid,pid,nid=self.fixture();original=self.call()[1]['version']
        first=self.call('PUT',rates(1),original);self.assertEqual(first[0],200)
        self.assertEqual(self.call('PUT',rates(1),first[1]['version'])[1]['version'],first[1]['version'])
        self.assertEqual(self.call('PUT',rates(2),original)[0],412)
        self.assertEqual(self.call('DELETE')[0],200) # legacy API remains unconditional
        self.assertEqual(self.call()[1]['version'],original) # documented content ABA
        self.assertEqual(self.call('PUT',rates(3),original)[0],200)

    def test_inherited_config_scope_and_unrelated_resource_versions(self):
        r,sid,pid,nid=self.fixture();v=self.call()[1]['version']
        other=plan(r,'unrelated');PlanNodeBandwidth(r).manage(other,nid,rates(1))
        self.assertEqual(self.call()[1]['version'],v)
        r.save_plan(dict(name='temporary',bandwidth=rates(2)),pid)
        self.assertEqual(self.call('PUT',rates(3),v)[0],412);v=self.call()[1]['version']
        r.configure_node(nid,{**test_xray.VLESS,'listen_port':24581})
        self.assertEqual(self.call('DELETE',version=v)[0],412);v=self.call()[1]['version']
        PlanNodes(r).save(pid,{'node_ids':[nid]})
        self.assertEqual(self.call('PUT',rates(3),v)[0],412)
        fresh=self.call()[1]['version'];PlanNodes(r).save(pid,{'node_ids':[]})
        self.assertEqual(self.call('PUT',rates(3),fresh)[0],403)
        self.assertEqual(self.call('DELETE',version=fresh)[0],412)

    def test_bad_conditions_auth_and_body_never_write(self):
        r,sid,pid,nid=self.fixture();v=self.call()[1]['version']
        for bad in ['', '*','W/'+v,v+', '+v,'"bw1-'+('A'*64)+'"']:
            self.assertEqual(self.call('PUT',rates(1),bad)[0],400)
        self.assertEqual(self.call('PUT',rates(0),v)[0],400)
        self.assertEqual(self.call('DELETE',version=v,token=r.provision_agent(sid)['token'])[0],401)
        connection=http.client.HTTPConnection(urlsplit(self.base).hostname,urlsplit(self.base).port,timeout=5)
        try:
            connection.putrequest('DELETE',self.path)
            connection.putheader('Authorization','Bearer '+self.server.token)
            connection.putheader('If-Match',v);connection.putheader('If-Match',v);connection.endheaders()
            response=connection.getresponse();self.assertEqual(response.status,400);response.read()
        finally:connection.close()
        self.assertIsNone(self.call()[1]['override'])

    def test_publish_failure_rolls_back_value_and_version(self):
        r,sid,pid,nid=self.fixture()
        # Execute the plan/node so policy changes really invoke existing publication.
        uid=r.save('users',{'name':'temporary'})['id'];r.assign_plan(uid,{'plan_id':pid});r.authorize_node_user(nid,uid)
        policy=PlanNodeBandwidth(r);before=policy.manage(pid,nid,versioned=True)
        with mock.patch.object(r,'_bump',side_effect=RuntimeError('fixture publish failure')):
            with self.assertRaises(RuntimeError):policy.manage(pid,nid,rates(5),if_match=before['version'],versioned=True)
        self.assertEqual(policy.manage(pid,nid,versioned=True),before)
