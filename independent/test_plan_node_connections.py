import asyncio
import copy
import json
import sqlite3
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from core import Registry,Error
from plan_node_connections import PlanNodeConnections
from plan_nodes import PlanNodes
from node_config import digest,validate_snapshot
from xray_runtime import configuration
from vless_runtime import VlessRuntime
from test_connection_plans import limit,plan_data
from test_runtime import free_port
import test_core
import test_plan_nodes
import test_xray
import test_user_connections
import test_online_ips
import test_reality


class NodeConnectionTests(unittest.TestCase):
    setUp=test_core.RegistryTests.setUp
    tearDown=test_core.RegistryTests.tearDown
    fixtures=test_plan_nodes.ScopeTests.fixtures

    def test_inherit_unlimited_idempotence_reopen_and_clear(self):
        r,uid,p,nodes,tokens=self.fixtures(); r.assign_plan(uid,{'plan_id':p})
        for nid in nodes: r.authorize_node_user(nid,uid)
        manager=PlanNodeConnections(r); old=[r.configuration(t) for t in tokens]
        self.assertIsNone(manager.manage(p,nodes[0])['override'])
        value=manager.manage(p,nodes[0],limit(None)); after=r.configuration(tokens[0])
        self.assertEqual(value['effective'],limit(None)); self.assertEqual(after['revision'],old[0]['revision']+1)
        self.assertEqual(after['nodes'][0]['clients'][0]['node_connections'],limit(None))
        self.assertEqual(r.configuration(tokens[1]),old[1])
        self.assertEqual(PlanNodeConnections(Registry(r.path)).manage(p,nodes[0]),value)
        manager.manage(p,nodes[0],limit(None)); self.assertEqual(r.configuration(tokens[0]),after)
        self.assertEqual(manager.manage(p,nodes[0],inherit=True)['effective'],limit(1))
        self.assertNotIn('node_connections',r.configuration(tokens[0])['nodes'][0]['clients'][0])

    def test_equal_general_user_override_still_versions_and_plan_switch_restores(self):
        r,uid,p,nodes,tokens=self.fixtures(); r.assign_plan(uid,{'plan_id':p}); r.authorize_node_user(nodes[0],uid)
        manager=PlanNodeConnections(r); manager.manage(p,nodes[0],limit(3)); old=r.configuration(tokens[0])
        r.user_connections(uid,limit(1)); hidden=r.configuration(tokens[0])
        self.assertEqual(hidden['revision'],old['revision']+1); self.assertNotIn('node_connections',hidden['nodes'][0]['clients'][0])
        manager.manage(p,nodes[0],limit(2)); self.assertEqual(r.configuration(tokens[0]),hidden)
        r.user_connections(uid,inherit=True); restored=r.configuration(tokens[0])
        self.assertEqual(restored['revision'],hidden['revision']+1)
        self.assertEqual(restored['nodes'][0]['clients'][0]['node_connections'],limit(2))
        second=r.save_plan({**plan_data(1),'name':'second'})['id']; r.assign_plan(uid,{'plan_id':second})
        current=r.configuration(tokens[0]); self.assertGreater(current['revision'],restored['revision'])
        self.assertNotIn('node_connections',current['nodes'][0]['clients'][0])

    def test_node_unlimited_preserves_plan_ip_limit(self):
        r,uid,p,nodes,tokens=self.fixtures()
        data=plan_data(1); data['connections']['max_ips']=1; r.save_plan(data,p)
        r.assign_plan(uid,{'plan_id':p}); r.authorize_node_user(nodes[0],uid)
        value=PlanNodeConnections(r).manage(p,nodes[0],limit(None))
        self.assertEqual(value['effective'],{'max_connections':None,'max_ips':1})
        snapshot=r.configuration(tokens[0]); validate_snapshot(snapshot)
        self.assertEqual(snapshot['nodes'][0]['clients'][0]['connections']['max_ips'],1)

    def test_invalid_outside_scope_shared_node_and_revision_rollback(self):
        r,uid,p,nodes,tokens=self.fixtures(); manager=PlanNodeConnections(r)
        for data in ({},[],limit(0),limit(True),limit(-1),limit(100001),limit('2'),{'max_connections':None,'max_ips':1}):
            with self.assertRaises(Error): manager.manage(p,nodes[0],data)
        PlanNodes(r).save(p,{'node_ids':[nodes[0]]})
        with self.assertRaises(Error): manager.manage(p,nodes[1],limit(2))
        from sharing import Shares
        from test_sharing import settings
        with r.connect() as db: sid=db.execute('SELECT server_id FROM nodes WHERE id=?',(nodes[0],)).fetchone()[0]
        shares=Shares(r); invite=shares.create(sid,settings()); access=shares.claim(invite['invite_token'])['access_token']
        guest=shares.guest(access,'create',data={'name':'delegated','config':{**test_xray.VLESS,'listen_port':24502}})
        with self.assertRaises(Error): manager.manage(p,guest['id'],limit(2))
        r.assign_plan(uid,{'plan_id':p}); r.authorize_node_user(nodes[0],uid); before=r.configuration(tokens[0])
        with r.connect() as db: db.execute("CREATE TRIGGER fail_conn BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): manager.manage(p,nodes[0],limit(2))
        self.assertIsNone(manager.manage(p,nodes[0])['override']); self.assertEqual(r.configuration(tokens[0]),before)

    def test_old_database_upgrade_unused_policy_and_delete_cascade(self):
        r,uid,p,nodes,tokens=self.fixtures()
        with r.connect() as db: db.execute('DROP TABLE plan_node_connections')
        r=Registry(r.path); manager=PlanNodeConnections(r); old=r.configuration(tokens[0])
        manager.manage(p,nodes[0],limit(2)); self.assertEqual(r.configuration(tokens[0]),old)
        r.delete_plan(p)
        with r.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM plan_node_connections').fetchone()[0],0)

    def test_malformed_snapshot_and_unpaced_execution_rejected(self):
        r,uid,p,nodes,tokens=self.fixtures(); r.assign_plan(uid,{'plan_id':p}); r.authorize_node_user(nodes[0],uid)
        PlanNodeConnections(r).manage(p,nodes[0],limit(None)); good=r.configuration(tokens[0]); validate_snapshot(good)
        with self.assertRaises(ValueError): configuration(good)
        for value in (limit(0),limit(True),{'max_connections':None,'max_ips':1}):
            bad=copy.deepcopy(good); bad['nodes'][0]['clients'][0]['node_connections']=value
            bad['digest']=digest({k:v for k,v in bad.items() if k!='digest'})
            with self.assertRaises(ValueError): validate_snapshot(bad)


class NodeConnectionApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    def test_admin_crud_and_agent_auth_boundary(self):
        r=self.server.registry; sid=r.save('servers',{'name':'host'})['id']
        nid=r.save('nodes',dict(name='node',server_id=sid))['id']; r.configure_node(nid,test_xray.VLESS)
        p=r.save_plan(plan_data(1))['id']; path=f'/api/plans/{p}/nodes/{nid}/connections'; token=r.provision_agent(sid)['token']
        for method,data in (('GET',None),('PUT',limit(2)),('DELETE',None)):
            with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path,method,data,auth=False)
            self.assertEqual(caught.exception.code,401)
            req=urllib.request.Request(self.base+path,method=method,data=json.dumps(data).encode() if data else None,headers={'Authorization':'Bearer '+token})
            with self.assertRaises(urllib.error.HTTPError) as caught: self.opener.open(req,timeout=3)
            self.assertEqual(caught.exception.code,401)
        self.assertIsNone(json.load(self.request(path))['override'])
        self.assertEqual(json.load(self.request(path,'PUT',limit(None)))['effective'],limit(None))
        self.assertEqual(json.load(self.request(path,'DELETE'))['effective'],limit(1))


@unittest.skipUnless(test_xray.BINARY.is_file(),'Real Xray required')
class NodeConnectionRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_user_connections.ConnectionRealTests.setUp
    tearDown=test_user_connections.ConnectionRealTests.tearDown
    request=test_user_connections.ConnectionRealTests.request
    asyncSetUp=test_user_connections.ConnectionRealTests.asyncSetUp
    asyncTearDown=test_user_connections.ConnectionRealTests.asyncTearDown
    open=test_user_connections.ConnectionRealTests.open
    accepted=test_user_connections.ConnectionRealTests.accepted
    rejected=test_user_connections.ConnectionRealTests.rejected
    close_writer=test_user_connections.ConnectionRealTests.close_writer
    idle=test_user_connections.ConnectionRealTests.idle

    async def sync(self): await self.runtime.apply(self.server.registry.configuration(self.token))

    async def test_per_node_slots_replace_general_and_fallback_nodes_share_general(self):
        r=self.server.registry; p=r.save_plan(plan_data(1))['id']; r.assign_plan(self.uid,{'plan_id':p})
        others=[]
        for i in range(2):
            nid=r.save('nodes',dict(name=str(i),server_id=self.sid))['id']; port=free_port()
            r.configure_node(nid,{**test_xray.VLESS,'listen_port':port}); auth=r.authorize_node_user(nid,self.uid)
            others.append((auth['uuid'],port))
        manager=PlanNodeConnections(r); manager.manage(p,self.nid,limit(2)); await self.sync()
        first,w1=await self.open(); await self.accepted(first)
        second,w2=await self.open(); await self.accepted(second)
        denied,_=await self.open(); await self.rejected(denied)
        fallback,w3=await self.open(*others[0]); await self.accepted(fallback)
        denied,_=await self.open(*others[1]); await self.rejected(denied)
        await self.close_writer(w1); await self.close_writer(w2); await self.close_writer(w3); await self.idle()
        self.assertFalse(self.runtime.task_scopes)
        manager.manage(p,self.nid,limit(None)); await self.sync()
        streams=await asyncio.gather(*(self.open() for _ in range(3)))
        for reader,_ in streams: await self.accepted(reader)
        manager.manage(p,self.nid,inherit=True); await self.sync()
        for reader,_ in streams: await self.rejected(reader)
        first,_=await self.open(); await self.accepted(first)
        denied,_=await self.open(*others[0]); await self.rejected(denied)

    async def test_racing_node_handshakes_restart_and_user_override_precedence(self):
        r=self.server.registry; p=r.save_plan(plan_data(1))['id']; r.assign_plan(self.uid,{'plan_id':p})
        PlanNodeConnections(r).manage(p,self.nid,limit(2)); await self.sync()
        streams=await asyncio.gather(*(self.open() for _ in range(8)))
        async def reply(reader):
            try: return await asyncio.wait_for(reader.readexactly(7),3)
            except asyncio.IncompleteReadError as error: self.assertEqual(error.partial,b''); return b''
            except ConnectionResetError: return b''
        replies=await asyncio.gather(*(reply(reader) for reader,_ in streams))
        self.assertEqual(replies.count(b'\0\0hello'),2); self.assertEqual(replies.count(b''),6)
        await self.runtime.close(); r=Registry(r.path); self.server.registry=r
        self.runtime=VlessRuntime(test_xray.BINARY,Path(self.temp.name)/'restarted',local_test_target=('127.0.0.1',self.target_port)); await self.sync()
        for _ in range(2): reader,_=await self.open(); await self.accepted(reader)
        denied,_=await self.open(); await self.rejected(denied)
        r.user_connections(self.uid,limit(1)); await self.sync()
        reader,_=await self.open(); await self.accepted(reader)
        denied,_=await self.open(); await self.rejected(denied)
        r.user_connections(self.uid,inherit=True); await self.sync()
        for _ in range(2): reader,_=await self.open(); await self.accepted(reader)

    async def test_failed_tightening_never_restores_old_unlimited_node(self):
        import socket
        r=self.server.registry; p=r.save_plan(plan_data(1))['id']; r.assign_plan(self.uid,{'plan_id':p})
        manager=PlanNodeConnections(r); manager.manage(p,self.nid,limit(None)); await self.sync()
        reader,_=await self.open(); await self.accepted(reader); old=self.runtime.snapshot
        busy=socket.socket(); busy.bind(('127.0.0.1',0)); busy.listen(); port=busy.getsockname()[1]
        nid=r.save('nodes',dict(name='busy',server_id=self.sid))['id']; r.configure_node(nid,{**test_xray.VLESS,'listen_port':port})
        manager.manage(p,self.nid,limit(1)); current=r.configuration(self.token)
        try:
            with self.assertRaises(OSError): await self.runtime.apply(current)
            await self.rejected(reader)
            with self.assertRaises(ValueError): await self.runtime.apply(old)
        finally: busy.close()
        await self.runtime.apply(current)
        reader,_=await self.open(); await self.accepted(reader)
        denied,_=await self.open(); await self.rejected(denied)

    async def test_node_unlimited_does_not_remove_global_online_ip_limit(self):
        r=self.server.registry
        data=plan_data(1); data['connections']['max_ips']=1; p=r.save_plan(data)['id']; r.assign_plan(self.uid,{'plan_id':p})
        nid=r.save('nodes',dict(name='ipv6',server_id=self.sid))['id']; port=free_port()
        r.configure_node(nid,{**test_xray.VLESS,'listen_host':'::1','listen_port':port}); auth=r.authorize_node_user(nid,self.uid)
        manager=PlanNodeConnections(r)
        for node in (self.nid,nid): manager.manage(p,node,limit(None))
        await self.sync(); first,_=await self.open(); await self.accepted(first)
        second,_=await self.open(); await self.accepted(second)
        denied,_=await test_online_ips.OnlineIpRealTests.open(self,'::1',port,auth['uuid']); await self.rejected(denied)
        self.assertEqual(set(self.runtime.task_ips.values()),{'127.0.0.1'})


@unittest.skipUnless(test_xray.BINARY.is_file(),'Real Xray required')
class NodeConnectionRealityTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=test_reality.RealityRealTests.asyncSetUp
    asyncTearDown=test_reality.RealityRealTests.asyncTearDown
    echo=test_reality.RealityRealTests.echo
    apply=test_reality.RealityRealTests.apply
    client=test_reality.RealityRealTests.client
    exchange=test_reality.RealityRealTests.exchange

    async def test_encrypted_node_limit_refuses_excess_without_damaging_existing_stream(self):
        p=self.registry.save_plan(plan_data(None))['id']; self.registry.assign_plan(self.uid,{'plan_id':p})
        manager=PlanNodeConnections(self.registry); manager.manage(p,self.nid,limit(1)); await self.apply()
        first,writer=await self.client(); writer.write(b'first'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(first.readexactly(5),4),b'first')
        denied,blocked=await self.client(); blocked.write(b'blocked'); await blocked.drain()
        try: reply=await asyncio.wait_for(denied.read(7),4)
        except ConnectionResetError: reply=b''
        self.assertEqual(reply,b''); blocked.close(); await blocked.wait_closed()
        writer.write(b'alive'); await writer.drain(); self.assertEqual(await asyncio.wait_for(first.readexactly(5),4),b'alive')
        writer.close(); await writer.wait_closed()
        async def released():
            while self.runtime.paced.task_users: await asyncio.sleep(.01)
        await asyncio.wait_for(released(),8)
        self.assertFalse(self.runtime.paced.task_scopes)
        self.assertEqual(await self.exchange(payload=b'replaced'),b'replaced')
