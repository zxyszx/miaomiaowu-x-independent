"""Online IP accounting is independent from the TCP connection count."""
import asyncio
import copy
import json
import socket
import unittest
import urllib.error
import uuid

from core import Registry
from node_config import digest, validate_snapshot
from xray_runtime import configuration
from test_runtime import free_port
import test_connection_plans
import test_core
import test_user_connections
import test_xray


def rule(ips=1,connections=None): return {'max_connections':connections,'max_ips':ips}


class OnlineIpApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request
    fixtures=test_connection_plans.ConnectionPlanTests.fixtures

    def test_plan_inheritance_override_clear_reopen_and_snapshot(self):
        r,uid,token=self.fixtures()
        data=test_connection_plans.plan_data(None); data['connections']=rule()
        plan=json.load(self.request('/api/plans','POST',data))
        r.assign_plan(uid,{'plan_id':plan['id']})
        before=r.configuration(token)
        self.assertEqual(before['nodes'][0]['clients'][0]['connections'],rule())
        validate_snapshot(before)
        with self.assertRaises(ValueError): configuration(before)
        path=f'/api/users/{uid}/connections'
        json.load(self.request(path,'PUT',{'max_connections':None,'max_ips':None}))
        self.assertNotIn('connections',r.configuration(token)['nodes'][0]['clients'][0])
        self.assertEqual(Registry(r.path).user_connections(uid),{'max_connections':None})
        self.assertEqual(json.load(self.request(path,'DELETE')),rule())
        self.assertEqual(Registry(r.path).configuration(token)['nodes'][0]['clients'][0]['connections'],rule())
        r.user_connections(uid,rule(2,3))
        self.assertEqual(r.user_connections(uid),rule(2,3))

    def test_invalid_ip_limits_and_inconsistent_snapshot_rejected(self):
        r,uid,token=self.fixtures()
        for value in (0,-1,True,'1',1.5,100001):
            with self.subTest(value=value),self.assertRaises(urllib.error.HTTPError) as caught:
                self.request(f'/api/users/{uid}/connections','PUT',rule(value))
            self.assertEqual(caught.exception.code,400)
        self.assertEqual(r.user_connections(uid),{'max_connections':None})
        sid=r.configuration(token)['server_id']
        node=r.save('nodes',{'name':'other','server_id':sid})['id']
        r.configure_node(node,{**test_xray.VLESS,'listen_port':24001}); r.authorize_node_user(node,uid)
        r.user_connections(uid,rule())
        bad=copy.deepcopy(r.configuration(token)); bad['nodes'][1]['clients'][0]['connections']=rule(2)
        bad['digest']=digest({k:v for k,v in bad.items() if k!='digest'})
        with self.assertRaisesRegex(ValueError,'连接限制不一致'): validate_snapshot(bad)


@unittest.skipUnless(test_xray.BINARY.is_file(),'Real Xray required')
class OnlineIpRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_user_connections.ConnectionRealTests.setUp
    tearDown=test_user_connections.ConnectionRealTests.tearDown
    request=test_user_connections.ConnectionRealTests.request
    asyncSetUp=test_user_connections.ConnectionRealTests.asyncSetUp
    asyncTearDown=test_user_connections.ConnectionRealTests.asyncTearDown
    accepted=test_user_connections.ConnectionRealTests.accepted
    rejected=test_user_connections.ConnectionRealTests.rejected
    close_writer=test_user_connections.ConnectionRealTests.close_writer

    async def open(self,host='127.0.0.1',port=None,credential=None):
        reader,writer=await asyncio.open_connection(host,port or self.port)
        self.addAsyncCleanup(self.close_writer,writer)
        header=b'\0'+uuid.UUID(credential or self.credential['uuid']).bytes+b'\0\1'+self.target_port.to_bytes(2,'big')+b'\1\x7f\0\0\1'
        writer.write(header+b'hello'); await writer.drain()
        return reader,writer

    async def dual(self):
        r=self.server.registry
        sid=r.configuration(self.token)['server_id']
        nid=r.save('nodes',{'name':'ipv6','server_id':sid})['id']; port=free_port()
        r.configure_node(nid,{**test_xray.VLESS,'listen_host':'::1','listen_port':port})
        credential=r.authorize_node_user(nid,self.uid)['uuid']
        r.user_connections(self.uid,rule())
        await self.runtime.apply(r.configuration(self.token))
        return port,credential

    async def test_same_ip_shares_slot_cross_node_different_ip_rejected_then_released(self):
        port,credential=await self.dual()
        first,w1=await self.open(); await self.accepted(first)
        second,w2=await self.open(); await self.accepted(second)
        blocked,_=await self.open('::1',port,credential); await self.rejected(blocked)
        await self.close_writer(w1)
        # Half-close/cleanup must not release the IP still used by another TCP connection.
        blocked,_=await self.open('::1',port,credential); await self.rejected(blocked)
        await self.close_writer(w2)
        async def released():
            while self.runtime.task_users: await asyncio.sleep(.01)
        await asyncio.wait_for(released(),3)
        third,_=await self.open('::1',port,credential); await self.accepted(third)
        blocked,_=await self.open(); await self.rejected(blocked)
        self.assertEqual(set(self.runtime.task_ips.values()),{'::1'})

    async def test_two_addresses_race_for_one_slot_without_oversubscription(self):
        port,credential=await self.dual()
        streams=await asyncio.gather(*(self.open() if i%2==0 else self.open('::1',port,credential) for i in range(8)))
        async def outcome(reader):
            try: return await asyncio.wait_for(reader.readexactly(7),3)
            except asyncio.IncompleteReadError as error:
                self.assertEqual(error.partial,b'')
                return b''
            except ConnectionResetError: return b''
        outcomes=await asyncio.gather(*(outcome(reader) for reader,_ in streams))
        self.assertEqual(outcomes.count(b'\0\0hello'),4)
        self.assertEqual(outcomes.count(b''),4)
        self.assertEqual(len(set(self.runtime.task_ips.values())),1)

    async def test_failed_tightening_closes_old_ip_slots_and_rejects_replay(self):
        r=self.server.registry
        await self.runtime.apply(r.configuration(self.token))
        old=self.runtime.snapshot
        reader,_=await self.open(); await self.accepted(reader)
        busy=socket.socket(); busy.bind(('127.0.0.1',0)); busy.listen()
        sid=old['server_id']
        node=r.save('nodes',{'name':'busy','server_id':sid})['id']
        r.configure_node(node,{**test_xray.VLESS,'listen_port':busy.getsockname()[1]})
        r.user_connections(self.uid,rule())
        current=r.configuration(self.token)
        try:
            with self.assertRaises(OSError): await self.runtime.apply(current)
            await self.rejected(reader)
            self.assertEqual(self.runtime.task_ips,{})
            with self.assertRaises(ValueError): await self.runtime.apply(old)
        finally: busy.close()
        await self.runtime.apply(current)
        first,_=await self.open(); await self.accepted(first)

    async def test_different_users_independent_and_combined_tcp_limit(self):
        port,credential=await self.dual()
        r=self.server.registry
        other=r.save('users',{'name':'other'})['id']
        ipv6_node=next(n['id'] for n in r.configuration(self.token)['nodes'] if n['config']['listen_host']=='::1')
        other_credential=r.authorize_node_user(ipv6_node,other)['uuid']
        r.user_connections(other,rule())
        r.user_connections(self.uid,rule(1,2))
        await self.runtime.apply(r.configuration(self.token))
        first,_=await self.open(); await self.accepted(first)
        second,_=await self.open(); await self.accepted(second)
        too_many,_=await self.open(); await self.rejected(too_many)
        different_ip,_=await self.open('::1',port,credential); await self.rejected(different_ip)
        independent,_=await self.open('::1',port,other_credential); await self.accepted(independent)
        await self.runtime.close()
        self.assertEqual(self.runtime.task_ips,{})
        self.assertEqual(self.runtime.task_users,{})
