"""Per-user connection admission must be shared across node credentials."""
import asyncio
import copy
import json
from pathlib import Path
import unittest
from unittest import mock
import urllib.error
import urllib.request
import uuid

from core import Registry
from node_config import digest, validate_snapshot
from xray_runtime import configuration
from vless_runtime import VlessRuntime
from test_runtime import free_port
import test_core
import test_xray
import test_user_bandwidth


class ConnectionApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    def test_validation_auth_and_no_unintended_mutation(self):
        registry=self.server.registry
        uid=registry.save('users',{'name':'fixture'})['id']
        path=f'/api/users/{uid}/connections'
        for value in (None,{},[],{'max_connections':True},{'max_connections':0},
                      {'max_connections':-1},{'max_connections':1.5},
                      {'max_connections':'2'},{'max_connections':100001},
                      {'max_connections':1,'max_devices':1}):
            with self.subTest(value=value),self.assertRaises(urllib.error.HTTPError) as caught:
                self.request(path,'PUT',value)
            self.assertEqual(caught.exception.code,413 if value is None else 400)
        for method,data in (('GET',None),('PUT',{'max_connections':1})):
            with self.assertRaises(urllib.error.HTTPError) as caught:
                self.request(path,method,data,auth=False)
            self.assertEqual(caught.exception.code,401)
        self.assertEqual(json.load(self.request(path)),{'max_connections':None})
        sid=registry.save('servers',{'name':'agent-only'})['id']
        token=registry.provision_agent(sid)['token']
        for method in ('GET','PUT'):
            req=urllib.request.Request(self.base+path,method=method,
                headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'},
                data=b'{"max_connections":1}' if method=='PUT' else None)
            with self.assertRaises(urllib.error.HTTPError) as caught:
                self.opener.open(req,timeout=3)
            self.assertEqual(caught.exception.code,401)
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.request('/api/users/999/connections')
        self.assertEqual(caught.exception.code,404)

    def test_persistence_version_scope_idempotence_reset_and_delete(self):
        registry=self.server.registry
        uid=registry.save('users',{'name':'one'})['id']
        other=registry.save('users',{'name':'other'})['id']
        tokens=[]
        for index in range(3):
            sid=registry.save('servers',{'name':str(index)})['id']
            tokens.append(registry.provision_agent(sid)['token'])
            for number in range(2):
                nid=registry.save('nodes',{'name':str(number),'server_id':sid})['id']
                registry.configure_node(nid,{**test_xray.VLESS,'listen_port':24000+number})
                registry.authorize_node_user(nid,uid if index<2 else other)
        before=[registry.configuration(t) for t in tokens]
        policy={'max_connections':2}
        self.assertEqual(json.load(self.request(f'/api/users/{uid}/connections','PUT',policy)),policy)
        after=[registry.configuration(t) for t in tokens]
        for old,new in zip(before[:2],after[:2]):
            self.assertEqual(new['revision'],old['revision']+1)
            self.assertTrue(all(n['clients'][0]['connections']==policy for n in new['nodes']))
            validate_snapshot(new)
            with self.assertRaises(ValueError): configuration(new)
        self.assertEqual(before[2],after[2])
        registry.user_connections(uid,policy)
        restored=Registry(registry.path)
        self.assertEqual(restored.user_connections(uid),policy)
        self.assertEqual([restored.configuration(t) for t in tokens],after)
        registry.user_connections(uid,{'max_connections':None})
        self.assertNotIn('connections',registry.configuration(tokens[0])['nodes'][0]['clients'][0])
        registry.user_connections(uid,policy)
        registry.delete('users',uid)
        with registry.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM user_connections').fetchone()[0],0)

    def test_inconsistent_or_invalid_snapshot_limits_are_rejected(self):
        registry=self.server.registry
        sid=registry.save('servers',{'name':'one'})['id']
        uid=registry.save('users',{'name':'one'})['id']
        token=registry.provision_agent(sid)['token']
        for index in range(2):
            nid=registry.save('nodes',{'name':str(index),'server_id':sid})['id']
            registry.configure_node(nid,{**test_xray.VLESS,'listen_port':24000+index})
            registry.authorize_node_user(nid,uid)
        for policy in ({'max_connections':True},{'max_connections':1}):
            value=copy.deepcopy(registry.configuration(token))
            value['nodes'][0]['clients'][0]['connections']=policy
            value['digest']=digest({k:v for k,v in value.items() if k!='digest'})
            with self.assertRaises(ValueError): validate_snapshot(value)

    def test_revision_failure_rolls_back_saved_policy(self):
        registry=self.server.registry
        sid=registry.save('servers',{'name':'one'})['id']
        uid=registry.save('users',{'name':'one'})['id']
        nid=registry.save('nodes',{'name':'one','server_id':sid})['id']
        registry.configure_node(nid,test_xray.VLESS)
        registry.authorize_node_user(nid,uid)
        token=registry.provision_agent(sid)['token']
        before=registry.configuration(token)
        with registry.connect() as db:
            db.execute("CREATE TRIGGER block_bump BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture failure'); END")
        import sqlite3
        with self.assertRaises(sqlite3.IntegrityError):
            registry.user_connections(uid,{'max_connections':1})
        self.assertEqual(registry.user_connections(uid),{'max_connections':None})
        self.assertEqual(registry.configuration(token),before)


@unittest.skipUnless(test_xray.BINARY.is_file(),'Real Xray required')
class ConnectionRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_xray.XrayRealTests.setUp
    tearDown=test_xray.XrayRealTests.tearDown
    request=test_xray.XrayRealTests.request
    asyncSetUp=test_user_bandwidth.UserBandwidthRealTests.asyncSetUp
    asyncTearDown=test_xray.XrayRealTests.asyncTearDown

    async def open(self,credential=None,port=None):
        reader,writer=await asyncio.open_connection('127.0.0.1',port or self.port)
        self.addAsyncCleanup(self.close_writer,writer)
        header=b'\0'+uuid.UUID(credential or self.credential['uuid']).bytes+b'\0\1'+self.target_port.to_bytes(2,'big')+b'\1\x7f\0\0\1'
        writer.write(header+b'hello'); await writer.drain()
        return reader,writer

    async def close_writer(self,writer):
        writer.close()
        try: await writer.wait_closed()
        except OSError: pass

    async def accepted(self,reader):
        self.assertEqual(await asyncio.wait_for(reader.readexactly(7),3),b'\0\0hello')

    async def rejected(self,reader):
        # A timeout is a failure, not proof of refusal.
        try: result=await asyncio.wait_for(reader.read(7),3)
        except ConnectionResetError: result=b''
        self.assertEqual(result,b'')

    async def idle(self):
        async def wait():
            while self.runtime.task_users: await asyncio.sleep(.01)
        await asyncio.wait_for(wait(),3)

    async def test_queued_old_listener_cannot_bypass_reloaded_connection_limit(self):
        await self.apply(None)
        entered=asyncio.Event(); release=asyncio.Event()
        serve=self.runtime._serve
        pending=[]
        async def delayed(entry,reader,writer):
            pending.append(asyncio.current_task())
            entered.set()
            try:
                await release.wait()
                await serve(entry,reader,writer)
            finally:
                await self.close_writer(writer)
        try:
            # Model a callback already accepted by the old listener but not yet
            # entered into the runtime's tracked connection set.
            with mock.patch.object(self.runtime,'_serve',side_effect=delayed):
                old_reader,_=await self.open()
                await asyncio.wait_for(entered.wait(),3)
            self.assertFalse(self.runtime.tasks)
            await asyncio.wait_for(self.apply(1),3)
            fresh_reader,_=await self.open()
            await self.accepted(fresh_reader)
            release.set()
            await self.rejected(old_reader)
            self.assertEqual(len(self.runtime.task_users),1)
        finally:
            release.set()
            for task in pending: task.cancel()
            await asyncio.gather(*pending,return_exceptions=True)

    async def test_accepted_callback_cancelled_before_first_turn_closes_writer(self):
        writer=mock.Mock()
        with mock.patch.object(self.runtime,'_serve',new_callable=mock.AsyncMock) as serve:
            self.runtime._accept(None,None,writer)
            self.assertEqual(len(self.runtime.accept_tasks),1)
            await self.runtime._cancel_connections()
            serve.assert_not_awaited()
        writer.close.assert_called_once()
        self.assertFalse(self.runtime.accept_tasks)
        self.assertFalse(self.runtime.tasks)

    async def apply(self,maximum):
        self.server.registry.user_connections(self.uid,{'max_connections':maximum})
        await self.runtime.apply(self.server.registry.configuration(self.token))

    async def test_shared_across_nodes_release_and_other_user_unaffected(self):
        registry=self.server.registry
        nid=registry.save('nodes',{'name':'two','server_id':self.sid})['id']
        port=free_port(); registry.configure_node(nid,{**test_xray.VLESS,'listen_port':port})
        second=registry.authorize_node_user(nid,self.uid)['uuid']
        other=registry.save('users',{'name':'unlimited'})['id']
        other_credential=registry.authorize_node_user(self.nid,other)['uuid']
        await self.apply(1)
        first,writer=await self.open(); await self.accepted(first)
        denied,_=await self.open(second,port); await self.rejected(denied)
        independent,other_writer=await self.open(other_credential); await self.accepted(independent)
        await self.close_writer(writer); await self.close_writer(other_writer); await self.idle()
        replacement,_=await self.open(second,port); await self.accepted(replacement)

    async def test_simultaneous_handshakes_reserve_before_upstream_await(self):
        await self.apply(1)
        streams=await asyncio.gather(*(self.open() for _ in range(8)))
        async def read(reader):
            try: return await asyncio.wait_for(reader.readexactly(7),3)
            except asyncio.IncompleteReadError as error:
                self.assertEqual(error.partial,b'')
                return b''
            except ConnectionResetError: return b''
        replies=await asyncio.gather(*(read(r) for r,_ in streams))
        self.assertEqual(replies.count(b'\0\0hello'),1)
        self.assertEqual(replies.count(b''),7)

    async def test_invalid_auth_does_not_consume_slot_and_restart_restores_limit(self):
        await self.apply(1)
        bad,_=await self.open(str(uuid.uuid4())); await self.rejected(bad)
        first,writer=await self.open(); await self.accepted(first)
        await self.runtime.close(); await self.rejected(first)
        self.server.registry=Registry(self.server.registry.path)
        self.runtime=VlessRuntime(test_xray.BINARY,Path(self.temp.name)/'restarted',
                                 local_test_target=('127.0.0.1',self.target_port))
        await self.runtime.apply(self.server.registry.configuration(self.token))
        restored,_=await self.open(); await self.accepted(restored)
        excess,_=await self.open(); await self.rejected(excess)

    async def test_limit_change_failure_closes_old_listener_and_unlimited_reset(self):
        await self.apply(None)
        reader,_=await self.open(); await self.accepted(reader)
        import socket
        busy=socket.socket(); busy.bind(('127.0.0.1',0)); busy.listen()
        port=busy.getsockname()[1]
        registry=self.server.registry
        nid=registry.save('nodes',{'name':'blocked','server_id':self.sid})['id']
        registry.configure_node(nid,{**test_xray.VLESS,'listen_port':port})
        old=self.runtime.snapshot
        try:
            registry.user_connections(self.uid,{'max_connections':1})
            current=registry.configuration(self.token)
            with self.assertRaises(OSError): await self.runtime.apply(current)
            await self.rejected(reader)
            with self.assertRaises(ValueError): await self.runtime.apply(old)
        finally: busy.close()
        await self.runtime.apply(current)
        first,_=await self.open(); await self.accepted(first)
        denied,_=await self.open(); await self.rejected(denied)
        await self.apply(None)
        streams=await asyncio.gather(*(self.open() for _ in range(2)))
        for stream,_ in streams: await self.accepted(stream)

    async def test_upstream_connect_failure_releases_reserved_slot(self):
        await self.apply(1)
        entry=next(iter(self.runtime.listeners.values()))
        actual=entry.inner_port
        entry.inner_port=free_port() # real TCP refusal before the private core
        failed,_=await self.open(); await self.rejected(failed)
        await self.idle()
        entry.inner_port=actual
        restored,_=await self.open(); await self.accepted(restored)
