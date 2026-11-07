import json
import urllib.error
import test_core
import unittest


class ManagementWebTests(unittest.TestCase):
    setUp = test_core.ApiTests.setUp
    tearDown = test_core.ApiTests.tearDown
    request = test_core.ApiTests.request

    def test_public_static_shell_whitelist_csp_and_api_auth_unchanged(self):
        from management_web import ROUTES
        for path in ROUTES:
            with self.request(path, auth=False) as reply:
                self.assertEqual(reply.status,200)
                self.assertEqual(reply.headers['Cache-Control'],'no-store')
                self.assertIn("connect-src 'self'",reply.headers['Content-Security-Policy'])
                self.assertNotIn(b'fixture-only',reply.read())
        for path in ('/manage/assets/server.py','/manage/../server.py','/manage/%2e%2e/server.py','/manage/assets/.local-backup'):
            with self.assertRaises(urllib.error.HTTPError) as caught:self.request(path,auth=False)
            self.assertEqual(caught.exception.code,404)
        for path in ('/api/plans','/api/nodes','/api/plans/1/nodes'):
            with self.assertRaises(urllib.error.HTTPError) as caught:self.request(path,auth=False)
            self.assertEqual(caught.exception.code,401)
        with self.assertRaises(urllib.error.HTTPError) as caught:self.request('/manage/bandwidth','POST',{},auth=False)
        self.assertEqual(caught.exception.code,401)

    def test_authenticated_catalog_and_scope_do_not_grant_execution(self):
        from plan_nodes import PlanNodes
        r=self.server.registry
        host=r.save('servers',{'name':'private-host'})['id']
        node=r.save('nodes',{'name':'private-node','server_id':host})['id']
        plan=r.save_plan({'name':'private-plan','bandwidth':{'upload_bps':32768,'download_bps':None}})['id']
        self.assertEqual(json.load(self.request('/api/plans'))[0]['id'],plan)
        self.assertEqual(json.load(self.request('/api/nodes'))[0]['id'],node)
        path=f'/api/plans/{plan}/nodes'
        self.assertFalse(json.load(self.request(path))['restricted'])
        PlanNodes(r).save(plan,{'node_ids':[]})
        scope=json.load(self.request(path));self.assertTrue(scope['restricted']);self.assertEqual(scope['node_ids'],[])
        with r.connect() as db:self.assertEqual(db.execute('SELECT count(*) FROM node_users').fetchone()[0],0)
