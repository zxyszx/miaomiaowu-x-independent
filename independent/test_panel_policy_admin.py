import json, unittest
from panel_policy_admin import route
import test_panel_admin as fx

class PolicyRouteTests(unittest.TestCase):
    def test_exact_routes_and_global_delete(self):
        self.assertEqual(route('/api/automatic-bandwidth','DELETE'),('automatic_bandwidth',None,False))
        self.assertEqual(route('/api/users/01/automatic-bandwidth','GET'),('automatic_bandwidth',1,False))
        self.assertEqual(route('/api/users/1/bandwidth-policy','GET'),('bandwidth_policy',1,False))
        self.assertEqual(route('/api/users/1/bandwidth','DELETE'),('user_bandwidth',1,False))
        self.assertEqual(route('/api/users/1/connections-policy','GET'),('connections_policy',1,False))
        self.assertEqual(route('/api/users/1/connections','PUT'),('user_connections',1,False))
        self.assertEqual(route('/api/users/1/temporary-bandwidth','GET'),('temporary_bandwidth',1,False))
        self.assertEqual(route('/api/servers/1/bandwidth','PUT'),('server_bandwidth',1,False))
        self.assertIsNone(route('/api/users/1/bandwidth-policy','PUT'))
        self.assertIsNone(route('/api/automatic-bandwidth?x=1','GET'))
    def test_nineteen_admitted_method_pairs_and_rejections(self):
        pairs=[('/api/automatic-bandwidth',m) for m in ('GET','PUT','DELETE')]
        pairs += [('/api/users/1/'+s,m) for s,ms in (('automatic-bandwidth',('GET','PUT','DELETE')),('bandwidth-policy',('GET',)),('bandwidth',('GET','PUT','DELETE')),('connections-policy',('GET',)),('connections',('GET','PUT','DELETE')),('temporary-bandwidth',('GET','PUT','DELETE'))) for m in ms]
        pairs += [('/api/servers/1/bandwidth',m) for m in ('GET','PUT')]
        self.assertEqual(len(pairs),19)
        self.assertTrue(all(route(*pair) is not None for pair in pairs))
        self.assertTrue(all(route(path,'PATCH') is None for path,_ in pairs))

class PolicyHTTPTests(unittest.TestCase):
    setUp=fx.AdminTests.setUp
    tearDown=fx.AdminTests.tearDown
    call=fx.AdminTests.call
    login=fx.AdminTests.login
    challenge=fx.AdminTests.challenge
    panel=fx.AdminTests.panel
    request=fx.AdminTests.request
    def setUp(self):
        fx.AdminTests.setUp(self); self.server.panel_admin_enabled=True; self.cookie,self.csrf=self.login()
    def test_admin_policy_get_and_put_delete(self):
        uid=self.registry.save('users',{'name':'policy-user'})['id']
        code,_,body,_=self.call('/api/users/%d/bandwidth-policy'%uid,cookie=self.cookie)
        self.assertEqual(code,200); self.assertIn('effective',json.loads(body))
        code,_,_,_=self.call('/api/users/%d/bandwidth'%uid,'PUT',{'upload_bps':1234,'download_bps':None},cookie=self.cookie,csrf=self.csrf)
        self.assertEqual(code,200)
        code,_,_,_=self.call('/api/users/%d/bandwidth'%uid,'DELETE',{},cookie=self.cookie,csrf=self.csrf)
        self.assertEqual(code,200)
    def test_plain_user_and_dual_credentials_fail_closed(self):
        uid=self.registry.save('users',{'name':'policy-user'})['id']
        code,_,_,_=self.call('/api/users/%d/bandwidth'%uid,'PUT',{'upload_bps':1,'download_bps':None},cookie=self.cookie,csrf=self.csrf)
        self.assertEqual(code,200)
        self.assertEqual(self.call('/api/users/%d/bandwidth'%uid)[0],401)
        self.assertEqual(self.call('/api/users/%d/bandwidth'%uid,'GET',cookie=self.cookie,headers=[('Authorization','Bearer '+self.server.token)])[0],400)

if __name__=='__main__': unittest.main()
