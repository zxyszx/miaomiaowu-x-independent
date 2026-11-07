import json, unittest
from panel_lifecycle_admin import route
import test_panel_admin as fx

class LifecycleRoutes(unittest.TestCase):
    def test_three_methods_exact(self):
        self.assertEqual(route('/api/users/1/plan','PUT'),('user_plan',1,False))
        self.assertEqual(route('/api/users/1/plan-lifecycle','GET'),('plan_lifecycle',1,False))
        self.assertEqual(route('/api/users/1/plan-lifecycle','PUT'),('plan_lifecycle',1,False))
        for method in ('GET','POST','DELETE','PATCH'):
            self.assertIsNone(route('/api/users/1/plan',method))
        self.assertIsNone(route('/api/users/1/plan-lifecycle?x=1','GET'))

class LifecycleHTTP(unittest.TestCase):
    setUp=fx.AdminTests.setUp; tearDown=fx.AdminTests.tearDown
    request=fx.AdminTests.request; panel=fx.AdminTests.panel; challenge=fx.AdminTests.challenge
    login=fx.AdminTests.login; call=fx.AdminTests.call
    def setUp(self):
        fx.AdminTests.setUp(self); self.server.panel_admin_enabled=True; self.cookie,self.csrf=self.login()
        self.uid=self.registry.save('users',{'name':'life-user'})['id']; self.pid=self.registry.save_plan({'name':'life-plan','bandwidth':{'upload_bps':1,'download_bps':None}})['id']; self.registry.assign_plan(self.uid,{'plan_id':self.pid})
    def test_assignment_and_lifecycle_get_put(self):
        code,_,body,_=self.call(f'/api/users/{self.uid}/plan-lifecycle',cookie=self.cookie)
        self.assertEqual(code,200); self.assertIn('status',json.loads(body))
        code,_,_,_=self.call(f'/api/users/{self.uid}/plan-lifecycle','PUT',{'enabled':True,'expires_at':None},cookie=self.cookie,csrf=self.csrf)
        self.assertEqual(code,200)
        code,_,_,_=self.call(f'/api/users/{self.uid}/plan','PUT',{'plan_id':None},cookie=self.cookie,csrf=self.csrf)
        self.assertEqual(code,200)
    def test_ordinary_and_dual_credential_fail_closed(self):
        self.assertEqual(self.call(f'/api/users/{self.uid}/plan-lifecycle')[0],401)
        self.assertEqual(self.call(f'/api/users/{self.uid}/plan-lifecycle',headers=[('Authorization','Bearer '+self.server.token)],cookie=self.cookie)[0],400)

if __name__=='__main__': unittest.main()
