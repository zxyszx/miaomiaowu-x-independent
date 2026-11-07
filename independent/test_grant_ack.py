"""Authenticated installation claims never arise from ordinary/staging ACKs."""
import copy
import json
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from agent import request
from grant_ack import fact_for,GrantAcknowledgement
from core import Error
from grant_capabilities import STAGING,INSTALLATION
from grant_configuration import GrantConfiguration
import test_grant_transport as fixture

class GrantAckTests(unittest.TestCase):
    def setup_ack(self):
        self.start_http(offer=[STAGING,INSTALLATION]);self.fetch()
        self.value=GrantConfiguration(self.r).bind(self.token,self.g['grant_id'])
        self.fact=fact_for(self.value)

    def ack(self,fact=None):
        return request(self.url,self.token,'/agent/grants/installed',self.fact if fact is None else fact)

    def counts(self):
        with self.r.connect() as db:
            return tuple(db.execute('SELECT count(*) FROM '+table).fetchone()[0]
                for table in ('usage_first_total_installations','usage_first_total_applied'))

    def test_http_exact_retry_and_concurrent_unique_record(self):
        self.setup_ack();self.assertEqual(self.counts(),(0,0))
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:self.ack(),range(2)))
        self.assertEqual(results[0],results[1]);self.assertEqual(self.counts(),(1,1))
        self.assertFalse(results[0]['ready_for_execution'])
        self.assertEqual(results[0]['installation'],self.fact)
        self.assertEqual(self.ack(),results[0])

    def test_staging_capability_cannot_ack(self):
        self.start_http();self.fetch()
        self.fact=fact_for(GrantConfiguration(self.r).bind(self.token,self.g['grant_id']))
        self.rejected(409,self.ack);self.assertEqual(self.counts(),(0,0))

    def test_bad_fact_never_writes_either_record(self):
        self.setup_ack()
        for key,value in [('runtime_digest','0'*64),('digest','0'*64),('protocol','reality'),
                          ('revision',self.fact['revision']+1),('server_id',True),
                          ('valid_until',True),('extra',True)]:
            bad=dict(self.fact);bad[key]=value
            self.rejected(400 if key in ('server_id','valid_until','extra') else 409,
                          lambda bad=bad:self.ack(bad))
            self.assertEqual(self.counts(),(0,0))
        # The normal HTTP client already rejects non-JSON finite values.
        for deadline in (float('nan'),float('inf')):
            with self.assertRaises(Error) as caught:
                GrantAcknowledgement(self.r).accept(self.token,dict(self.fact,valid_until=deadline))
            self.assertEqual(caught.exception.status,400)
            self.assertEqual(self.counts(),(0,0))

    def test_policy_changed_before_first_ack_rejected(self):
        self.setup_ack();self.cycles.set_total_budget(self.period,0)
        self.rejected(409,self.ack);self.assertEqual(self.counts(),(0,0))

    def test_atomic_failure_and_no_private_http_detail(self):
        self.setup_ack()
        with self.r.connect() as db:
            db.execute("CREATE TRIGGER reject_applied BEFORE INSERT ON usage_first_total_applied BEGIN SELECT RAISE(ABORT,'private storage detail'); END")
        self.rejected(500,self.ack);self.assertEqual(self.counts(),(0,0))

    def test_historical_retry_does_not_extend_deadline_but_auth_still_required(self):
        self.setup_ack();first=self.ack()
        with patch('time.time',return_value=self.fact['valid_until']+1):
            self.assertEqual(self.ack(),first)
        with self.r.connect() as db:db.execute("UPDATE agents SET token_hash='revoked'")
        self.rejected(401,self.ack);self.assertEqual(self.counts(),(1,1))

    def test_first_ack_expired_rejected(self):
        self.setup_ack()
        with patch('time.time',return_value=self.fact['valid_until']):
            self.rejected(409,self.ack)
        self.assertEqual(self.counts(),(0,0))

    def test_ack_cannot_create_configuration_binding(self):
        self.setup_ack()
        with self.r.connect() as db:db.execute('DELETE FROM usage_first_total_configurations')
        self.rejected(409,self.ack);self.assertEqual(self.counts(),(0,0))

for name in ('setUp','tearDown','fixtures','fixture','setup_total','setup_drain','applied',
             'report','data','envelope','accept','sealed','request','prepare','start',
             'start_http','fetch','post','rejected'):
    setattr(GrantAckTests,name,getattr(fixture.GrantTransportTests,name))
GrantAckTests.encode=staticmethod(fixture.GrantTransportTests.encode)
