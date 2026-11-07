"""Installation journal storage contracts; these are not remote execution proofs."""
import copy
import json
import sqlite3
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import test_grant_agent as staging
from grant_installation import record


class InstallationJournalTests(unittest.TestCase):
    setUp = staging.GrantAgentTests.setUp
    sign = staging.GrantAgentTests.sign
    identity = staging.GrantAgentTests.identity
    stage = staging.GrantAgentTests.stage

    def rows(self):
        with sqlite3.connect(self.path) as db:
            return db.execute('SELECT fact,first_installed_at FROM local_grant_installations').fetchall()

    def save(self):
        return record(self.store, self.row, 'a'*64, 'vless')

    def test_exact_retry_preserves_history_without_live_or_secret_claim(self):
        self.stage()
        fact = self.save()
        saved = self.rows()
        self.assertEqual(self.save(), fact)
        self.assertEqual(self.rows(), saved)
        self.assertEqual(json.loads(saved[0][0]), fact)
        self.assertEqual(set(fact), {'grant_id','digest','runtime_digest','protocol',
                                    'server_id','session_id','revision','valid_until'})
        self.assertNotIn(self.row['client']['uuid'], saved[0][0])
        self.assertNotIn('active', saved[0][0])

    def test_original_stage_is_required_and_must_match(self):
        self.stage()
        with sqlite3.connect(self.path) as db:
            db.execute('DELETE FROM staged_grants')
        with self.assertRaises(ValueError): self.save()
        self.stage()
        changed = copy.deepcopy(self.row)
        changed['reserved_bytes'] += 1
        self.sign(changed)
        with self.assertRaises(ValueError): record(self.store, changed, 'a'*64, 'vless')

    def test_protocol_digest_and_immutable_runtime_binding(self):
        self.stage()
        with self.assertRaises(ValueError): record(self.store, self.row, 'bad', 'vless')
        with self.assertRaises(ValueError): record(self.store, self.row, 'a'*64, 'reality')
        self.save()
        before = self.rows()
        with self.assertRaises(ValueError): record(self.store, self.row, 'b'*64, 'vless')
        self.assertEqual(self.rows(), before)

    def test_expiry_rechecked_after_write_lock(self):
        self.stage()
        with patch('grant_agent.time.time', side_effect=[self.row['issued_at'], self.row['valid_until']]):
            with self.assertRaises(ValueError): self.save()
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM sqlite_master WHERE name='local_grant_installations'").fetchone()[0], 0)

    def test_write_failure_rolls_back_and_retry_preserves_stage(self):
        self.stage()
        self.save()
        with sqlite3.connect(self.path) as db:
            db.execute('DELETE FROM local_grant_installations')
            db.execute("CREATE TRIGGER failed BEFORE INSERT ON local_grant_installations BEGIN SELECT RAISE(ABORT,'disk'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.save()
        self.assertEqual(self.rows(), [])
        with sqlite3.connect(self.path) as db: db.execute('DROP TRIGGER failed')
        self.save()
        self.assertEqual(len(self.rows()), 1)

    def test_concurrent_exact_retry_creates_one_fact(self):
        self.stage()
        with ThreadPoolExecutor(2) as pool:
            facts = list(pool.map(lambda _: self.save(), range(2)))
        self.assertEqual(facts[0], facts[1])
        self.assertEqual(len(self.rows()), 1)

    def test_multi_missing_kind_and_same_id_kind_conflict_never_write_journal(self):
        attacks=('multi','missing_kind','conflict')
        for attack in attacks:
            with self.subTest(attack=attack):
                path=self.path.with_name(attack+'.db')
                store=type(self.store)(path,self.row['server_id'],self.row['session_id'])
                if attack=='missing_kind':
                    content={key:self.row[key] for key in staging.FIELDS}
                    if 'server_bandwidth' in self.row:
                        content['server_bandwidth']=self.row['server_bandwidth']
                    raw=json.dumps(content,sort_keys=True,separators=(',',':'),
                                   ensure_ascii=False,allow_nan=False)
                    with sqlite3.connect(path) as db:
                        db.execute('CREATE TABLE grant_owner(server_id INTEGER NOT NULL)')
                        db.execute('INSERT INTO grant_owner VALUES(?)',(self.row['server_id'],))
                        db.execute('''CREATE TABLE staged_grants(
                            grant_id TEXT PRIMARY KEY,cycle_id TEXT NOT NULL UNIQUE,
                            payload TEXT NOT NULL,digest TEXT NOT NULL)''')
                        db.execute('INSERT INTO staged_grants VALUES(?,?,?,?)',
                                   (self.row['grant_id'],self.row['cycle_id'],raw,
                                    self.row['digest']))
                else:
                    store.stage(self.row,self.identity(self.row,'multi_allocation'))
                    if attack=='conflict':
                        with sqlite3.connect(path) as db:
                            row=db.execute('''SELECT grant_id,cycle_id,payload,digest
                                FROM staged_grants''').fetchone()
                            db.execute('INSERT INTO staged_grants VALUES(?,?,?,?,?)',
                                       ('first_grant',*row))
                with self.assertRaises((ValueError,sqlite3.Error)):
                    record(store,self.row,'a'*64,'vless')
                with sqlite3.connect(path) as db:
                    self.assertEqual(db.execute("""SELECT count(*) FROM sqlite_master
                        WHERE type='table' AND name='local_grant_installations'""").fetchone()[0],0)
