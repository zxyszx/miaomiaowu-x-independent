import tempfile
import unittest
import uuid
from unittest import mock
from execution_epochs import ExecutionEpochs


class EpochTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.runtime=mock.Mock()
        self.runtime.close=mock.AsyncMock()
        self.runtime.usage_snapshot.return_value=dict(server_id=1,items=[])
        self.epochs=ExecutionEpochs('unused','unused',self.temp.name,self.runtime,str(uuid.uuid4()))
        self.client=dict(user_id=1,uuid=str(uuid.uuid4()),cycle_id='one')

    def snapshot(self,client=None):
        return dict(nodes=[dict(id=1,clients=[client or self.client])])

    async def test_same_period_budget_change_does_not_rotate(self):
        first=await self.epochs.prepare(self.snapshot())
        self.client['cycle_budget']=dict(upload_bytes=20,download_bytes=30)
        self.assertEqual(await self.epochs.prepare(self.snapshot()),first)
        self.runtime.close.assert_not_awaited()

    async def test_removed_identity_readded_with_new_cycle_rotates_after_close(self):
        first=await self.epochs.prepare(self.snapshot())
        await self.epochs.prepare(dict(nodes=[]))
        self.client['cycle_id']='two'
        original=self.epochs.delivery.rotate
        async def rotate(session):
            self.runtime.close.assert_awaited_once()
            await original(session)
        with mock.patch.object(self.epochs.delivery,'rotate',side_effect=rotate):
            self.assertNotEqual(await self.epochs.prepare(self.snapshot()),first)
        self.assertEqual(self.epochs.delivery.unconfirmed_session,self.epochs.session)
        self.epochs.confirmed(first)
        self.assertEqual(self.epochs.delivery.unconfirmed_session,self.epochs.session)
        self.epochs.confirmed(self.epochs.session)
        self.assertIsNone(self.epochs.delivery.unconfirmed_session)

    async def test_failed_tail_capture_never_commits_new_epoch_or_reopens(self):
        first=await self.epochs.prepare(self.snapshot())
        self.client['cycle_id']='two'
        with mock.patch.object(self.epochs.delivery,'rotate',side_effect=OSError('full')):
            with self.assertRaises(ValueError): await self.epochs.prepare(self.snapshot())
        self.runtime.close.assert_awaited_once()
        self.assertEqual(self.epochs.session,first)
        self.assertEqual(list(self.epochs.cycles.values()),['one'])
        self.assertNotEqual(await self.epochs.prepare(self.snapshot()),first)
