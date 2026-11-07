"""Rotate accounting epochs before a previously executed identity changes cycle."""
import uuid
from usage_delivery import UsageDelivery,FAILURES


class ExecutionEpochs:
    def __init__(self,base,token,state,runtime,session):
        self.runtime=runtime
        self.delivery=UsageDelivery(base,token,state,runtime,session)
        # Keep removed identities too: re-adding one in another period must not
        # rebind the proof or bring its old cumulative bytes into the new period.
        self.cycles={}

    @property
    def session(self): return self.delivery.session

    async def prepare(self,snapshot):
        desired={(node['id'],client['user_id'],client['uuid']):client.get('cycle_id')
                 for node in snapshot['nodes'] for client in node.get('clients',[])}
        changed=any(key in self.cycles and self.cycles[key]!=value
                    for key,value in desired.items())
        if changed:
            await self.runtime.close()
            try: await self.delivery.rotate(str(uuid.uuid4()))
            except FAILURES as error:
                # Closed listeners cannot resume with an uncaptured old tail.
                # Retrying the sync may recover durable storage; never reset it.
                raise ValueError('旧周期流量未持久保存，暂停周期切换') from error
            self.delivery.unconfirmed_session=self.session
            self.cycles={}
        self.cycles.update(desired)
        return self.session

    def confirmed(self,session):
        if session==self.session: self.delivery.unconfirmed_session=None
