"""Independent asyncio TCP forwarder. Does not execute shell commands or use Xray."""
import asyncio
import copy
from dataclasses import dataclass, field
from node_config import validate_snapshot
from bandwidth import BytePacer, CombinedPacer


@dataclass
class Listener:
    config: dict
    server: object = None
    tasks: set = field(default_factory=set)
    pacers: dict = field(default_factory=dict)

    def set_config(self, config):
        self.config = copy.deepcopy(config)
        limits = config.get('bandwidth', {})
        self.pacers = {direction: BytePacer(limits.get(direction + '_bps'))
                       for direction in ('upload', 'download')}


class ForwardRuntime:
    def __init__(self, max_connections=256):
        self.listeners = {}
        self.snapshot = None
        self.desired_identity = None
        self.max_connections = max_connections
        self.server_pacers = {d: BytePacer(None) for d in ('upload', 'download')}

    async def _serve(self, entry, reader, writer):
        task = asyncio.current_task()
        if not entry.server.is_serving() or len(entry.tasks) >= self.max_connections:
            writer.close()
            await writer.wait_closed()
            return
        entry.tasks.add(task)
        upstream = None
        try:
            remote_reader, upstream = await asyncio.wait_for(asyncio.open_connection(
                entry.config['target_host'], entry.config['target_port']), timeout=5)
            # Python 3.9 wait_for can lose cancellation as its child completes.
            # Retired connections must not proceed even in that race.
            if task not in entry.tasks or not entry.server.is_serving(): return

            async def pump(source, destination, pacer):
                while True:
                    block = await source.read(65536)
                    if not block:
                        if destination.can_write_eof():
                            destination.write_eof()
                            await destination.drain()
                        return
                    await pacer.write(destination, block)

            directions = [asyncio.create_task(pump(reader, upstream, CombinedPacer(entry.pacers['upload'],self.server_pacers['upload']))),
                          asyncio.create_task(pump(remote_reader, writer, CombinedPacer(entry.pacers['download'],self.server_pacers['download'])))]
            try:
                await asyncio.gather(*directions)
            finally:
                for direction in directions: direction.cancel()
                await asyncio.gather(*directions, return_exceptions=True)
        except (OSError, asyncio.TimeoutError):
            pass  # Close the failed connection; never claim the upstream is healthy.
        finally:
            for stream in (writer, upstream):
                if stream is not None: stream.close()
            for stream in (writer, upstream):
                if stream is not None:
                    try: await stream.wait_closed()
                    except OSError: pass
            entry.tasks.discard(task)

    async def _stop(self, entry):
        entry.server.close()
        tasks = list(entry.tasks)
        entry.tasks.clear()
        for task in tasks: task.cancel()
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)
        # Python 3.12 waits for accepted streams as well as the listening socket.
        await entry.server.wait_closed()

    async def apply(self, value):
        # Own a copy so subsequent caller mutations cannot silently change targets.
        snapshot = copy.deepcopy(validate_snapshot(value))
        if 'valid_until' in snapshot: raise ValueError('TCP 转发执行器不支持临时用户配置')
        if any(n['config'].get('protocol') for n in snapshot['nodes']):
            raise ValueError('TCP 执行端不能加载 Xray 节点，请选择 Xray 执行端')
        identity=(snapshot['server_id'],snapshot['revision'],snapshot['digest'])
        if self.desired_identity is not None:
            server,revision,checksum=self.desired_identity
            if identity[0]!=server: raise ValueError('服务器身份变化')
            if identity[1]<revision: raise ValueError('拒绝旧配置版本')
            if identity[1]==revision and identity[2]!=checksum:
                raise ValueError('相同配置版本内容不一致')
        self.desired_identity=identity
        if self.snapshot is not None:
            if snapshot['server_id'] != self.snapshot['server_id']:
                raise ValueError('服务器身份变化')
            if snapshot['revision'] < self.snapshot['revision']:
                raise ValueError('拒绝旧配置版本')
            if snapshot['revision'] == self.snapshot['revision']:
                if snapshot['digest'] != self.snapshot['digest']:
                    raise ValueError('相同配置版本内容不一致')
                return
            previous_limits={node['id']:node['config'].get('bandwidth',{})
                             for node in self.snapshot['nodes'] if node['config']['enabled']}
            node_limits_changed=any(node['id'] in previous_limits and
                                    node['config'].get('bandwidth',{})!=previous_limits[node['id']]
                                    for node in snapshot['nodes'])
            # A bind failure must not preserve an obsolete node-level limit,
            # just as it must not preserve an obsolete server-wide limit.
            if snapshot.get('bandwidth') != self.snapshot.get('bandwidth') or node_limits_changed:
                await self.close()
        wanted = {(n['config']['listen_host'], n['config']['listen_port']): n['config']
                  for n in snapshot['nodes'] if n['config']['enabled']}
        added = {}
        try:
            # Bind every new socket before touching the working configuration.
            for endpoint, config in wanted.items():
                if endpoint in self.listeners: continue
                entry = Listener(copy.deepcopy(config))
                entry.set_config(config)
                entry.server = await asyncio.start_server(
                    lambda r, w, entry=entry: self._serve(entry, r, w),
                    *endpoint, start_serving=False)
                added[endpoint] = entry
            for entry in added.values(): await entry.server.start_serving()
        except BaseException:
            for entry in added.values(): await self._stop(entry)
            raise
        # No awaits while switching targets. Existing changed connections close below.
        retired, changed_tasks = [], []
        for endpoint, entry in self.listeners.items():
            if endpoint not in wanted:
                entry.server.close()
                retired.append(entry)
            elif entry.config != wanted[endpoint]:
                changed_tasks.extend(entry.tasks)
                entry.tasks.clear()
                entry.set_config(wanted[endpoint])
        self.listeners = {key: value for key, value in self.listeners.items() if key in wanted}
        self.listeners.update(added)
        limits=snapshot.get('bandwidth',{})
        if self.snapshot is None or limits != self.snapshot.get('bandwidth',{}):
            self.server_pacers={d:BytePacer(limits.get(d+'_bps')) for d in ('upload','download')}
        self.snapshot = snapshot
        for task in changed_tasks: task.cancel()
        if changed_tasks: await asyncio.gather(*changed_tasks, return_exceptions=True)
        for entry in retired: await self._stop(entry)

    async def close(self):
        for entry in self.listeners.values(): await self._stop(entry)
        self.listeners.clear()
        self.snapshot = None
