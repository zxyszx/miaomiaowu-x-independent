#!/usr/bin/env python3
"""Explicit TCP or Xray execution agent for independently configured nodes."""
import argparse
import asyncio
import json
from pathlib import Path
import sys
import time
import signal
import urllib.error
import uuid
from agent import request, send, sync, begin_session, probe_pending
from forward_runtime import ForwardRuntime


LEASE_SECONDS=45
SYNC_INTERVAL=15


async def _blocking(runtime, function, *args):
    # Cancelling to_thread() does not stop its OS thread. Stop forwarding first,
    # then drain that worker before another cycle can read/write the same state.
    worker=asyncio.create_task(asyncio.to_thread(function,*args))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        await runtime.close()
        try: await asyncio.shield(worker)
        except Exception: pass
        raise


async def cycle(base, token, state, runtime, uptime, session=None, epochs=None):
    try:
        if epochs is not None: session=epochs.session
        while True:
            extra=() if session is None else (session,)
            if session is not None: await _blocking(runtime,begin_session,base,token,session)
            await _blocking(runtime, send, base, token, uptime,*extra)
            await _blocking(runtime, sync, base, token, state,*extra)
            snapshot = json.loads((Path(state) / 'staged-config.json').read_text())
            next_session=await epochs.prepare(snapshot) if epochs is not None else session
            if next_session==session: break
            # Claim, heartbeat and stage again under the new execution session.
            # A changed remote snapshot is checked again before any listener opens.
            session=next_session
        await runtime.apply(snapshot)
        data={key:snapshot[key] for key in ('revision','digest')}
        if session is not None: data['session_id']=session
        receipt = await _blocking(runtime, request, base, token, '/agent/config/applied',data)
        if receipt.get('accepted') is not True or receipt.get('state') != 'listening' or receipt.get('revision') != snapshot['revision']:
            raise ValueError('主控未确认执行回执')
        if epochs is not None: epochs.confirmed(session)
        return receipt
    except urllib.error.HTTPError as error:
        if error.code in (401, 403): await runtime.close()
        raise


async def optional_probe(base, token, runtime):
    """Probe without holding the configuration lease; return auth revocation."""
    try:
        await asyncio.to_thread(probe_pending,base,token)
    except (OSError,ValueError,TypeError,KeyError) as error:
        if isinstance(error,urllib.error.HTTPError) and error.code in (401,403):
            await runtime.close()
            return True
        print('Optional domain probe failed; execution sync continues',file=sys.stderr,flush=True)
    return False


async def run(config, state, runtime=None):
    runtime = runtime or ForwardRuntime()
    start = time.monotonic()
    last_success = start
    confirmed=False
    session=str(uuid.uuid4())
    probe=None
    usage=None
    usage_task=None
    epochs=None
    if callable(getattr(runtime,'usage_snapshot',None)):
        from execution_epochs import ExecutionEpochs
        epochs=ExecutionEpochs(config['master'],config['token'],state,runtime,session)
        usage=epochs.delivery
    try:
        while True:
            if probe is not None and probe.done():
                if probe.result(): return
                probe=None
            try:
                remaining=(max(0,last_success+LEASE_SECONDS-time.monotonic())
                           if confirmed else LEASE_SECONDS)
                await asyncio.wait_for(cycle(config['master'],config['token'],state,runtime,
                                             time.monotonic()-start,session,epochs),timeout=remaining)
                # Revocation may arrive while a configuration cycle is in flight.
                # Never leave newly applied listeners running after that result.
                if probe is not None and probe.done() and probe.result(): return
                last_success = time.monotonic()
                confirmed=True
                if usage is not None and usage_task is None:
                    usage_task=asyncio.create_task(usage.run())
                if probe is None:
                    probe=asyncio.create_task(optional_probe(config['master'],config['token'],runtime))
                print('Configuration listeners active; upstream reachability is checked per connection', flush=True)
            except (OSError, asyncio.TimeoutError, ValueError, TypeError, RuntimeError):
                if time.monotonic()-last_success>=LEASE_SECONDS:
                    await runtime.close()
                    confirmed=False
                print('Execution sync failed; runtime state was not confirmed', file=sys.stderr, flush=True)
            # Do not sleep beyond the remaining confirmed lease. Once stopped,
            # a new attempt gets a fresh bounded window to restore service.
            delay=(min(SYNC_INTERVAL,max(0,last_success+LEASE_SECONDS-time.monotonic()))
                   if confirmed else SYNC_INTERVAL)
            await asyncio.sleep(delay)
    finally:
        await runtime.close()
        if usage is not None: await usage.finish(usage_task)
        # to_thread cancellation cannot stop the worker. Close listeners first,
        # then drain it so an old receipt cannot outlive this Agent process loop.
        if probe is not None: await asyncio.shield(probe)


async def run_until_stopped(config,state,runtime):
    loop=asyncio.get_running_loop()
    task=asyncio.create_task(run(config,state,runtime))
    for signum in (signal.SIGINT,signal.SIGTERM): loop.add_signal_handler(signum,task.cancel)
    try: await task
    except asyncio.CancelledError: pass
    finally:
        for signum in (signal.SIGINT,signal.SIGTERM): loop.remove_signal_handler(signum)


async def run_grant_until_stopped(scheduler):
    loop=asyncio.get_running_loop()
    task=asyncio.create_task(scheduler.run())
    for signum in (signal.SIGINT,signal.SIGTERM): loop.add_signal_handler(signum,task.cancel)
    try: await task
    except asyncio.CancelledError: pass
    finally:
        for signum in (signal.SIGINT,signal.SIGTERM): loop.remove_signal_handler(signum)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--state', type=Path)
    parser.add_argument('--engine', choices=('tcp','xray'), default='tcp')
    parser.add_argument('--mode', choices=('legacy','grant'), default='legacy')
    parser.add_argument('--xray-binary', type=Path)
    parser.add_argument('--server-id', type=int)
    parser.add_argument('--session-id')
    parser.add_argument('--grant-id')
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    if not isinstance(config, dict) or set(config) != {'master', 'token'}:
        raise SystemExit('配置应包含 master 和 token')
    state=args.state or args.config.resolve().parent / 'agent-state'
    if args.mode=='grant':
        missing=[name for name,value in (
            ('--xray-binary',args.xray_binary),('--server-id',args.server_id),
            ('--session-id',args.session_id),('--grant-id',args.grant_id)) if value is None]
        if missing: parser.error('授予模式需要 '+', '.join(missing))
        from grant_scheduler import GrantScheduler
        scheduler=GrantScheduler(config['master'],config['token'],state,
            args.xray_binary,args.server_id,args.session_id,args.grant_id)
        try: asyncio.run(run_grant_until_stopped(scheduler))
        except KeyboardInterrupt: pass
        return
    runtime=None
    if args.engine=='xray':
        if args.xray_binary is None: parser.error('Xray 执行端需要 --xray-binary')
        from reality_runtime import RealityRuntime
        runtime=RealityRuntime(args.xray_binary,state/'xray')
    try: asyncio.run(run_until_stopped(config,state,runtime))
    except KeyboardInterrupt: pass


if __name__ == '__main__': main()
