#!/usr/bin/env python3
"""Isolated Linux/systemd lifecycle QA. Requires root; removes its own service/data."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.request
import uuid


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--migration',action='store_true',help='test reconstructed legacy snapshot format and paired rollback')
    args=parser.parse_args()
    if os.geteuid() != 0 or not Path('/run/systemd/system').is_dir():
        raise SystemExit('Run as root on a Linux systemd test host.')
    name = 'independent-x-qa-' + uuid.uuid4().hex[:12]
    root = Path('/opt') / name
    state = Path('/var/lib') / name
    unit = Path('/etc/systemd/system') / (name + '.service')
    source = Path(__file__).resolve().parent
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]

    def systemctl(*args):
        return subprocess.check_output(['systemctl', *args], text=True).strip()

    def request(path, data=None, authenticated=True, *, method=None, token=None):
        headers = {'Content-Type': 'application/json'}
        if authenticated:
            headers['Authorization'] = 'Bearer ' + (token or (state / 'admin-token').read_text().strip())
        req = urllib.request.Request('http://127.0.0.1:%d%s' % (port, path),
                                     data=None if data is None else json.dumps(data).encode(),
                                     headers=headers,method=method)
        with urllib.request.urlopen(req, timeout=2) as response:
            return json.load(response)

    def ready():
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                if request('/health', authenticated=False).get('ready'):
                    return
            except (OSError, ValueError):
                pass
            time.sleep(.1)
        raise RuntimeError('QA service did not become healthy')

    def pid():
        return int(systemctl('show', name, '--property=MainPID', '--value'))

    def switch(release):
        temporary = root / 'next'
        temporary.symlink_to(release)
        temporary.replace(root / 'current')

    def assert_preserved():
        assert hashlib.sha256((state / 'admin-token').read_bytes()).digest() == token_hash
        assert request('/api/users') == users

    try:
        root.mkdir(mode=0o755)
        for release in ('release-a', 'release-b'):
            target = root / release
            target.mkdir(mode=0o755)
            for file in source.glob('*.py'):
                shutil.copy2(file, target / file.name)
                (target / file.name).chmod(0o644)
        # Only the disposable QA copy changes: prove the new process executes
        # different source, rather than merely restarting behind a new symlink.
        new_server = root / 'release-b/server.py'
        old_source = new_server.read_text()
        assert "'stage':'temporary-bandwidth'" in old_source
        new_server.write_text(old_source.replace("'stage':'temporary-bandwidth'",
                                                 "'stage':'linux-qa-release-b'", 1))
        if args.migration:
            # Reconstruct only the independent pre-field-extension behavior.
            # This is a fixture, not a claim to run an archived official release.
            old_core=root/'release-a/core.py'
            code=old_core.read_text()
            code=code.replace('                CREATE TABLE IF NOT EXISTS schema_migrations(name TEXT PRIMARY KEY);\n','')
            start=code.index('            # A format change is a new configuration')
            end=code.index('        os.chmod(self.path,0o600)',start)
            code=code[:start]+code[end:]
            code=code.replace("                        client['valid_until']=temporary['expires_at']\n",'')
            old_core.write_text(code)
        switch('release-a')
        unit.write_text('[Unit]\nDescription=Isolated independent backend QA\n'
                        '[Service]\nType=simple\nDynamicUser=yes\n'
                        'StateDirectory=%s\nStateDirectoryMode=0700\n'
                        'ExecStart=/usr/bin/python3 %s/current/server.py --port %d --data %s\n'
                        'Restart=on-failure\nRestartSec=1\nNoNewPrivileges=yes\n'
                        'ProtectSystem=strict\nProtectHome=yes\nPrivateTmp=yes\n'
                        % (name, root, port, state))
        systemctl('daemon-reload')
        systemctl('start', name)
        ready()
        assert request('/health', authenticated=False)['stage'] == 'temporary-bandwidth'
        request('/api/users', {'name': 'isolated-lifecycle-fixture'})
        users = request('/api/users')
        assert len(users) == 1
        token_hash = hashlib.sha256((state / 'admin-token').read_bytes()).digest()
        systemctl('restart', name)
        ready()
        assert_preserved()
        print('PASS: systemd startup and restart preserve user and admin credential', flush=True)

        old_pid = pid()
        systemctl('kill', '--signal=KILL', name)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and pid() in (0, old_pid):
            time.sleep(.1)
        assert pid() not in (0, old_pid)
        ready()
        assert_preserved()
        print('PASS: unexpected termination automatically recovers with data intact', flush=True)

        if args.migration:
            sid=request('/api/servers',{'name':'migration server'})['id']
            nid=request('/api/nodes',{'name':'migration node','server_id':sid})['id']
            request(f'/api/nodes/{nid}/config',{'protocol':'vless','listen_host':'127.0.0.1','listen_port':24001,'enabled':True},method='PUT')
            request(f"/api/nodes/{nid}/users/{users[0]['id']}",{},method='POST')
            agent_token=request(f'/api/servers/{sid}/agent',{})['token']
            request(f"/api/users/{users[0]['id']}/temporary-bandwidth",
                    {'bandwidth':{'upload_bps':32768,'download_bps':None},'expires_at':time.time()+600},method='PUT')
            legacy=request('/agent/config',token=agent_token)
            assert 'valid_until' in legacy and 'valid_until' not in legacy['nodes'][0]['clients'][0]
            legacy_receipt={key:legacy[key] for key in ('revision','digest')}
            request('/agent/config/ack',legacy_receipt,token=agent_token)
            request('/agent/config/applied',legacy_receipt,token=agent_token)
            agent_state=root/'agent-state'; agent_state.mkdir(mode=0o700)
            staged=agent_state/'staged-config.json'
            staged.write_text(json.dumps(legacy)); staged.chmod(0o600)
            sys.path.insert(0,str(source))
            from agent import sync
            base='http://127.0.0.1:%d'%port

        systemctl('stop', name)
        with sqlite3.connect(state / 'registry.db') as db:
            with sqlite3.connect(root / 'before-upgrade.db') as backup:
                db.backup(backup)
                assert backup.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        switch('release-b')
        systemctl('start', name)
        ready()
        assert_preserved()
        assert request('/health', authenticated=False)['stage'] == 'linux-qa-release-b'
        if args.migration:
            upgraded=request('/agent/config',token=agent_token)
            assert upgraded['revision']==legacy['revision']+1
            assert upgraded['nodes'][0]['clients'][0]['valid_until']==upgraded['valid_until']
            assert upgraded['digest']!=legacy['digest']
            status=next(row for row in request('/api/agent-status') if row['server_id']==sid)
            assert not status['listeners_current']
            assert sync(base,agent_token,agent_state)['accepted']
            assert json.loads(staged.read_text())==upgraded
            systemctl('restart',name); ready()
            assert request('/agent/config',token=agent_token)==upgraded
            assert_preserved()
            print('PASS: real systemd legacy-format migration, stale receipt status and Agent disk synchronization',flush=True)
        systemctl('stop', name)
        switch('release-a')
        systemctl('start', name)
        ready()
        assert_preserved()
        assert request('/health', authenticated=False)['stage'] == 'temporary-bandwidth'
        if args.migration:
            rolled=request('/agent/config',token=agent_token)
            assert rolled['revision']==upgraded['revision'] and rolled['digest']!=upgraded['digest']
            try: sync(base,agent_token,agent_state)
            except ValueError: pass
            else: raise AssertionError('code-only downgrade must not silently replace same-version snapshot')
            assert json.loads(staged.read_text())==upgraded
            print('PASS: Agent rejects unsafe code-only downgrade with changed snapshot digest',flush=True)
            # Restore the stopped master DB AND the matching isolated Agent state.
            # Never delete the version fence in a deployed Agent to force rollback.
            systemctl('stop',name)
            with sqlite3.connect(root/'before-upgrade.db') as backup:
                with sqlite3.connect(state/'registry.db') as destination:
                    backup.backup(destination)
                    assert destination.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
            staged.write_text(json.dumps(legacy))
            systemctl('start',name); ready(); assert_preserved()
            assert request('/agent/config',token=agent_token)==legacy
            assert sync(base,agent_token,agent_state)['accepted']
            systemctl('stop',name); switch('release-b')
            systemctl('start',name); ready(); assert_preserved()
            assert request('/agent/config',token=agent_token)==upgraded
            assert sync(base,agent_token,agent_state)['accepted']
            print('PASS: paired master/Agent state restore and repeat upgrade preserve data and credentials',flush=True)
            print('LIMIT: reconstructed independent legacy fixture, not archived release; no cross-host rollout, web updater or live Xray execution in this service test.',flush=True)
        else:
            print('PASS: SQLite backup, release-directory switch, and code rollback preserve data', flush=True)
            print('LIMIT: QA health marker changes between releases; no schema migration or web updater is tested.', flush=True)
    finally:
        subprocess.run(['systemctl', 'stop', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        unit.unlink(missing_ok=True)
        subprocess.run(['systemctl', 'daemon-reload'], check=True)
        subprocess.run(['systemctl', 'reset-failed', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        shutil.rmtree(root, ignore_errors=True)
        # DynamicUser redirects /var/lib/<name> into /var/lib/private/<name>.
        if state.is_symlink():
            private_state = state.resolve()
            state.unlink()
            if private_state.parent == Path('/var/lib/private') and private_state.name == name:
                shutil.rmtree(private_state)
        elif state.exists():
            shutil.rmtree(state)
        print('CLEANUP: isolated QA unit and fixture directories removed', flush=True)


if __name__ == '__main__':
    main()
