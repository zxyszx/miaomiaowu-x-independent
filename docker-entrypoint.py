"""Initialize only empty mounted data, drop privileges, exec frozen launcher."""
import fcntl
import ipaddress
import os
from pathlib import Path
import re
import stat
import sys
from urllib.parse import urlsplit

DATA = Path('/app/data')
UID = GID = 10001
MARKER = 'product=miaomiaowu-x-independent\nformat=1\n'


def main():
    if len(sys.argv) != 1:
        raise ValueError('command overrides are not supported; configure MMX_EXTERNAL_ORIGIN')
    origin = os.environ.get('MMX_EXTERNAL_ORIGIN', '')
    if not re.fullmatch(r'https?://[A-Za-z0-9.-]+(?::[0-9]+)?', origin):
        raise ValueError('MMX_EXTERNAL_ORIGIN is required: http[s]://actual-IP-or-domain:host-port; no path or wildcard')
    parsed = urlsplit(origin)
    if parsed.port is not None and not 1 <= parsed.port <= 65535:
        raise ValueError('invalid external origin port')
    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError:
        if not all(re.fullmatch(r'[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?', p) for p in parsed.hostname.split('.')):
            raise ValueError('invalid external origin hostname')
    else:
        if address.is_unspecified or address.is_multicast:
            raise ValueError('external origin must be an actual address, never 0.0.0.0 or multicast')

    # Hold the directory lock across both execs without creating a file that
    # would turn an empty, unmarked directory into an unknown nonempty one.
    descriptor = os.open(DATA, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    os.set_inheritable(descriptor, True)
    entries = os.listdir(descriptor)
    if not entries:
        if os.getuid() == 0:
            # No recursive ownership changes: only the verified empty root.
            os.fchown(descriptor, UID, GID)
            os.fchmod(descriptor, 0o700)
    else:
        marker = DATA / '.miaomiaowu-independent-data'
        info = marker.lstat()
        if not stat.S_ISREG(info.st_mode) or marker.read_text() != MARKER:
            raise ValueError('nonempty data is not a recognized independent directory; unchanged')
    for path in [DATA, *DATA.rglob('*')]:
        info = path.lstat()
        if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
            raise ValueError('data contains a link or special file; unchanged')
        if (info.st_uid, info.st_gid) != (UID, GID) or info.st_mode & 0o077:
            raise ValueError('existing data must retain container UID/GID 10001 and private permissions; no ownership repair performed')
    if os.getuid() == 0:
        os.setgroups([])
        os.setgid(GID)
        os.setuid(UID)
    if (os.getuid(), os.getgid()) != (UID, GID):
        raise ValueError('runtime must run as UID/GID 10001')
    os.umask(0o077)
    print(f'Container runtime independent-preview-20261108.1; UID {os.getuid()}; data {DATA}', flush=True)
    for page in ('servers', 'nodes', 'plans', 'bandwidth'):
        print(f'Manage: {origin}/manage/{page}', flush=True)
    os.execv(sys.executable, [sys.executable, '/app/runtime/run.py', '--data', str(DATA),
                            '--host', '0.0.0.0', '--port', '45300', '--external-origin', origin])


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError) as error:
        print(f'Container startup refused: {error}. Use a new empty directory/volume or the original container data; no recursive chown or data deletion.', file=sys.stderr)
        raise SystemExit(1)
