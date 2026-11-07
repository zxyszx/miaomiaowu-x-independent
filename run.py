#!/usr/bin/env python3
"""Run the independent preview with isolated data and explicit listen address."""
import argparse, os, sys, stat
from pathlib import Path
HERE = Path(__file__).resolve().parent
BUILD_ID = 'independent-preview-20261108.1'
ap = argparse.ArgumentParser(description=__doc__); ap.add_argument('--host', default='127.0.0.1'); ap.add_argument('--external-origin'); ap.add_argument('--port', type=int, default=45300); ap.add_argument('--data', type=Path); ap.add_argument('--version', action='store_true'); args = ap.parse_args()
if args.version:
    print(BUILD_ID); raise SystemExit(0)
if sys.version_info[:2] != (3, 12): ap.error('this tested build requires Python 3.12.x')
if not 1 <= args.port <= 65535: ap.error('port out of range')
if args.data is None: ap.error('--data is required; use an external persistent directory')
data_input = args.data.expanduser()
if data_input.is_symlink(): ap.error('--data must not be a symlink')
data = data_input.resolve(strict=False)
if data == HERE or HERE in data.parents: ap.error('--data must be outside the code directory')
marker = data / '.miaomiaowu-independent-data'
if data.exists() and not data.is_dir(): ap.error('--data must be a directory')
if marker.exists() or marker.is_symlink():
    marker_stat = marker.lstat()
    if not stat.S_ISREG(marker_stat.st_mode): ap.error('data marker must be a regular file')
    if marker.read_text() != 'product=miaomiaowu-x-independent\nformat=1\n': ap.error('data marker content is invalid')
elif data.exists() and any(data.iterdir()):
    ap.error('refusing non-empty unmarked data directory')
data.mkdir(mode=0o700, parents=True, exist_ok=True)
if not marker.exists():
    marker.write_text('product=miaomiaowu-x-independent\nformat=1\n'); os.chmod(marker, 0o600)
env = dict(os.environ); env['PYTHONPATH'] = str(HERE / 'independent')
command = [sys.executable, str(HERE / 'independent' / 'server.py'), '--host', args.host, '--port', str(args.port), '--data', str(data)]
if args.external_origin is not None:
    command += ['--external-origin', args.external_origin]
os.execve(sys.executable, command, env)
