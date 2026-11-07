#!/bin/bash
# Installer revision install-047.1; runtime remains 044.3.
set -euo pipefail
umask 022
die() { echo "Installation refused: $*" >&2; exit 1; }
[[ $(id -u) = 0 ]] || die "use sudo bash"
[[ $(uname -s) = Linux ]] || die "Linux required"
case $(uname -m) in x86_64|aarch64|arm64) ;; *) die "supported architectures: x86_64/aarch64";; esac
if command -v systemctl >/dev/null && [[ -d /run/systemd/system ]]; then
 SERVICE_MANAGER=systemd
elif command -v rc-service >/dev/null; then
 die "OpenRC detected: native installation not implemented/validated yet"
else die "no supported running service manager; systemd required (Docker/containers unsupported)"; fi
PREFIX=${MMX_PREFIX:-/opt/miaomiaowu-x-independent}
DATA=${MMX_DATA:-/var/lib/miaomiaowu-x-independent}
UNIT=miaomiaowu-x-independent.service
SERVICE_USER=mmx-independent
# Restricted paths also prevent unit-file/shell interpolation.
for p in "$PREFIX" "$DATA"; do
 [[ $p =~ ^/[a-zA-Z0-9_/-]+$ && $p != / && $p != */ && $p != *//* ]] || die "unsafe path: $p"
 [[ ! -e $p && ! -L $p ]] || die "existing path: $p"
 part=$p
 while [[ $part != / ]]; do [[ ! -L $part ]] || die "linked ancestor: $part"; part=$(dirname "$part"); done
 [[ -d $(dirname "$p") ]] || die "parent directory must exist: $p"
done
[[ $DATA != "$PREFIX"/* && $PREFIX != "$DATA"/* && $DATA != "$PREFIX" ]] || die "code/data must be disjoint"
[[ $(systemctl show "$UNIT" -p LoadState --value) = not-found ]] || die "service already exists (including disabled)"
! getent passwd "$SERVICE_USER" >/dev/null || die "service account already exists"
! getent group "$SERVICE_USER" >/dev/null || die "service group already exists"
# Package management only supplies bootstrap tools, never replaces system Python.
if command -v apt-get >/dev/null; then
 PM=apt
elif command -v dnf >/dev/null; then PM=dnf
elif command -v yum >/dev/null; then PM=yum
elif command -v apk >/dev/null; then die "apk detected: Alpine/musl installer not implemented/validated yet"
else die "no supported package manager (apt/dnf/yum; apk recognized but unsupported)"; fi
echo "Installer install-047.1: Linux $(uname -m), package manager $PM"
if ! command -v curl >/dev/null || ! command -v python3 >/dev/null; then
 case $PM in apt) apt-get update; apt-get install -y ca-certificates curl python3;; dnf|yum) "$PM" install -y ca-certificates curl python3;; esac
fi
command -v useradd >/dev/null || die "useradd required"
python3 - <<'PY'
import socket
with socket.socket() as s:
 s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
 s.bind(('127.0.0.1',45300))
PY
tmp=$(mktemp -d /tmp/mmx-install-047.XXXXXXXX)
cleanup() { rm -rf -- "$tmp"; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'echo "Installation failed. Retained code/data if created; inspect journalctl -u miaomiaowu-x-independent.service. No automatic retry or data deletion." >&2' ERR
COMMIT=74fddc380e19b51650e04220e1d8eb815bd5f21e
curl -fL --proto '=https' --proto-redir '=https' --tlsv1.2 --connect-timeout 30 --max-time 180 "https://github.com/zxyszx/miaomiaowu-x-independent/archive/$COMMIT.tar.gz" -o "$tmp/source.tar.gz"
python3 - "$tmp/source.tar.gz" "$tmp/code" <<'PY'
import hashlib,json,pathlib,sys,tarfile
archive,out=map(pathlib.Path,sys.argv[1:])
with tarfile.open(archive) as t:
 members=t.getmembers()
 if len(members)>2000 or sum(m.size for m in members)>40*1024*1024: raise SystemExit('archive too large')
 seen={}; roots=set()
 for m in members:
  p=pathlib.PurePosixPath(m.name)
  if p.is_absolute() or '..' in p.parts or not p.parts or m.name in seen: raise SystemExit('unsafe/duplicate archive path')
  if not (m.isdir() or m.isfile()): raise SystemExit('links and special files forbidden')
  seen[m.name]=m; roots.add(p.parts[0])
 if len(roots)!=1: raise SystemExit('ambiguous archive root')
 base=next(iter(roots))
 manifest=seen.get(base+'/MANIFEST.json')
 if not manifest or not manifest.isfile(): raise SystemExit('missing manifest')
 raw=t.extractfile(manifest).read()
 if hashlib.sha256(raw).hexdigest()!='f3297df539462c73a09550c9405a5a47d05aeabe0c815fedc68ab69f95c05af9': raise SystemExit('manifest SHA256 mismatch')
 doc=json.loads(raw)
 if doc['build_id']!='independent-preview-20261107.3': raise SystemExit('wrong runtime build')
 expected={base+'/MANIFEST.json'}
 for e in doc['files']:
  p=pathlib.PurePosixPath(e['path'])
  if p.is_absolute() or '..' in p.parts: raise SystemExit('unsafe manifest path')
  name=base+'/'+e['path']
  if name in expected: raise SystemExit('duplicate manifest path')
  expected.add(name)
  m=seen.get(name)
  if not m or not m.isfile(): raise SystemExit('missing/non-file payload')
  b=t.extractfile(m).read()
  if len(b)!=e['bytes'] or hashlib.sha256(b).hexdigest()!=e['sha256']: raise SystemExit('payload hash mismatch')
 if {m.name for m in members if m.isfile()}!=expected: raise SystemExit('unexpected files')
 allowed_dirs={base}
 for name in expected:
  allowed_dirs.update(str(p) for p in pathlib.PurePosixPath(name).parents if str(p)!='.')
 if any(m.name.rstrip('/') not in allowed_dirs for m in members if m.isdir()): raise SystemExit('unexpected directories')
 # Only now create files. Never use tar extract with untrusted metadata.
 out.mkdir(mode=0o755)
 for name in sorted(expected):
  dest=out/pathlib.PurePosixPath(name).relative_to(base)
  dest.parent.mkdir(parents=True,exist_ok=True)
  dest.write_bytes(t.extractfile(seen[name]).read()); dest.chmod(0o644)
print('Verified pinned runtime manifest and every archive entry')
PY
# Reserve fresh final paths; on failure keep them for diagnosis.
mkdir -m 755 "$PREFIX"
cp -R "$tmp/code"/. "$PREFIX/"
mkdir -m 700 "$DATA"
curl -fL --proto '=https' --proto-redir '=https' --tlsv1.2 --connect-timeout 30 --max-time 180 https://astral.sh/uv/0.13.7/install.sh -o "$tmp/uv-install.sh"
UV_UNMANAGED_INSTALL="$PREFIX/tools" sh "$tmp/uv-install.sh"
export UV_PYTHON_INSTALL_DIR="$PREFIX/python" UV_PYTHON_BIN_DIR="$PREFIX/python-bin" UV_CACHE_DIR="$tmp/uv-cache"
"$PREFIX/tools/uv" python install 3.12
"$PREFIX/tools/uv" venv --python 3.12 --managed-python "$PREFIX/venv"
"$PREFIX/venv/bin/python" -c 'import sys; assert sys.version_info[:2]==(3,12)'
groupadd --system "$SERVICE_USER"
useradd --system --gid "$SERVICE_USER" --home-dir "$DATA" --shell /usr/sbin/nologin "$SERVICE_USER"
chown "$SERVICE_USER:$SERVICE_USER" "$DATA"
chown -R root:root "$PREFIX"
chmod -R go-w "$PREFIX"
# Bytecode stays out of root-owned code. Service writes only its data.
unit_tmp="$tmp/$UNIT"
printf '%s\n' '[Unit]' 'Description=Miaomiaowu X independent incremental preview' 'After=network.target' '[Service]' 'Type=simple' "User=$SERVICE_USER" "Group=$SERVICE_USER" "WorkingDirectory=$PREFIX" "ExecStart=$PREFIX/venv/bin/python $PREFIX/run.py --data $DATA" 'Environment=PYTHONDONTWRITEBYTECODE=1' 'Restart=no' 'UMask=0077' 'NoNewPrivileges=yes' 'ProtectSystem=strict' "ReadWritePaths=$DATA" 'ProtectHome=yes' 'PrivateTmp=yes' '[Install]' 'WantedBy=multi-user.target' > "$unit_tmp"
# noclobber also prevents replacing disabled/dangling unit-file conflicts.
(set -C; : > "/etc/systemd/system/$UNIT")
install -m 644 "$unit_tmp" "/etc/systemd/system/$UNIT"
systemctl daemon-reload
systemctl enable --now "$UNIT"
"$PREFIX/venv/bin/python" - <<'PY'
import json,time,urllib.request
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
for i in range(30):
 try:
  with opener.open('http://127.0.0.1:45300/health',timeout=2) as r: d=json.load(r)
  if d.get('service')=='independent-x-core': break
 except Exception: pass
 time.sleep(1)
else: raise SystemExit('health failed; inspect systemd journal')
print('Health verified; runtime independent-preview-20261107.3')
PY
systemctl is-active --quiet "$UNIT"
echo "Service: $UNIT; data: $DATA; administrator token file: $DATA/admin-token"
echo 'Access: ssh -N -L 45300:127.0.0.1:45300 user@server then http://127.0.0.1:45300/manage/servers'

