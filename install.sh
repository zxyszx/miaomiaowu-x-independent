#!/bin/bash
# Installer revision install-050.1; pinned runtime independent-preview-20261108.1.
set -euo pipefail
umask 022
die() { echo "Installation refused: $*" >&2; exit 1; }
MODE=${1:-install}
[[ $# -le 1 && ( $MODE = install || $MODE = update ) ]] || die "usage: bash install.sh [install|update]"
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
UNIT=${MMX_UNIT:-miaomiaowu-x-independent.service}
SERVICE_USER=${MMX_SERVICE_USER:-mmx-independent}
[[ $UNIT =~ ^[a-z][a-z0-9-]{0,55}\.service$ ]] || die "unsafe unit name"
[[ $SERVICE_USER =~ ^[a-z][a-z0-9-]{0,30}$ ]] || die "unsafe service account"
HOST=${MMX_HOST:-0.0.0.0}
PORT=${MMX_PORT:-${PORT:-45300}}
# MMX_PORT takes precedence over PORT.
# Restricted paths also prevent unit-file/shell interpolation.
for p in "$PREFIX" "$DATA"; do
 [[ $p =~ ^/[a-zA-Z0-9_/-]+$ && $p != / && $p != */ && $p != *//* ]] || die "unsafe path: $p"
 if [[ $MODE = install ]]; then [[ ! -e $p && ! -L $p ]] || die "existing path: $p"; fi
 part=$p
 while [[ $part != / ]]; do [[ ! -L $part ]] || die "linked ancestor: $part"; part=$(dirname "$part"); done
 [[ -d $(dirname "$p") ]] || die "parent directory must exist: $p"
done
[[ $DATA != "$PREFIX"/* && $PREFIX != "$DATA"/* && $DATA != "$PREFIX" ]] || die "code/data must be disjoint"
if [[ $MODE = install ]]; then
 [[ $(systemctl show "$UNIT" -p LoadState --value) = not-found ]] || die "service already exists (including disabled)"
 ! getent passwd "$SERVICE_USER" >/dev/null || die "service account already exists"
 ! getent group "$SERVICE_USER" >/dev/null || die "service group already exists"
fi
# Package management only supplies bootstrap tools, never replaces system Python.
if command -v apt-get >/dev/null; then
 PM=apt
elif command -v dnf >/dev/null; then PM=dnf
elif command -v yum >/dev/null; then PM=yum
elif command -v apk >/dev/null; then die "apk detected: Alpine/musl installer not implemented/validated yet"
else die "no supported package manager (apt/dnf/yum; apk recognized but unsupported)"; fi
echo "Installer install-050.1 ($MODE): Linux $(uname -m), package manager $PM"
if ! command -v curl >/dev/null || ! command -v python3 >/dev/null; then
 case $PM in apt) apt-get update; apt-get install -y ca-certificates curl python3;; dnf|yum) "$PM" install -y ca-certificates curl python3;; esac
fi
command -v useradd >/dev/null || die "useradd required"
lock=/run/mmx-$UNIT-install.lock
mkdir -m 700 "$lock" || die "another installer is running, or retained lock requires diagnosis: $lock"
tmp=$(mktemp -d /tmp/mmx-install-050.XXXXXXXX)
STOPPED=0
UNIT_CHANGED=0
BACKUP=
cleanup() { rm -rf -- "$tmp"; rmdir "$lock"; }
trap cleanup EXIT
failure() {
 local rc=$1
 trap - ERR INT TERM
 set +e
 if [[ $MODE = update && $STOPPED = 1 ]]; then
  systemctl stop "$UNIT"
  if [[ $UNIT_CHANGED = 1 ]]; then install -m 644 "$BACKUP/original.service" "/etc/systemd/system/$UNIT"; fi
  systemctl daemon-reload
  if systemctl start "$UNIT" && systemctl is-active --quiet "$UNIT"; then
   echo "Original unit restored and old service started. Data was NOT restored or deleted." >&2
  else echo "Original service recovery failed; keep data and inspect the journal before further action." >&2; fi
 fi
 echo "Failed (rc=$rc). Code/data/backup retained; journalctl -u $UNIT -n 80 --no-pager. Backup: ${BACKUP:-not created}. Do not rerun fresh installation." >&2
 exit "$rc"
}
trap 'failure $?' ERR
trap 'failure 130' INT
trap 'failure 143' TERM
# Validate before any service stop, and never interpolate an untrusted URL into a unit.
python3 - "$HOST" "$PORT" <<'PY'
import ipaddress,sys
try:
 assert str(ipaddress.IPv4Address(sys.argv[1])) in ('0.0.0.0','127.0.0.1')
 assert sys.argv[2].isascii() and sys.argv[2].isdigit() and 1<=int(sys.argv[2])<=65535
except (ValueError,AssertionError): raise SystemExit('MMX_HOST must be 0.0.0.0 or 127.0.0.1 (local health required); PORT/MMX_PORT must be 1..65535')
PY
PORT=$((10#$PORT))
ORIGIN=${MMX_EXTERNAL_ORIGIN:-}
if [[ -z $ORIGIN ]]; then
 if [[ $HOST = 127.* ]]; then ORIGIN=http://$HOST:$PORT
 else
  # Require agreement between two independent HTTPS IPv4 observations. This
  # identifies egress IP only; inbound NAT/firewall reachability is not assumed.
  ip_one=$(curl -4fsS --proto '=https' --proto-redir '=https' --tlsv1.2 --connect-timeout 5 --max-time 12 https://api.ipify.org) || ip_one=
  ip_two=$(curl -4fsS --proto '=https' --proto-redir '=https' --tlsv1.2 --connect-timeout 5 --max-time 12 https://ipv4.icanhazip.com) || ip_two=
  if ! python3 -c 'import ipaddress,sys; a,b=sys.argv[1:]; ip=ipaddress.IPv4Address(a); assert a==b and ip.is_global and not ip.is_multicast' "$ip_one" "$ip_two" 2>/dev/null; then
   echo "In your root terminal, enter the actual browser origin including port $PORT, then rerun:" >&2
   echo "read -r -p 'External origin: ' MMX_EXTERNAL_ORIGIN" >&2
   echo 'export MMX_EXTERNAL_ORIGIN' >&2
   echo "curl -fL https://raw.githubusercontent.com/zxyszx/miaomiaowu-x-independent/main/install.sh | bash -s -- $MODE" >&2
   die "public IPv4 detection failed/disagreed; service/data unchanged"
  fi
  ORIGIN=http://$ip_one:$PORT
  echo "Detected public egress IPv4 $ip_one; inbound reachability still depends on routing/firewall. Prefer MMX_EXTERNAL_ORIGIN for NAT/domain use."
 fi
fi
python3 - "$ORIGIN" "$PORT" <<'ORIGIN_CHECK'
import ipaddress,re,sys,urllib.parse
try:
 raw=sys.argv[1]; p=urllib.parse.urlsplit(raw)
 assert re.fullmatch(r'https?://[A-Za-z0-9.-]+(?::[0-9]+)?',raw)
 assert p.hostname and not p.username and not p.password
 assert (p.port or (443 if p.scheme=='https' else 80))==int(sys.argv[2]), 'origin port must match listen port for direct access'
 try: assert not ipaddress.ip_address(p.hostname).is_unspecified
 except ValueError: assert all(re.fullmatch(r'[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?',s) for s in p.hostname.split('.'))
except (ValueError,AssertionError) as e: raise SystemExit('Invalid MMX_EXTERNAL_ORIGIN: fixed http[s]://actual-IP-or-domain:port required; no wildcard, credentials, paths, whitespace or 0.0.0.0. '+str(e))
ORIGIN_CHECK
if [[ $MODE = install ]]; then
 python3 - "$HOST" "$PORT" <<'PORT_CHECK'
import socket,sys
with socket.socket() as s:
 s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
 s.bind((sys.argv[1],int(sys.argv[2])))
PORT_CHECK
fi
COMMIT=9d5bad5cc9aef2d30fbcabe9450b5a4a022396ec
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
 if hashlib.sha256(raw).hexdigest()!='4fb124fd89c0c375667dbdc4562c583ce0d87bc967c8c3dfb11e0dee60071236': raise SystemExit('manifest SHA256 mismatch')
 doc=json.loads(raw)
 if doc['build_id']!='independent-preview-20261108.1': raise SystemExit('wrong runtime build')
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
CODE=$PREFIX
if [[ $MODE = update ]]; then
 # Only the known 047 layout is eligible. This release changes server/run but
 # no data/schema implementation, making old-code recovery compatible.
 systemctl is-active --quiet "$UNIT" || die "update requires the recognized old service active"
 [[ $(systemctl show "$UNIT" -p FragmentPath --value) = /etc/systemd/system/$UNIT ]] || die "unknown unit location"
 [[ -z $(systemctl show "$UNIT" -p DropInPaths --value) ]] || die "unit overrides require manual review before update"
 python3 - "$PREFIX" "$DATA" "$UNIT" "$SERVICE_USER" "$tmp/code" "$tmp/token.sha256" <<'PY'
import hashlib,json,pathlib,pwd,stat,sys
prefix,data,unit,user,new,token_hash=sys.argv[1:]
prefix,data,new=map(pathlib.Path,(prefix,data,new))
def regular(p,uid):
 s=p.lstat(); assert stat.S_ISREG(s.st_mode) and s.st_uid==uid and not s.st_mode & 0o022, 'unsafe file: '+str(p)
assert prefix.is_dir() and prefix.stat().st_uid==0 and not prefix.stat().st_mode & 0o022
# Upgrade reuses this Python as root for verification, so protect its complete
# environment as well as the allowlisted application source.
for directory in (prefix/'python',prefix/'venv'):
 assert directory.is_dir() and not directory.is_symlink()
 for p in [directory,*directory.rglob('*')]:
  s=p.lstat(); assert s.st_uid==0, 'runtime is not root-owned: '+str(p)
  if p.is_symlink():
   assert p.resolve().is_relative_to(prefix), 'runtime link escapes installation: '+str(p)
  else: assert not s.st_mode & 0o022, 'runtime is writable by service/others: '+str(p)
uid=pwd.getpwnam(user).pw_uid; assert uid!=0
assert data.is_dir() and data.stat().st_uid==uid and stat.S_IMODE(data.stat().st_mode)==0o700
marker=data/'.miaomiaowu-independent-data'; regular(marker,uid)
assert marker.read_text()=='product=miaomiaowu-x-independent\nformat=1\n'
regular(data/'admin-token',uid)
regular(data/'registry.db',uid)
for p in data.rglob('*'): assert not p.is_symlink() and (p.is_file() or p.is_dir()), 'unsupported data entry'
old_manifest=prefix/'MANIFEST.json'; regular(old_manifest,0); raw=old_manifest.read_bytes()
assert hashlib.sha256(raw).hexdigest()=='f3297df539462c73a09550c9405a5a47d05aeabe0c815fedc68ab69f95c05af9', 'only original 047/.3 installation can be upgraded by this release'
old=json.loads(raw)
for entry in old['files']:
 p=prefix/entry['path']; regular(p,0); b=p.read_bytes()
 assert len(b)==entry['bytes'] and hashlib.sha256(b).hexdigest()==entry['sha256'], 'old installation modified: '+entry['path']
 if entry['path'].startswith(('independent/','web/')) and entry['path']!='independent/server.py':
  assert (new/entry['path']).read_bytes()==b, 'data/schema or business implementation differs; rollback contract requires review'
unitfile=pathlib.Path('/etc/systemd/system')/unit; regular(unitfile,0)
expected='\n'.join(['[Unit]','Description=Miaomiaowu X independent incremental preview','After=network.target','[Service]','Type=simple',f'User={user}',f'Group={user}',f'WorkingDirectory={prefix}',f'ExecStart={prefix}/venv/bin/python {prefix}/run.py --data {data}','Environment=PYTHONDONTWRITEBYTECODE=1','Restart=no','UMask=0077','NoNewPrivileges=yes','ProtectSystem=strict',f'ReadWritePaths={data}','ProtectHome=yes','PrivateTmp=yes','[Install]','WantedBy=multi-user.target',''])
assert unitfile.read_text()==expected, 'unknown or modified 047 unit; refusing takeover'
pathlib.Path(token_hash).write_text(hashlib.sha256((data/'admin-token').read_bytes()).hexdigest())
print('Recognized unmodified 047 installation; data/schema implementation unchanged')
PY
 CODE=$PREFIX-build-20261108-1
 [[ ! -e $CODE && ! -L $CODE ]] || die "new build directory exists; diagnose previous attempt: $CODE"
 "$PREFIX/venv/bin/python" -c 'import sys; assert sys.version_info[:2]==(3,12)'
fi
# Reserve only fresh code paths; never overwrite the old build.
mkdir -m 755 "$CODE"
cp -R "$tmp/code"/. "$CODE/"
chown -R root:root "$CODE"
chmod -R go-w "$CODE"
chmod 755 "$CODE" "$CODE/independent" "$CODE/web"
if [[ $MODE = install ]]; then
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
else
 BACKUP=$PREFIX-backup-$(date -u +%Y%m%dT%H%M%SZ)-$$
 mkdir -m 700 "$BACKUP"
 cp -p "/etc/systemd/system/$UNIT" "$BACKUP/original.service"
 STOPPED=1
 systemctl stop "$UNIT"
 if systemctl is-active --quiet "$UNIT"; then failure 1; fi
 # With all service processes stopped, verify DB integrity and create a
 # consistent snapshot. Backup stays root-only and is never restored here.
 python3 - "$DATA" <<'PY'
import pathlib,sqlite3,sys
data=pathlib.Path(sys.argv[1])
for p in data.rglob('*'): assert not p.is_symlink() and (p.is_file() or p.is_dir()), 'unsupported data entry'
with sqlite3.connect((data/'registry.db').as_uri()+'?mode=ro',uri=True) as c:
 assert c.execute('PRAGMA quick_check').fetchall()==[('ok',)]
PY
 tar -czf "$BACKUP/data.tar.gz" -C "$(dirname "$DATA")" "$(basename "$DATA")"
 chmod 600 "$BACKUP/data.tar.gz"
 echo "Stopped-data backup: $BACKUP/data.tar.gz (root-only; contains credentials, do not share)"
fi
# Check the actual requested bind address/port, including the stopped old port.
python3 - "$HOST" "$PORT" <<'PY'
import socket,sys
with socket.socket() as s:
 s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
 s.bind((sys.argv[1],int(sys.argv[2])))
PY
unit_tmp="$tmp/$UNIT"
printf '%s\n' '[Unit]' 'Description=Miaomiaowu X independent incremental preview' 'After=network.target' '[Service]' 'Type=simple' "User=$SERVICE_USER" "Group=$SERVICE_USER" "WorkingDirectory=$CODE" "ExecStart=$PREFIX/venv/bin/python $CODE/run.py --data $DATA --host $HOST --port $PORT --external-origin $ORIGIN" 'Environment=PYTHONDONTWRITEBYTECODE=1' 'Restart=no' 'UMask=0077' 'NoNewPrivileges=yes' 'ProtectSystem=strict' "ReadWritePaths=$DATA" 'ProtectHome=yes' 'PrivateTmp=yes' '[Install]' 'WantedBy=multi-user.target' > "$unit_tmp"
if [[ $MODE = install ]]; then (set -C; : > "/etc/systemd/system/$UNIT"); fi
UNIT_CHANGED=1
install -m 644 "$unit_tmp" "/etc/systemd/system/$UNIT"
systemctl daemon-reload
if [[ $MODE = install ]]; then systemctl enable --now "$UNIT"; else systemctl start "$UNIT"; fi
"$PREFIX/venv/bin/python" - "$PORT" "$ORIGIN" "$DATA" "$tmp/token.sha256" "$MODE" <<'PY'
import hashlib,json,pathlib,sys,time,urllib.request
port,origin,data,old_hash,mode=sys.argv[1:]; data=pathlib.Path(data)
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
for i in range(30):
 try:
  with opener.open('http://127.0.0.1:'+port+'/health',timeout=2) as r: d=json.load(r)
  if d.get('service')=='independent-x-core': break
 except Exception: pass
 time.sleep(1)
else: raise SystemExit('health failed; inspect systemd journal')
assert (data/'.miaomiaowu-independent-data').read_text()=='product=miaomiaowu-x-independent\nformat=1\n'
token=(data/'admin-token').read_bytes()
if mode=='update': assert hashlib.sha256(token).hexdigest()==pathlib.Path(old_hash).read_text(), 'token changed'
request=urllib.request.Request('http://127.0.0.1:'+port+'/api/servers',headers={'Origin':origin,'Authorization':'Bearer '+token.decode().strip()})
with opener.open(request,timeout=5) as r: assert r.status==200 and isinstance(json.load(r),list)
print('Health and fixed-Origin authorized read verified; runtime independent-preview-20261108.1')
PY
systemctl is-active --quiet "$UNIT"
STOPPED=0
echo "Service: $UNIT; build: independent-preview-20261108.1; listen: $HOST:$PORT; data: $DATA"
echo "Administrator token file: $DATA/admin-token (read only in your own terminal, never share)"
for page in servers nodes plans bandwidth; do echo "Manage: $ORIGIN/manage/$page"; done
echo 'HTTP is unencrypted; restrict firewall source addresses. HTTPS requires separately configured TLS termination. Public egress detection does not prove inbound reachability.'
if [[ $HOST = 127.* ]]; then echo "Loopback option: ssh -N -L $PORT:$HOST:$PORT user@server"; fi
if [[ $MODE = update ]]; then echo "Upgrade complete. Old code: $PREFIX; stopped-data backup: $BACKUP. No database restore/deletion performed."; fi

