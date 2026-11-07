"""Independent REALITY target candidates; TLS checks are not REALITY acceptance."""
import ipaddress
import json
import math
import re
import socket
import sqlite3
import ssl
import threading
import time

from core import Error

_PROBE_SLOTS = threading.BoundedSemaphore(4)
PROBE_TIMEOUT = 5
MAX_RESULT_AGE = 300


def fields(data):
    if not isinstance(data, dict) or set(data) != {'domain', 'port', 'enabled'}:
        raise Error('需要完整的 domain、port、enabled 字段')
    domain = data['domain']
    if not isinstance(domain, str) or not domain or domain != domain.strip() or domain.endswith('..'):
        raise Error('请填写域名，不含网址、路径或空格')
    try:
        domain = domain.rstrip('.').encode('idna').decode('ascii').lower()
    except UnicodeError:
        raise Error('域名格式无效') from None
    labels = domain.split('.')
    if len(domain) > 253 or len(labels) < 2 or any(
            not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', label) for label in labels):
        raise Error('请填写完整域名，不含协议、端口或通配符')
    try:
        ipaddress.ip_address(domain)
    except ValueError:
        pass
    else:
        raise Error('请填写域名，不能使用 IP 地址')
    if type(data['port']) is not int or not 1 <= data['port'] <= 65535:
        raise Error('端口应为 1–65535 的整数')
    if type(data['enabled']) is not bool:
        raise Error('启用状态应为布尔值')
    return dict(domain=domain, port=data['port'], enabled=data['enabled'])


def _public_address(address):
    ip = ipaddress.ip_address(address)
    if getattr(ip, 'ipv4_mapped', None):
        ip = ip.ipv4_mapped
    return ip.is_global and not ip.is_multicast


def _tls_probe(domain, port, deadline, context=None):
    """Resolve once; validate every answer and connect to the pinned numeric IP."""
    if not ssl.HAS_TLSv1_3:
        return {'ok': False, 'error': 'unsupported_local_tls'}
    addresses = socket.getaddrinfo(domain, port, type=socket.SOCK_STREAM)
    if not addresses:
        return {'ok': False, 'error': 'dns_error'}
    if any(not _public_address(item[4][0]) for item in addresses):
        return {'ok': False, 'error': 'non_public_address'}
    context = context or ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.set_alpn_protocols(['h2', 'http/1.1'])
    error = 'connection_error'
    for family, kind, protocol, _, address in addresses[:8]:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return {'ok': False, 'error': 'timeout'}
        try:
            started = time.monotonic()
            with socket.socket(family, kind, protocol) as connection:
                connection.settimeout(remaining)
                connection.connect(address)
                with context.wrap_socket(connection, server_hostname=domain) as secured:
                    version, alpn = secured.version(), secured.selected_alpn_protocol()
                    eligible = version == 'TLSv1.3' and alpn == 'h2'
                    return dict(ok=eligible, error=None if eligible else 'unsupported_tls_or_alpn',
                                tls_version=version, alpn=alpn,
                                latency_ms=round((time.monotonic() - started) * 1000, 3))
        except ssl.SSLCertVerificationError:
            error = 'certificate_error'
        except (TimeoutError, socket.timeout):
            error = 'timeout'
        except ssl.SSLError:
            error = 'tls_error'
        except OSError:
            error = 'connection_error'
    return {'ok': False, 'error': error}


def probe_tls(domain, port):
    # A system DNS resolver can outlive a socket timeout. Bound both the caller's
    # wait and the number of outstanding resolver workers; never spawn unbounded
    # background threads or block service shutdown on a stuck resolver.
    slots = _PROBE_SLOTS
    if not slots.acquire(blocking=False):
        raise Error('探测任务繁忙，请稍后重试', 503)
    done, result = threading.Event(), []
    deadline = time.monotonic() + PROBE_TIMEOUT

    def worker():
        try:
            result.append(_tls_probe(domain, port, deadline))
        except socket.gaierror:
            result.append({'ok': False, 'error': 'dns_error'})
        except Exception:
            result.append({'ok': False, 'error': 'probe_error'})
        finally:
            slots.release()
            done.set()
    try:
        threading.Thread(target=worker, daemon=True).start()
    except Exception:
        slots.release()
        raise
    if not done.wait(PROBE_TIMEOUT):
        return {'ok': False, 'error': 'timeout'}
    return result[0]


class DomainPool:
    def __init__(self, registry):
        self.registry = registry

    @staticmethod
    def item(row):
        item = dict(row)
        item['enabled'] = bool(item['enabled'])
        item['result'] = json.loads(item['result']) if item['result'] else None
        return item

    @staticmethod
    def server_item(db, row, server_id):
        item = DomainPool.item(row)
        state = db.execute('SELECT * FROM reality_server_probes WHERE server_id=? AND domain_id=?',
                           (server_id, row['id'])).fetchone()
        item.update(server_id=server_id, status='not_checked', checked_at=None, result=None)
        if state and state['domain_revision'] == row['revision']:
            item.update(status=state['status'], checked_at=state['checked_at'],
                        result=json.loads(state['result']) if state['result'] else None,
                        probe_generation=state['probe_generation'])
        else:
            item['probe_generation'] = 0
        return item

    def list(self, server_id=None):
        with self.registry.connect() as db:
            db.execute('BEGIN')
            if server_id is not None and db.execute('SELECT 1 FROM servers WHERE id=?', (server_id,)).fetchone() is None:
                raise Error('服务器不存在', 404)
            rows = db.execute('SELECT * FROM reality_domains ORDER BY id').fetchall()
            return [self.server_item(db, row, server_id) if server_id is not None else self.item(row) for row in rows]

    def save(self, data, ident=None):
        data = fields(data)
        try:
            with self.registry.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                if ident is None:
                    ident = db.execute('INSERT INTO reality_domains(domain,port,enabled) VALUES(?,?,?)',
                                       (data['domain'], data['port'], int(data['enabled']))).lastrowid
                else:
                    row = db.execute('SELECT * FROM reality_domains WHERE id=?', (ident,)).fetchone()
                    if row is None:
                        raise Error('域名记录不存在', 404)
                    if any(row[key] != value for key, value in data.items()):
                        db.execute('UPDATE reality_domains SET domain=?,port=?,enabled=?,revision=revision+1,'
                                   "status='not_checked',checked_at=NULL,result=NULL WHERE id=?",
                                   (data['domain'], data['port'], int(data['enabled']), ident))
                return self.item(db.execute('SELECT * FROM reality_domains WHERE id=?', (ident,)).fetchone())
        except sqlite3.IntegrityError:
            raise Error('该域名和端口已存在', 409) from None

    def delete(self, ident):
        with self.registry.connect() as db:
            if not db.execute('DELETE FROM reality_domains WHERE id=?', (ident,)).rowcount:
                raise Error('域名记录不存在', 404)

    def probe(self, ident):
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM reality_domains WHERE id=?', (ident,)).fetchone()
            if row is None:
                raise Error('域名记录不存在', 404)
            if not row['enabled']:
                raise Error('请先启用该域名', 409)
            revision, generation = row['revision'], row['probe_generation'] + 1
            db.execute("UPDATE reality_domains SET probe_generation=?,status='checking',checked_at=NULL,result=NULL WHERE id=?",
                       (generation, ident))
        try:
            result = probe_tls(row['domain'], row['port'])
        except Exception:
            with self.registry.connect() as db:
                db.execute("UPDATE reality_domains SET status='not_checked' WHERE id=? AND revision=? AND probe_generation=?",
                           (ident, revision, generation))
            raise
        with self.registry.connect() as db:
            changed = db.execute('UPDATE reality_domains SET status=?,checked_at=?,result=? '
                                 'WHERE id=? AND revision=? AND probe_generation=?',
                                 ('available' if result['ok'] else 'unavailable', time.time(),
                                  json.dumps(result), ident, revision, generation)).rowcount
            if not changed:
                raise Error('域名已修改、删除或重新探测，请使用最新结果', 409)
            return self.item(db.execute('SELECT * FROM reality_domains WHERE id=?', (ident,)).fetchone())

    @staticmethod
    def eligible(item, now):
        result = item['result'] or {}
        latency = result.get('latency_ms')
        return (item['enabled'] and item['status'] == 'available'
                and item['checked_at'] is not None and 0 <= now - item['checked_at'] <= MAX_RESULT_AGE
                and result.get('ok') is True and result.get('tls_version') == 'TLSv1.3'
                and result.get('alpn') == 'h2' and type(latency) in (int, float)
                and math.isfinite(latency) and latency >= 0)

    def select(self, server_id=None):
        now = time.time()
        candidates = [item for item in self.list(server_id) if self.eligible(item, now)]
        if not candidates:
            raise Error('没有近期探测通过的域名，请先探测已启用域名', 409)
        return min(candidates, key=lambda item: (item['result']['latency_ms'], item['id']))

    def bind(self, node_id, data):
        """Copy a verified candidate into an existing REALITY node atomically.

        No persistent subscription: later pool edits cannot silently change a
        running node. Do not return the node's private key in this response.
        """
        if not isinstance(data, dict) or set(data) != {'domain_id', 'domain_revision'} or any(
                type(data[key]) is not int or data[key] < 1 for key in data):
            raise Error('需要正整数 domain_id 和 domain_revision，请使用最新候选记录')
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM reality_domains WHERE id=?', (data['domain_id'],)).fetchone()
            if row is None:
                raise Error('域名记录不存在', 404)
            item = self.item(row)
            if item['revision'] != data['domain_revision']:
                raise Error('域名已修改，请重新选择并探测', 409)
            node = db.execute('SELECT n.server_id,c.payload FROM nodes n LEFT JOIN node_configs c '
                              'ON c.node_id=n.id WHERE n.id=?', (node_id,)).fetchone()
            if node is None:
                raise Error('节点不存在', 404)
            item = self.server_item(db, row, node['server_id'])
            if not self.eligible(item, time.time()):
                raise Error('请先由节点所属服务器探测该域名，需在 5 分钟内通过 TLS 1.3／h2 检查', 409)
            config = json.loads(node['payload']) if node['payload'] else {}
            if config.get('protocol') != 'vless' or 'reality' not in config:
                raise Error('请先配置 REALITY 节点及密钥，再应用域名', 409)
            config['reality'].update(target_host=item['domain'], target_port=item['port'],
                                     server_name=item['domain'])
            self.registry._configure_node(db, node_id, config)
            revision = db.execute('SELECT revision FROM server_configs WHERE server_id=?',
                                  (node['server_id'],)).fetchone()[0]
            return dict(node_id=node_id, domain_id=item['id'], domain_revision=item['revision'],
                        domain=item['domain'], port=item['port'], server_revision=revision)
