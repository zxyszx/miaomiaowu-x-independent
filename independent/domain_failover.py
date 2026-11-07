"""Opt-in, poll-driven REALITY target failover with conservative rollback."""
import copy
import json
import time

from core import Error
from domain_pool import DomainPool, MAX_RESULT_AGE

CONFIRM_TIMEOUT = 120
FAILURE_WINDOW = 3600


class Failover:
    def __init__(self, registry):
        self.registry = registry

    @staticmethod
    def public(row):
        if row is None:
            return {'policy': None, 'state': None}
        state = json.loads(row['state'])
        # Internal snapshots contain private keys. They must never enter API responses.
        return {'policy': json.loads(row['settings']), 'state': {
            key: state[key] for key in ('phase', 'current_domain_id', 'failures', 'cooldown_until',
                                       'message', 'pending_revision', 'pending_since') if key in state}}

    @staticmethod
    def matches(config, domain):
        reality = config.get('reality', {})
        return (reality.get('target_host') == domain['domain']
                and reality.get('target_port') == domain['port']
                and reality.get('server_name') == domain['domain'])

    def policy(self, node_id, data=None, delete=False):
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            node = db.execute('SELECT n.server_id,c.payload FROM nodes n LEFT JOIN node_configs c '
                              'ON c.node_id=n.id WHERE n.id=?', (node_id,)).fetchone()
            if node is None:
                raise Error('节点不存在', 404)
            old = db.execute('SELECT * FROM reality_failover WHERE node_id=?', (node_id,)).fetchone()
            if delete or data is not None:
                if old and json.loads(old['state'])['phase'] in ('pending', 'rollback_pending'):
                    raise Error('正在等待切换或回退确认，请待处理结束后修改策略', 409)
            if delete:
                db.execute('DELETE FROM reality_failover WHERE node_id=?', (node_id,))
                return {'deleted': True}
            if data is not None:
                self.validate(data)
                config = json.loads(node['payload']) if node['payload'] else {}
                if config.get('protocol') != 'vless' or 'reality' not in config or not config.get('enabled'):
                    raise Error('仅已启用的 REALITY 节点可配置自动切换', 409)
                if db.execute('SELECT 1 FROM agents WHERE server_id=?', (node['server_id'],)).fetchone() is None:
                    raise Error('节点所属服务器尚未接入 Agent', 409)
                domains = [db.execute('SELECT * FROM reality_domains WHERE id=?', (ident,)).fetchone()
                           for ident in data['domain_ids']]
                if any(domain is None or not domain['enabled'] for domain in domains):
                    raise Error('请使用已启用的域名候选', 409)
                current = next((domain for domain in domains if self.matches(config, domain)), None)
                if current is None:
                    raise Error('候选列表需包含节点当前域名，且目标地址与 SNI 一致', 409)
                encoded = json.dumps(data, sort_keys=True)
                if (old and old['settings'] == encoded and json.loads(old['state'])['phase'] == 'monitoring'
                        and json.loads(old['state'])['expected_payload'] == node['payload']
                        and json.loads(old['state'])['server_id'] == node['server_id']):
                    return self.public(old)  # Saving identical settings cannot clear the debounce/cooldown.
                state = dict(phase='monitoring', current_domain_id=current['id'], failures=0,
                             cooldown_until=0, message='monitoring', server_id=node['server_id'],
                             expected_payload=node['payload'], failure_key=None, last_failure_at=None)
                db.execute('INSERT INTO reality_failover(node_id,settings,state) VALUES(?,?,?) '
                           'ON CONFLICT(node_id) DO UPDATE SET settings=excluded.settings,state=excluded.state',
                           (node_id, encoded, json.dumps(state)))
            return self.public(db.execute('SELECT * FROM reality_failover WHERE node_id=?', (node_id,)).fetchone())

    @staticmethod
    def validate(data):
        if (not isinstance(data, dict) or set(data) != {'domain_ids', 'failure_threshold', 'cooldown_seconds'}
                or not isinstance(data['domain_ids'], list) or not 2 <= len(data['domain_ids']) <= 16
                or any(type(ident) is not int or ident < 1 for ident in data['domain_ids'])
                or len(set(data['domain_ids'])) != len(data['domain_ids'])
                or type(data['failure_threshold']) is not int or not 2 <= data['failure_threshold'] <= 10
                or type(data['cooldown_seconds']) is not int or not 300 <= data['cooldown_seconds'] <= 86400):
            raise Error('需要2–16个不同候选、2–10轮失败阈值及300–86400秒冷却时间')

    @staticmethod
    def write(db, node_id, state):
        db.execute('UPDATE reality_failover SET state=? WHERE node_id=?', (json.dumps(state), node_id))

    def tick(self, db, server_id):
        """Called in the configuration poll's transaction; never performs network I/O."""
        rows = db.execute('SELECT f.*,n.server_id,c.payload FROM reality_failover f '
                          'JOIN nodes n ON n.id=f.node_id LEFT JOIN node_configs c ON c.node_id=n.id '
                          'WHERE n.server_id=? ORDER BY n.id', (server_id,)).fetchall()
        now = time.time()
        for row in rows:
            state, settings = json.loads(row['state']), json.loads(row['settings'])
            if state['phase'] == 'paused':
                continue
            if state['server_id'] != server_id or row['payload'] != state['expected_payload']:
                state.update(phase='paused', message='node_changed')
                self.write(db, row['node_id'], state)
                continue
            desired = self.registry._snapshot(db, server_id)
            receipt = db.execute('SELECT * FROM runtime_receipts WHERE server_id=?', (server_id,)).fetchone()
            confirmed = (receipt is not None and receipt['revision'] == desired['revision']
                         and receipt['digest'] == desired['digest'] and 0 <= now-receipt['reported_at'] < 45)
            if state['phase'] in ('pending', 'rollback_pending'):
                if confirmed and receipt['revision'] >= state['pending_revision']:
                    if state['phase'] == 'pending':
                        state.update(phase='monitoring', message='applied', failures=0, failure_key=None,
                                     last_failure_at=None, cooldown_until=now+settings['cooldown_seconds'])
                    else:
                        state.update(phase='paused', message='rolled_back')
                    for key in ('previous_payload', 'previous_domain_id', 'pending_revision', 'pending_since'):
                        state.pop(key, None)
                elif now-state['pending_since'] >= CONFIRM_TIMEOUT:
                    if state['phase'] == 'pending':
                        restored = self.registry._configure_node(db, row['node_id'], json.loads(state['previous_payload']))
                        revision = db.execute('SELECT revision FROM server_configs WHERE server_id=?', (server_id,)).fetchone()[0]
                        state.update(phase='rollback_pending', message='rollback_waiting', failures=0,
                                     current_domain_id=state['previous_domain_id'],
                                     expected_payload=json.dumps(restored, sort_keys=True),
                                     pending_revision=revision, pending_since=now)
                    else:
                        # Do not claim rollback succeeded when no execution receipt arrived.
                        state.update(phase='paused', message='rollback_unconfirmed')
                self.write(db, row['node_id'], state)
                continue
            domain = db.execute('SELECT * FROM reality_domains WHERE id=?', (state['current_domain_id'],)).fetchone()
            if domain is None or not domain['enabled'] or not self.matches(json.loads(row['payload']), domain):
                state.update(phase='paused', message='candidate_changed')
                self.write(db, row['node_id'], state)
                continue
            item = DomainPool.server_item(db, domain, server_id)
            if DomainPool.eligible(item, now):
                state.update(failures=0, failure_key=None, last_failure_at=None, message='healthy')
            elif (item['status'] == 'unavailable' and item['checked_at'] is not None
                  and 0 <= now-item['checked_at'] <= MAX_RESULT_AGE):
                # Count completed probe rounds, never each poll or each retry attempt.
                terminal = db.execute("SELECT 1 FROM reality_probe_jobs WHERE server_id=? AND domain_id=? "
                                      "AND domain_revision=? AND probe_generation=? AND status='completed'",
                                      (server_id, domain['id'], domain['revision'], item['probe_generation'])).fetchone()
                key = [domain['revision'], item['probe_generation']]
                if terminal and key != state['failure_key']:
                    previous = state['last_failure_at']
                    state['failures'] = (state['failures'] if previous is not None and 0 <= now-previous <= FAILURE_WINDOW else 0)+1
                    state.update(failure_key=key, last_failure_at=now, message='failure_observed')
                if terminal and state['failures'] >= settings['failure_threshold']:
                    if not confirmed:
                        state['message'] = 'waiting_current_receipt'
                    elif now < state['cooldown_until']:
                        state['message'] = 'cooldown'
                    else:
                        candidates = [db.execute('SELECT * FROM reality_domains WHERE id=?', (ident,)).fetchone()
                                      for ident in settings['domain_ids'] if ident != domain['id']]
                        target = next((candidate for candidate in candidates if candidate is not None
                                       and DomainPool.eligible(DomainPool.server_item(db, candidate, server_id), now)), None)
                        if target is None:
                            state['message'] = 'no_eligible_backup'
                        else:
                            config = copy.deepcopy(json.loads(row['payload']))
                            config['reality'].update(target_host=target['domain'], target_port=target['port'], server_name=target['domain'])
                            stored = self.registry._configure_node(db, row['node_id'], config)
                            revision = db.execute('SELECT revision FROM server_configs WHERE server_id=?', (server_id,)).fetchone()[0]
                            state.update(phase='pending', message='switch_waiting', current_domain_id=target['id'],
                                         previous_domain_id=domain['id'], previous_payload=row['payload'],
                                         expected_payload=json.dumps(stored, sort_keys=True),
                                         pending_revision=revision, pending_since=now)
            self.write(db, row['node_id'], state)
