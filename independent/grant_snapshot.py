"""Deterministic public runtime configuration shared by master and Agent.

REALITY private ports and ingress secrets are deliberately not part of it.
"""
import copy
from node_config import XRAY_SCHEMA, digest


def snapshot_for(content):
    reality='reality' in content['node_config']
    client=copy.deepcopy(content['client'])
    client.update(cycle_id=content['cycle_id'],valid_until=content['valid_until'])
    snapshot=dict(schema=XRAY_SCHEMA,server_id=content['server_id'],
        server_name='grant-reality-runtime' if reality else 'grant-runtime',
        revision=content['revision'],valid_until=content['valid_until'],
        nodes=[dict(id=content['node_id'],name='grant-reality' if reality else 'grant',
                    config=copy.deepcopy(content['node_config']),clients=[client])])
    if 'server_bandwidth' in content:
        snapshot['bandwidth']=copy.deepcopy(content['server_bandwidth'])
    snapshot['digest']=digest(snapshot)
    return snapshot
