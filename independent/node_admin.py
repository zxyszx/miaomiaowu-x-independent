"""Base-node content CAS, including opaque configuration/share context."""
import hashlib
import json
import re
from core import Error


def validate(value):
    if value is not None and (not isinstance(value,str) or not re.fullmatch(r'"nd1-[0-9a-f]{64}"',value)):
        raise Error('节点配置版本无效')


def response(db,row):
    node=dict(row)
    config=db.execute('SELECT payload FROM node_configs WHERE node_id=?',(node['id'],)).fetchone()
    share=db.execute('SELECT share_id FROM shared_nodes WHERE node_id=?',(node['id'],)).fetchone()
    content=[node,json.loads(config[0]) if config else None,share[0] if share else None]
    encoded=json.dumps(content,sort_keys=True,separators=(',',':')).encode()
    return {**node,'version':'"nd1-'+hashlib.sha256(encoded).hexdigest()+'"'}
