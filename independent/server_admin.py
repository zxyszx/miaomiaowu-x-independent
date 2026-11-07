"""Server base-name content version. Not a revision or Agent capability."""
import hashlib
import json
import re
from core import Error


def validate(value):
    if value is not None and (not isinstance(value,str) or not re.fullmatch(r'"sv1-[0-9a-f]{64}"',value)):
        raise Error('服务器记录版本无效')


def response(row):
    content=dict(row)
    encoded=json.dumps(content,sort_keys=True,separators=(',',':')).encode()
    return {**content,'version':'"sv1-'+hashlib.sha256(encoded).hexdigest()+'"'}
