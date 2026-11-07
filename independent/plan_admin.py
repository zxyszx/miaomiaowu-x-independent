"""Content CAS for the base plan representation; not an authorization token."""
import hashlib
import json
import re
from core import Error


def validate(value):
    if value is not None and (not isinstance(value,str) or not re.fullmatch(r'"pl1-[0-9a-f]{64}"',value)):
        raise Error('套餐配置版本无效')


def version(result):
    content={key:result[key] for key in ('id','name','bandwidth','connections') if key in result}
    return '"pl1-'+hashlib.sha256(json.dumps(content,sort_keys=True,separators=(',',':')).encode()).hexdigest()+'"'


def response(result):
    return {**result,'version':version(result)}
