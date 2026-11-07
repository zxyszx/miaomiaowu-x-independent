"""Versioned independent TCP-forward configuration, not an Xray configuration."""
import hashlib
import base64
import ipaddress
import json
import math
import re
import uuid

SCHEMA = 'independent-tcp-forward-v1'
XRAY_SCHEMA = 'independent-xray-v1'

def automatic_bandwidth(value):
    if value == {'enabled':False} and type(value.get('enabled')) is bool:
        return {'enabled':False}
    keys={'enabled','threshold_bps','sustain_seconds','download_bps','duration_seconds'}
    if not isinstance(value,dict) or set(value)!=keys or value['enabled'] is not True:
        raise ValueError('自动限速需要启用状态、下载阈值、持续秒数、下载上限和恢复秒数')
    for key,maximum in (('threshold_bps',1073741824),('download_bps',1073741824),
                        ('sustain_seconds',3600),('duration_seconds',86400)):
        if type(value[key]) is not int or not 1<=value[key]<=maximum:
            raise ValueError('自动限速参数应为范围内的正整数，速度单位为字节/秒')
    if value['download_bps']>=value['threshold_bps']:
        raise ValueError('触发后的下载上限必须低于触发阈值')
    return dict(value)

def connections(value):
    if not isinstance(value,dict) or set(value) not in ({'max_connections'},{'max_connections','max_ips'}):
        raise ValueError('连接限制需要 max_connections，可选 max_ips')
    for name in ('max_connections','max_ips'):
        limit=value.get(name)
        if limit is not None and (type(limit) is not int or not 1<=limit<=100000):
            raise ValueError('连接数和在线 IP 上限应为 1–100000 的整数，null 表示不限')
    result=dict(value)
    if result.get('max_ips') is None: result.pop('max_ips',None)
    return result

def bandwidth(value):
    if not isinstance(value,dict) or set(value)!={'upload_bps','download_bps'}:
        raise ValueError('限速需要 upload_bps 和 download_bps，单位为字节/秒')
    for limit in value.values():
        if limit is not None and (type(limit) is not int or not 1 <= limit <= 1073741824):
            raise ValueError('限速应为 1–1073741824 字节/秒的整数，null 表示不限速')
    return dict(value)

def profile(value):
    if isinstance(value,dict) and value.get('protocol')=='vless':
        base={'protocol','listen_host','listen_port','enabled'}
        if set(value) not in (base,base|{'reality'}):
            raise ValueError('VLESS 配置字段无效')
        if type(value['listen_port']) is not int or not 1 <= value['listen_port'] <= 65535 or type(value['enabled']) is not bool:
            raise ValueError('VLESS 端口或启用状态无效')
        if not isinstance(value['listen_host'],str): raise ValueError('监听地址无效')
        try: address=ipaddress.ip_address(value['listen_host'])
        except ValueError: raise ValueError('监听地址无效') from None
        if 'reality' in value:
            reality=value['reality']
            if not isinstance(reality,dict) or set(reality)!={'target_host','target_port','server_name','private_key','short_id'}:
                raise ValueError('REALITY 配置字段无效')
            host=reality['target_host']
            name=reality['server_name']
            hostname=r'(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?'
            if not isinstance(host,str) or not re.fullmatch(hostname,host) or name!=host:
                raise ValueError('REALITY 目标与 SNI 应为相同的规范域名')
            if type(reality['target_port']) is not int or not 1<=reality['target_port']<=65535:
                raise ValueError('REALITY 目标端口无效')
            key=reality['private_key']
            try:
                raw=base64.b64decode(key+'=',altchars=b'-_',validate=True)
                if len(raw)!=32 or base64.urlsafe_b64encode(raw).decode().rstrip('=')!=key or not any(raw): raise ValueError()
            except (TypeError,ValueError): raise ValueError('REALITY 私钥必须为 32 字节的规范 Base64URL 值') from None
            if not isinstance(reality['short_id'],str) or not re.fullmatch('[0-9a-f]{16}',reality['short_id']):
                raise ValueError('REALITY short_id 应为 16 位小写十六进制')
        elif not address.is_loopback:
            raise ValueError('明文 VLESS 仅允许回环监听')
        return {**value,'listen_host':str(address)}
    keys = {'listen_host','listen_port','target_host','target_port','enabled'}
    if not isinstance(value,dict) or set(value) not in (keys,keys|{'bandwidth'}):
        raise ValueError('节点配置字段不完整或包含不支持的字段')
    if 'bandwidth' in value:
        bandwidth(value['bandwidth'])
    if type(value['enabled']) is not bool:
        raise ValueError('启用状态必须为布尔值')
    for key in ('listen_port','target_port'):
        if type(value[key]) is not int or not 1 <= value[key] <= 65535:
            raise ValueError('端口应为 1–65535 的整数')
    if not isinstance(value['listen_host'],str): raise ValueError('监听地址应为 IP 地址')
    try: listen=str(ipaddress.ip_address(value['listen_host']))
    except (ValueError,TypeError): raise ValueError('监听地址应为 IP 地址') from None
    target=value['target_host']
    if not isinstance(target,str) or not 1 <= len(target) <= 253:
        raise ValueError('目标地址无效')
    try: target=str(ipaddress.ip_address(target))
    except ValueError:
        if not re.fullmatch(r'(?=.{1,253}$)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)*[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?',target):
            raise ValueError('目标地址应为 IP 或主机名') from None
        target=target.lower()
    return {**value,'listen_host':listen,'target_host':target}

def digest(snapshot):
    raw=json.dumps(snapshot,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
    return hashlib.sha256(raw).hexdigest()

def validate_snapshot(value):
    keys={'server_id','revision','schema','server_name','nodes','digest'}
    if not isinstance(value,dict) or not keys<=set(value)<=keys|{'bandwidth','valid_until'}:
        raise ValueError('下发配置结构无效')
    if 'valid_until' in value:
        deadline=value['valid_until']
        if value['schema']!=XRAY_SCHEMA or type(deadline) not in (int,float) or not math.isfinite(deadline) or deadline<=0:
            raise ValueError('临时配置有效期无效')
    if 'bandwidth' in value: bandwidth(value['bandwidth'])
    content={key:item for key,item in value.items() if key!='digest'}
    if value['schema'] not in (SCHEMA,XRAY_SCHEMA) or not isinstance(value['digest'],str) or digest(content)!=value['digest']:
        raise ValueError('下发配置校验失败')
    for key in ('server_id','revision'):
        if type(value[key]) is not int or value[key]<1: raise ValueError('配置版本或服务器编号无效')
    if not isinstance(value['server_name'],str) or not 1 <= len(value['server_name']) <=120:
        raise ValueError('服务器名称无效')
    if not isinstance(value['nodes'],list): raise ValueError('节点列表无效')
    ids, ports=set(),set()
    for node in value['nodes']:
        if not isinstance(node,dict) or 'config' not in node:
            raise ValueError('下发节点结构无效')
        is_xray=isinstance(node['config'],dict) and node['config'].get('protocol')=='vless'
        if set(node)!=({'id','name','config','clients'} if is_xray else {'id','name','config'}): raise ValueError('下发节点字段无效')
        if is_xray:
            if value['schema']!=XRAY_SCHEMA or not isinstance(node['clients'],list): raise ValueError('VLESS 用户列表无效')
            users, credentials=set(),set()
            for client in node['clients']:
                if not isinstance(client,dict) or not {'user_id','uuid'}<=set(client)<={'user_id','uuid','bandwidth','valid_until','connections','automatic_bandwidth','node_bandwidth','node_connections','cycle_id','cycle_budget','cycle_usage'} or type(client['user_id']) is not int or client['user_id']<1:
                    raise ValueError('VLESS 用户无效')
                if 'cycle_id' in client and (not isinstance(client['cycle_id'],str) or not client['cycle_id'].strip()):
                    raise ValueError('用户周期编号无效')
                for field in ('cycle_budget','cycle_usage'):
                    if field in client:
                        cycle_value=client[field]
                        if not isinstance(cycle_value,dict) or set(cycle_value)!={'upload_bytes','download_bytes'} or any(type(x) is not int or x<0 for x in cycle_value.values()):
                            raise ValueError('用户周期流量额度无效')
                if 'automatic_bandwidth' in client: automatic_bandwidth(client['automatic_bandwidth'])
                if 'connections' in client and connections(client['connections'])!=client['connections']:
                    raise ValueError('连接限制未规范化')
                if 'bandwidth' in client: bandwidth(client['bandwidth'])
                if 'node_bandwidth' in client: bandwidth(client['node_bandwidth'])
                if 'node_connections' in client:
                    # This field replaces TCP slots only, never the global online-IP policy.
                    value_connections=client['node_connections']
                    if not isinstance(value_connections,dict) or set(value_connections)!={'max_connections'} or connections(value_connections)!=value_connections:
                        raise ValueError('节点连接限制未规范化')
                if 'valid_until' in client:
                    deadline=client['valid_until']
                    if type(deadline) not in (int,float) or not math.isfinite(deadline) or deadline<=0:
                        raise ValueError('用户临时配置有效期无效')
                try: credential=str(uuid.UUID(client['uuid']))
                except (ValueError,TypeError,AttributeError): raise ValueError('VLESS UUID 无效') from None
                if credential!=client['uuid'] or credential in credentials or client['user_id'] in users:
                    raise ValueError('VLESS 用户或凭据重复或未规范化')
                users.add(client['user_id']); credentials.add(credential)
        if type(node['id']) is not int or node['id']<1 or node['id'] in ids:
            raise ValueError('下发节点编号无效或重复')
        if not isinstance(node['name'],str) or not 1 <= len(node['name']) <=120:
            raise ValueError('下发节点名称无效')
        config=profile(node['config'])
        if config!=node['config']: raise ValueError('下发配置未规范化')
        if config['enabled'] and config['listen_port'] in ports: raise ValueError('监听端口冲突')
        ids.add(node['id'])
        if config['enabled']: ports.add(config['listen_port'])
    deadlines={}
    connection_policies={}
    auto_policies={}
    cycle_policies={}
    for node in value['nodes']:
        if not node['config']['enabled']: continue
        for client in node.get('clients',[]):
            ident=client['user_id']; deadline=client.get('valid_until')
            auto=client.get('automatic_bandwidth')
            if ident in auto_policies and auto_policies[ident]!=auto:
                raise ValueError('同一用户的自动限速规则不一致')
            auto_policies[ident]=auto
            policy=client.get('connections',{'max_connections':None})
            if ident in connection_policies and connection_policies[ident]!=policy:
                raise ValueError('同一用户的连接限制不一致')
            connection_policies[ident]=policy
            cycle=(client.get('cycle_id'),client.get('cycle_budget'),client.get('cycle_usage'))
            if ident in cycle_policies and cycle_policies[ident]!=cycle:
                raise ValueError('同一用户的流量周期额度不一致')
            cycle_policies[ident]=cycle
            if ident in deadlines and deadlines[ident]!=deadline:
                raise ValueError('同一用户的临时配置有效期不一致')
            deadlines[ident]=deadline
    active=[deadline for deadline in deadlines.values() if deadline is not None]
    if active and value.get('valid_until')!=min(active):
        raise ValueError('临时配置最早有效期不一致')
    return value


def user_deadlines(snapshot):
    """Validated per-user deadlines; an empty mapping preserves legacy expiry."""
    return {client['user_id']:client['valid_until']
            for node in snapshot['nodes'] if node['config']['enabled']
            for client in node.get('clients',[]) if 'valid_until' in client}
