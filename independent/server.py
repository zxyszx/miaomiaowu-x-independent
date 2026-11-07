#!/usr/bin/env python3
"""Local independent API foundation; no official binary, database or license dependency."""
import argparse
import hmac
import json
import math
import os
from pathlib import Path
import secrets
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs,urlsplit
from core import Registry, Error
from domain_pool import DomainPool

class PanelHTTPServer(ThreadingHTTPServer):
    """Panel shutdown joins every accepted request before clearing its guard."""
    daemon_threads=False
    block_on_close=True

    def __init__(self,*args,**kwargs):
        import threading
        self.worker_failed=threading.Event()
        super().__init__(*args,**kwargs)

    def handle_error(self,request,client_address):
        # Unhandled worker failures are not a clean drain. Do not log secrets.
        self.worker_failed.set()

    def process_request_thread(self,request,client_address):
        try: super().process_request_thread(request,client_address)
        except BaseException:
            self.worker_failed.set()
            raise

    def server_close(self):
        if self.daemon_threads or not self.block_on_close:
            raise Error('面板请求线程排空配置无效',503)
        super().server_close()  # closes listener, then joins non-daemon workers
        if self.worker_failed.is_set(): raise Error('面板请求异常结束，保留运行标记',503)

class Handler(BaseHTTPRequestHandler):
    def parse_request(self):
        if not super().parse_request(): return False
        self.panel_response_headers=[]
        try:
            from panel_authorization import classify
            self.panel_cookies=classify(self)
            return True
        except Error as error:
            self.close_connection=True
            self.reply(error.status,{'error':str(error)})
            return False

    def log_message(self, *args):
        pass  # Do not log authorization headers or resource content.

    def reply(self, status, value):
        body = json.dumps(value, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Content-Length', str(len(body)))
        for key,value in getattr(self,'panel_response_headers',[]): self.send_header(key,value)
        self.end_headers()
        self.wfile.write(body)

    def body(self, limit=16384):
        size = int(self.headers.get('Content-Length','0'))
        if not 0 < size <= limit:
            raise Error('请求体为空或过大',413)
        self.connection.settimeout(10)
        return json.loads(self.rfile.read(size))

    def handle_api(self):
        path = urlsplit(self.path).path
        if path.startswith('/panel/'):
            try:
                from panel_handlers import serve
                return serve(self)
            except Error as error: return self.reply(error.status,{'error':str(error)})
            except (ValueError,UnicodeError,TypeError): return self.reply(400,{'error':'身份请求参数无效'})
            except TimeoutError: return self.reply(408,{'error':'请求体超时'})
        config=getattr(self.server,'panel_config',None)
        if path.startswith('/api/') and config and config.session in self.panel_cookies:
            try:
                from panel_admin import serve
                return serve(self)
            except Error as error: return self.reply(error.status,{'error':str(error)})
            except (ValueError,UnicodeError,TypeError): return self.reply(400,{'error':'管理请求参数无效'})
            except TimeoutError: return self.reply(408,{'error':'请求体超时'})
            except Exception: return self.reply(500,{'error':'管理请求未完成'})
        if self.command == 'GET' and path.startswith('/manage/'):
            from management_web import serve
            return serve(self, path)
        if self.command == 'GET' and path == '/health':
            return self.reply(200, {'service':'independent-x-core', 'stage':'temporary-bandwidth', 'ready':True})
        if path in ('/agent/multi/lifecycle','/agent/multi/installed','/agent/multi/execution'):
            try:
                from multi_agent_lifecycle import gated,MultiAgentLifecycle
                gated(self.server.registry)
                if self.command!='POST': raise Error('接口不存在',404)
                token=self.headers.get('Authorization','').removeprefix('Bearer ')
                data=self.body()
                if path.endswith('/lifecycle'):
                    result=MultiAgentLifecycle(self.server.registry).advance(token,data)
                elif path.endswith('/installed'):
                    from multi_agent_installation import MultiAgentInstallation
                    result=MultiAgentInstallation(self.server.registry).accept(token,data)
                else:
                    from multi_agent_execution import MultiAgentExecution
                    result=MultiAgentExecution(self.server.registry).record(token,data)
                return self.reply(200,result)
            except Error as error: return self.reply(error.status,{'error':str(error)})
            except (ValueError,UnicodeError,TypeError): return self.reply(400,{'error':'请求参数无效'})
            except Exception: return self.reply(500,{'error':'Agent 请求未完成'})
        if path in ('/agent/grants/usage','/agent/grants/installed','/agent/grants/configuration','/agent/grants/resolve','/agent/grants/capabilities','/agent/grants/lifecycle','/agent/grants/execution','/agent/session','/agent/heartbeat','/agent/usage','/agent/usage/settle','/agent/usage/allocation-evidence','/agent/usage/closed','/agent/usage/closed/fragment','/agent/usage/drains','/agent/config','/agent/config/ack','/agent/config/applied','/agent/probe-job') or path.startswith('/agent/probe-job/'):
            try:
                token=self.headers.get('Authorization','').removeprefix('Bearer ')
                if self.command=='POST' and path=='/agent/usage/allocation-evidence':
                    from allocation_evidence import AllocationEvidence
                    # Evidence only: retain the ordinary 16 KiB bound. No
                    # execution grant, usage confirmation or hold release.
                    return self.reply(200,AllocationEvidence(self.server.registry).receive(token,self.body()))
                if self.command=='POST' and path=='/agent/usage/drains':
                    from usage_migration import UsageMigration
                    return self.reply(200,UsageMigration(self.server.registry).discover(token,self.body()))
                if self.command=='POST' and path=='/agent/usage/closed/fragment':
                    from closed_receiver import ClosedReceiver
                    # A bounded manifest plus one <=64-identity fragment. Other
                    # Agent routes retain their original 16 KiB request limit.
                    data=self.body(limit=1024*1024)
                    if not isinstance(data,dict) or set(data)!={'cycle_id','envelope','fragment'}:
                        raise Error('最终分片请求字段无效')
                    return self.reply(200,ClosedReceiver(self.server.registry).accept(
                        data['cycle_id'],token,data['envelope'],data['fragment']))
                if self.command=='POST' and path=='/agent/usage/closed':
                    from usage_migration import UsageMigration
                    data=self.body()
                    if not isinstance(data,dict) or set(data)!={'cycle_id','checkpoint','digest'}:
                        raise Error('最终检查点请求字段无效')
                    return self.reply(200,UsageMigration(self.server.registry).accept_closed(
                        data['cycle_id'],token,dict(checkpoint=data['checkpoint'],digest=data['digest'])))
                if self.command=='POST' and path=='/agent/usage/settle':
                    from usage_ledger import UsageLedger
                    return self.reply(200,UsageLedger(self.server.registry).settle(token,self.body()))
                if self.command=='POST' and path=='/agent/usage':
                    from usage_ledger import UsageLedger
                    return self.reply(200,UsageLedger(self.server.registry).report(token,self.body()))
                if self.command=='POST' and path=='/agent/grants/usage':
                    from grant_meter import GrantMeter
                    return self.reply(200,GrantMeter(self.server.registry).report(token,self.body()))
                if self.command=='POST' and path=='/agent/grants/installed':
                    from grant_ack import GrantAcknowledgement
                    return self.reply(200,GrantAcknowledgement(self.server.registry).accept(token,self.body()))
                if self.command=='POST' and path=='/agent/grants/configuration':
                    from grant_transport import pull
                    return self.reply(200,pull(self.server.registry,token,self.body()))
                if self.command=='POST' and path=='/agent/grants/resolve':
                    from grant_transport import resolve
                    return self.reply(200,resolve(self.server.registry,token,self.body()))
                if self.command=='POST' and path=='/agent/grants/capabilities':
                    from grant_capabilities import GrantCapabilities
                    return self.reply(200,GrantCapabilities(self.server.registry).negotiate(token,self.body()))
                if self.command=='POST' and path=='/agent/grants/lifecycle':
                    from grant_lifecycle import GrantLifecycle
                    data=self.body()
                    if (not isinstance(data,dict) or
                        set(data) != {'action','grant_id','session_id'} or
                        data['action'] not in ('begin','installed','close','status')):
                        raise Error('授予生命周期请求字段无效')
                    lifecycle=GrantLifecycle(self.server.registry)
                    action=data['action']
                    if action == 'begin':
                        value=lifecycle.begin(token,data['grant_id'],data['session_id'])
                    elif action == 'installed':
                        value=lifecycle.mark_installed(token,data['grant_id'],data['session_id'])
                    elif action == 'close':
                        value=lifecycle.close(token,data['grant_id'],data['session_id'])
                    else:
                        value=lifecycle.status(token,data['grant_id'],data['session_id'])
                    return self.reply(200,value)
                if self.command=='POST' and path=='/agent/grants/execution':
                    from grant_execution import GrantExecution
                    data=self.body()
                    if not isinstance(data,dict) or set(data)!={'fact'}:
                        raise Error('执行回执请求字段无效')
                    return self.reply(200,GrantExecution(self.server.registry).record(token,data['fact']))
                if self.command=='POST' and path=='/agent/session':
                    return self.reply(200,self.server.registry.begin_session(token,self.body()))
                if self.command=='POST' and path=='/agent/heartbeat':
                    return self.reply(200,self.server.registry.heartbeat(token,self.body()))
                if self.command=='GET' and path=='/agent/config':
                    return self.reply(200,self.server.registry.configuration(token))
                if self.command=='POST' and path=='/agent/config/ack':
                    return self.reply(200,self.server.registry.acknowledge_configuration(token,self.body()))
                if self.command=='POST' and path=='/agent/config/applied':
                    return self.reply(200,self.server.registry.acknowledge_runtime(token,self.body()))
                if self.command=='GET' and path=='/agent/probe-job':
                    return self.reply(200,self.server.registry.claim_reality_probe(token))
                if self.command=='POST' and path.startswith('/agent/probe-job/'):
                    body=self.body()
                    if not isinstance(body,dict) or set(body) != {'lease_token','result'}:
                        raise Error('探测回执字段无效')
                    return self.reply(200,self.server.registry.complete_reality_probe(token,path.rsplit('/',1)[1],body['lease_token'],body['result']))
                raise Error('接口不存在',404)
            except Error as error: return self.reply(error.status,{'error':str(error)})
            except (ValueError,UnicodeError): return self.reply(400,{'error':'请求参数无效'})
            except Exception: return self.reply(500,{'error':'Agent 请求未完成'})
        if path.startswith('/share/'):
            from sharing import Shares
            shares=Shares(self.server.registry)
            try:
                token=self.headers.get('Authorization','').removeprefix('Bearer ')
                parts=path.strip('/').split('/')
                if path=='/share/claim' and self.command=='POST':
                    if self.body()!={}: raise Error('接入请求不需要额外字段')
                    return self.reply(200,shares.claim(token))
                if path=='/share/status' and self.command=='GET': return self.reply(200,shares.guest(token,'status'))
                if path=='/share/usage/periods' and self.command=='GET': return self.reply(200,shares.guest(token,'periods'))
                if path=='/share/usage' and self.command=='GET':
                    query=parse_qs(urlsplit(self.path).query,keep_blank_values=True)
                    def point(key):
                        value=query.get(key,[None])[-1]
                        if value is None: return None
                        try: value=float(value)
                        except ValueError: raise Error('流量周期参数无效')
                        if not math.isfinite(value) or value<0: raise Error('流量周期参数无效')
                        return value
                    start,end=point('start'),point('end')
                    if start is not None and end is not None and start>=end: raise Error('流量周期参数无效')
                    return self.reply(200,shares.guest(token,'usage',data=(start,end)))
                if path=='/share/nodes':
                    if self.command=='GET': return self.reply(200,shares.guest(token,'list'))
                    if self.command=='POST': return self.reply(201,shares.guest(token,'create',data=self.body()))
                if len(parts)==3 and parts[:2]==['share','nodes'] and parts[2].isdigit():
                    if self.command=='PUT': return self.reply(200,shares.guest(token,'update',int(parts[2]),self.body()))
                    if self.command=='DELETE': return self.reply(200,shares.guest(token,'delete',int(parts[2])))
                if len(parts)==5 and parts[:2]==['share','nodes'] and parts[2].isdigit() and parts[3]=='users':
                    if self.command=='PUT':
                        if self.body()!={}: raise Error('授权请求不需要额外字段')
                        return self.reply(200,shares.guest(token,'authorize',int(parts[2]),reference=parts[4]))
                    if self.command=='DELETE': return self.reply(200,shares.guest(token,'revoke',int(parts[2]),reference=parts[4]))
                raise Error('分享接口不存在',404)
            except Error as error: return self.reply(error.status,{'error':str(error)})
            except (ValueError,UnicodeError): return self.reply(400,{'error':'请求参数无效'})
            except Exception: return self.reply(500,{'error':'分享请求未完成'})
        if not hmac.compare_digest(self.headers.get('Authorization','').encode(), ('Bearer '+self.server.token).encode()):
            return self.reply(401, {'error':'需要管理员 API 凭据'})
        if (self.command=='DELETE' and getattr(self.server,'panel_runtime',None)
            and len(path.strip('/').split('/'))==3 and path.strip('/').split('/')[:2]==['api','users']
            and path.strip('/').split('/')[2].isdigit() and int(path.strip('/').split('/')[2])>0):
            try:
                from panel_admin import serve
                return serve(self,legacy_delete=True)
            except Error as error: return self.reply(error.status,{'error':str(error)})
            except Exception: return self.reply(500,{'error':'资源删除未完成'})
        try:
            parts = path.strip('/').split('/')
            if len(parts)==4 and parts[:2]==['api','users'] and parts[2].isdigit() and parts[3]=='usage' and self.command=='GET':
                from usage_ledger import UsageLedger
                return self.reply(200,UsageLedger(self.server.registry).user(int(parts[2])))
            if len(parts)==4 and parts[:2]==['api','servers'] and parts[2].isdigit() and parts[3]=='shares':
                from sharing import Shares
                shares=Shares(self.server.registry)
                if self.command=='GET': return self.reply(200,shares.owner(server_id=int(parts[2])))
                if self.command=='POST': return self.reply(201,shares.create(int(parts[2]),self.body()))
                raise Error('分享接口不存在',404)
            if parts[:2]==['api','shares']:
                from sharing import Shares
                shares=Shares(self.server.registry)
                if len(parts)==3:
                    if self.command=='GET': return self.reply(200,shares.owner(ident=parts[2]))
                    if self.command=='DELETE': return self.reply(200,shares.owner(ident=parts[2],action='revoke'))
                if len(parts)==4 and parts[3]=='rotate' and self.command=='POST':
                    if self.body()!={}: raise Error('轮换请求不需要额外字段')
                    return self.reply(200,shares.owner(ident=parts[2],action='rotate'))
                if len(parts)==4 and parts[3]=='usage' and self.command=='GET':
                    query=parse_qs(urlsplit(self.path).query,keep_blank_values=True)
                    def point(key):
                        value=query.get(key,[None])[-1]
                        if value is None: return None
                        try: value=float(value)
                        except ValueError: raise Error('流量周期参数无效')
                        if not math.isfinite(value) or value<0: raise Error('流量周期参数无效')
                        return value
                    start,end=point('start'),point('end')
                    if start is not None and end is not None and start>=end: raise Error('流量周期参数无效')
                    return self.reply(200,shares.usage_for_owner(parts[2],start,end))
                if len(parts)==5 and parts[3]=='usage' and parts[4]=='periods' and self.command=='GET':
                    return self.reply(200,shares.periods_for_owner(parts[2]))
                if len(parts)==5 and parts[3]=='usage' and parts[4]=='reset' and self.command=='POST':
                    body=self.body()
                    if not isinstance(body,dict) or set(body)-{'reason'}: raise Error('周期重置字段无效')
                    return self.reply(200,shares.reset_usage(parts[2],body.get('reason','manual')))
                raise Error('分享接口不存在',404)
            if len(parts)==4 and parts[:2]==['api','users'] and parts[2].isdigit() and parts[3]=='plan-order':
                from plan_order import PlanOrder
                order=PlanOrder(self.server.registry); uid=int(parts[2])
                if self.command=='GET': return self.reply(200,order.manage(uid))
                if self.command=='PUT':
                    data=self.body()
                    if data is None: raise Error('套餐顺序应为对象')
                    return self.reply(200,order.manage(uid,data))
                raise Error('套餐顺序接口不存在',404)
            if len(parts)==4 and parts[:2]==['api','users'] and parts[2].isdigit() and parts[3]=='plan-renewals':
                from plan_renewals import Renewals
                renewals=Renewals(self.server.registry); uid=int(parts[2])
                if self.command=='GET': return self.reply(200,renewals.list(uid))
                if self.command=='POST': return self.reply(201,renewals.create(uid,self.body()))
                raise Error('续期申请接口不存在',404)
            if len(parts)==4 and parts[:2]==['api','plan-renewals'] and parts[2].isdigit() and parts[3]=='decision':
                from plan_renewals import Renewals
                if self.command=='POST': return self.reply(200,Renewals(self.server.registry).decide(int(parts[2]),self.body()))
                raise Error('续期审核接口不存在',404)
            if len(parts)==4 and parts[:2]==['api','users'] and parts[2].isdigit() and parts[3]=='plan-lifecycle':
                from plan_lifecycle import Lifecycle
                lifecycle=Lifecycle(self.server.registry); uid=int(parts[2])
                if self.command=='GET': return self.reply(200,lifecycle.manage(uid))
                if self.command=='PUT':
                    data=self.body()
                    if data is None: raise Error('套餐有效期应为对象')
                    return self.reply(200,lifecycle.manage(uid,data))
                raise Error('套餐有效期接口不存在',404)
            if len(parts) in (4,5) and parts[:2]==['api','users'] and parts[2].isdigit() and parts[3]=='accounts':
                from accounts import Accounts
                accounts=Accounts(self.server.registry); parent=int(parts[2])
                if len(parts)==4:
                    if self.command=='GET': return self.reply(200,accounts.list(parent))
                    if self.command=='POST': return self.reply(201,accounts.create(parent,self.body()))
                if len(parts)==5 and parts[4].isdigit():
                    if self.command=='DELETE': return self.reply(200,accounts.delete(parent,int(parts[4])))
                    if self.command=='PUT': return self.reply(200,accounts.update(parent,int(parts[4]),self.body()))
                raise Error('套餐子账号接口不存在',404)
            if parts[:2]==['api','speed-tests']:
                speed=getattr(self.server,'speed_tests',None)
                if speed is None: raise Error('此服务未启用独立测速端',503)
                if len(parts)==2:
                    if self.command=='GET': return self.reply(200,speed.get())
                    if self.command=='POST': return self.reply(202,speed.start(self.body()))
                if parts==['api','speed-tests','status'] and self.command=='GET':
                    return self.reply(200,{'source':'master','local_available':not speed.closed,
                        'local_node_server_id':speed.local_server_id,
                        'node_protocols':['vless-raw-loopback']+(['vless-reality-loopback'] if speed.xray_binary and speed.xray_binary.is_file() else []),
                        'remote_agent_available':False})
                if len(parts)==3 and parts[2]!='status':
                    if self.command=='GET': return self.reply(200,speed.get(parts[2]))
                    if self.command=='DELETE': return self.reply(200,speed.cancel(parts[2]))
                raise Error('接口不存在',404)
            if path=='/api/automatic-bandwidth' or (len(parts)==4 and parts[:2]==['api','users'] and parts[2].isdigit() and parts[3]=='automatic-bandwidth'):
                uid=None if path=='/api/automatic-bandwidth' else int(parts[2])
                if self.command=='GET': return self.reply(200,self.server.registry.automatic_bandwidth(uid))
                if self.command=='DELETE': return self.reply(200,self.server.registry.automatic_bandwidth(uid,inherit=True))
                if self.command=='PUT':
                    data=self.body()
                    if not isinstance(data,dict): raise Error('自动限速应为对象')
                    return self.reply(200,self.server.registry.automatic_bandwidth(uid,data))
                raise Error('接口不存在',404)
            if parts[:2] == ['api', 'reality-domains']:
                pool = DomainPool(self.server.registry)
                if len(parts) == 2:
                    if self.command == 'GET': return self.reply(200, pool.list())
                    if self.command == 'POST': return self.reply(201, pool.save(self.body()))
                if parts == ['api', 'reality-domains', 'selection'] and self.command == 'GET':
                    return self.reply(200, pool.select())
                if len(parts) in (3, 4) and parts[2].isdigit() and int(parts[2]) > 0:
                    ident = int(parts[2])
                    if len(parts) == 3:
                        if self.command == 'PUT': return self.reply(200, pool.save(self.body(), ident))
                        if self.command == 'DELETE':
                            pool.delete(ident)
                            return self.reply(200, {'deleted': True})
                    if len(parts) == 4 and parts[3] == 'probe' and self.command == 'POST':
                        return self.reply(200, pool.probe(ident))
                    if len(parts) == 4 and parts[3] == 'probe-schedule':
                        if self.command == 'GET':
                            return self.reply(200,self.server.registry.reality_probe_schedule(ident))
                        if self.command == 'PUT':
                            data=self.body()
                            if data is None: raise Error('巡检配置应为对象')
                            return self.reply(200,self.server.registry.reality_probe_schedule(ident,data))
                        if self.command == 'DELETE':
                            return self.reply(200,self.server.registry.reality_probe_schedule(ident,delete=True))
                raise Error('接口不存在', 404)
            if (len(parts) in (4,5) and parts[:2]==['api','servers'] and parts[2].isdigit()
                    and parts[3]=='reality-domains' and self.command=='GET'):
                pool=DomainPool(self.server.registry)
                if len(parts)==4: return self.reply(200,pool.list(int(parts[2])))
                if parts[4]=='selection': return self.reply(200,pool.select(int(parts[2])))
                raise Error('接口不存在',404)
            if len(parts)==6 and parts[:2]==['api','servers'] and parts[2].isdigit() and parts[3]=='reality-domains' and parts[4].isdigit() and parts[5]=='probe-agent' and self.command=='POST':
                return self.reply(202,self.server.registry.create_reality_probe_job(int(parts[2]),int(parts[4])))
            if parts[:2]==['api','node-groups']:
                from node_groups import NodeGroups
                groups=NodeGroups(self.server.registry)
                if len(parts)==2:
                    if self.command=='GET': return self.reply(200,groups.get())
                    if self.command=='POST': return self.reply(201,groups.save(self.body()))
                if len(parts)==3 and parts[2].isdigit() and int(parts[2])>0:
                    ident=int(parts[2])
                    if self.command=='GET': return self.reply(200,groups.get(ident))
                    if self.command=='PUT': return self.reply(200,groups.save(self.body(),ident))
                    if self.command=='DELETE': return self.reply(200,groups.delete(ident))
                raise Error('节点组接口不存在',404)
            if parts[:2]==['api','plans']:
                if len(parts)==6 and parts[2].isdigit() and parts[3]=='nodes' and parts[4].isdigit() and parts[5]=='connections':
                    from plan_node_connections import PlanNodeConnections
                    policy=PlanNodeConnections(self.server.registry); pid,nid=int(parts[2]),int(parts[4])
                    if self.command=='GET': return self.reply(200,policy.manage(pid,nid))
                    if self.command=='PUT':
                        data=self.body()
                        if data is None: raise Error('节点连接限制应为对象')
                        return self.reply(200,policy.manage(pid,nid,data))
                    if self.command=='DELETE': return self.reply(200,policy.manage(pid,nid,inherit=True))
                    raise Error('节点连接限制接口不存在',404)
                if len(parts)==6 and parts[2].isdigit() and parts[3]=='nodes' and parts[4].isdigit() and parts[5]=='bandwidth':
                    from plan_node_bandwidth import PlanNodeBandwidth
                    policy=PlanNodeBandwidth(self.server.registry); pid,nid=int(parts[2]),int(parts[4])
                    if self.command=='GET': return self.reply(200,policy.manage(pid,nid,versioned=True))
                    versions=self.headers.get_all('If-Match',[])
                    if len(versions)>1: raise Error('限速配置版本重复')
                    condition={'if_match':versions[0] if versions else None,'versioned':True}
                    if self.command=='PUT':
                        data=self.body()
                        if data is None: raise Error('节点限速应为对象')
                        return self.reply(200,policy.manage(pid,nid,data,**condition))
                    if self.command=='DELETE': return self.reply(200,policy.manage(pid,nid,inherit=True,**condition))
                    raise Error('节点限速接口不存在',404)
                if len(parts)==4 and parts[2].isdigit() and parts[3]=='nodes':
                    from plan_nodes import PlanNodes
                    scope=PlanNodes(self.server.registry); ident=int(parts[2])
                    if self.command=='GET': return self.reply(200,scope.get(ident))
                    if self.command=='PUT': return self.reply(200,scope.save(ident,self.body()))
                    if self.command=='DELETE': return self.reply(200,scope.clear(ident))
                    raise Error('套餐节点范围接口不存在',404)
                if len(parts)==2:
                    if self.command=='GET': return self.reply(200,self.server.registry.plans(versioned=True))
                    if self.command=='POST':
                        if self.headers.get_all('If-Match',[]): raise Error('创建套餐不接受编辑版本')
                        return self.reply(201,self.server.registry.save_plan(self.body(),versioned=True))
                if len(parts)==3 and parts[2].isdigit() and int(parts[2])>0:
                    if self.command=='PUT':
                        versions=self.headers.get_all('If-Match',[])
                        if len(versions)>1: raise Error('套餐配置版本重复')
                        return self.reply(200,self.server.registry.save_plan(self.body(),int(parts[2]),
                            if_match=versions[0] if versions else None,versioned=True))
                    if self.command=='DELETE':
                        versions=self.headers.get_all('If-Match',[])
                        if len(versions)>1: raise Error('套餐配置版本重复')
                        self.server.registry.delete_plan(int(parts[2]),if_match=versions[0] if versions else None)
                        return self.reply(200,{'deleted':True})
                raise Error('接口不存在',404)
            if len(parts)==4 and parts[:2]==['api','users'] and parts[2].isdigit():
                if parts[3]=='connections-policy' and self.command=='GET':
                    return self.reply(200,self.server.registry.connections_policy(int(parts[2])))
                if parts[3]=='connections':
                    if self.command=='GET': return self.reply(200,self.server.registry.user_connections(int(parts[2])))
                    if self.command=='DELETE': return self.reply(200,self.server.registry.user_connections(int(parts[2]),inherit=True))
                    if self.command=='PUT':
                        data=self.body()
                        if not isinstance(data,dict): raise Error('连接限制应为对象')
                        return self.reply(200,self.server.registry.user_connections(int(parts[2]),data))
                    raise Error('接口不存在',404)
                if parts[3]=='temporary-bandwidth':
                    if self.command=='GET': return self.reply(200,self.server.registry.temporary_bandwidth(int(parts[2])))
                    if self.command=='PUT':
                        data=self.body()
                        if not isinstance(data,dict): raise Error('临时规则应为对象')
                        return self.reply(200,self.server.registry.temporary_bandwidth(int(parts[2]),data))
                    if self.command=='DELETE': return self.reply(200,self.server.registry.temporary_bandwidth(int(parts[2]),remove=True))
                    raise Error('接口不存在',404)
                if parts[3]=='plan' and self.command=='PUT':
                    return self.reply(200,self.server.registry.assign_plan(int(parts[2]),self.body()))
                if parts[3]=='bandwidth-policy' and self.command=='GET':
                    return self.reply(200,self.server.registry.bandwidth_policy(int(parts[2])))
            if len(parts)==4 and parts[:2]==['api','servers'] and parts[2].isdigit() and parts[3]=='bandwidth':
                if self.command=='GET': return self.reply(200,self.server.registry.server_bandwidth(int(parts[2])))
                if self.command=='PUT':
                    data=self.body()
                    if not isinstance(data,dict): raise Error('限速配置应为对象')
                    return self.reply(200,self.server.registry.server_bandwidth(int(parts[2]),data))
                raise Error('接口不存在',404)
            if len(parts)==4 and parts[:2]==['api','users'] and parts[2].isdigit() and parts[3]=='bandwidth':
                if self.command=='DELETE': return self.reply(200,self.server.registry.user_bandwidth(int(parts[2]),inherit=True))
                if self.command=='GET': return self.reply(200,self.server.registry.user_bandwidth(int(parts[2])))
                if self.command=='PUT':
                    data=self.body()
                    if not isinstance(data,dict): raise Error('限速配置应为对象')
                    return self.reply(200,self.server.registry.user_bandwidth(int(parts[2]),data))
                raise Error('接口不存在',404)
            if path == '/api/agent-status' and self.command == 'GET':
                return self.reply(200,self.server.registry.agent_status())
            if len(parts)==5 and parts[:2]==['api','nodes'] and parts[2].isdigit() and parts[3]=='users' and parts[4].isdigit() and self.command in ('POST','DELETE'):
                return self.reply(200,self.server.registry.authorize_node_user(int(parts[2]),int(parts[4]),revoke=self.command=='DELETE'))
            if len(parts)==4 and parts[:2]==['api','nodes'] and parts[2].isdigit() and parts[3]=='config' and self.command=='PUT':
                return self.reply(200,self.server.registry.configure_node(int(parts[2]),self.body()))
            if len(parts)==4 and parts[:2]==['api','nodes'] and parts[2].isdigit() and parts[3]=='reality-failover':
                from domain_failover import Failover
                failover=Failover(self.server.registry)
                if self.command=='GET': return self.reply(200,failover.policy(int(parts[2])))
                if self.command=='PUT':
                    data=self.body()
                    Failover.validate(data)
                    return self.reply(200,failover.policy(int(parts[2]),data))
                if self.command=='DELETE': return self.reply(200,failover.policy(int(parts[2]),delete=True))
                raise Error('接口不存在',404)
            if len(parts)==4 and parts[:2]==['api','nodes'] and parts[2].isdigit() and parts[3]=='reality-domain' and self.command=='POST':
                return self.reply(200,DomainPool(self.server.registry).bind(int(parts[2]),self.body()))
            if len(parts)==4 and parts[:2]==['api','servers'] and parts[2].isdigit() and parts[3]=='agent':
                if self.command=='POST':
                    return self.reply(201,self.server.registry.provision_agent(int(parts[2])))
                if self.command=='DELETE':
                    self.server.registry.revoke_agent(int(parts[2]))
                    return self.reply(200,{'revoked':True})
                raise Error('接口不存在',404)
            if parts[:3]==['api','manage','servers']:
                registry=self.server.registry
                versions=self.headers.get_all('If-Match',[])
                if len(parts)==3:
                    if self.command=='GET': return self.reply(200,registry.list('servers',versioned=True))
                    if self.command=='POST':
                        if versions: raise Error('创建服务器不接受编辑版本')
                        return self.reply(201,registry.save('servers',self.body(),versioned=True))
                if len(parts)==4 and parts[3].isdigit() and int(parts[3])>0 and self.command=='PUT':
                    if len(versions)!=1: raise Error('必须提供单个服务器记录版本')
                    return self.reply(200,registry.save('servers',self.body(),int(parts[3]),if_match=versions[0],versioned=True))
                raise Error('服务器基础管理接口不存在',404)
            if len(parts) not in (2,3) or parts[0] != 'api':
                raise Error('接口不存在',404)
            kind = parts[1]
            data = None
            if self.command in ('POST','PUT'):
                data = self.body()
            registry = self.server.registry
            if kind == 'quotas':
                if self.command == 'GET' and len(parts)==2:
                    return self.reply(200, registry.quotas())
                if self.command == 'PUT' and len(parts)==3:
                    if not isinstance(data,dict) or set(data)!={'limit'}:
                        raise Error('配额请求字段无效')
                    registry.set_quota(parts[2],data['limit'])
                    return self.reply(200,registry.quotas())
                raise Error('接口不存在',404)
            registry.kind(kind)
            if self.command == 'GET' and len(parts)==2:
                return self.reply(200, registry.list(kind,versioned=kind=='nodes'))
            if self.command == 'POST' and len(parts)==2:
                if kind=='nodes' and self.headers.get_all('If-Match',[]): raise Error('创建节点不接受编辑版本')
                return self.reply(201,registry.save(kind,data,versioned=kind=='nodes'))
            if len(parts)==3 and parts[2].isdigit() and int(parts[2])>0:
                ident=int(parts[2])
                if self.command=='PUT':
                    if kind=='nodes':
                        versions=self.headers.get_all('If-Match',[])
                        if len(versions)>1: raise Error('节点配置版本重复')
                        return self.reply(200,registry.save(kind,data,ident,if_match=versions[0] if versions else None,versioned=True))
                    return self.reply(200,registry.save(kind,data,ident))
                if self.command=='DELETE':
                    registry.delete(kind,ident)
                    return self.reply(200,{'deleted':True})
            raise Error('接口不存在',404)
        except Error as error:
            self.reply(error.status,{'error':str(error)})
        except (ValueError,UnicodeError):
            self.reply(400,{'error':'请求参数无效'})
        except Exception:
            self.reply(500,{'error':'操作未完成'})

    do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = do_HEAD = do_OPTIONS = do_TRACE = do_CONNECT = handle_api

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port',type=int,default=45300)
    parser.add_argument('--data',type=Path,default=Path(__file__).resolve().parent/'data')
    parser.add_argument('--speed-local-server-id',type=int,
                        help='explicit registry server identity hosted on this machine for loopback VLESS measurements')
    parser.add_argument('--speed-xray-binary',type=Path,help='local Xray client binary for REALITY measurement')
    parser.add_argument('--panel-control',type=Path,help='preinitialized control directory outside data backups')
    parser.add_argument('--panel-origin',help='fixed trusted origin; production requires direct TLS')
    parser.add_argument('--panel-loopback-test',action='store_true',help='explicit HTTP loopback test cookies only')
    parser.add_argument('--panel-tls-cert',type=Path)
    parser.add_argument('--panel-tls-key',type=Path)
    args=parser.parse_args()
    runtime=None
    if args.panel_control or args.panel_origin or args.panel_loopback_test or args.panel_tls_cert or args.panel_tls_key:
        from panel_identity import PanelIdentity
        from panel_runtime import PanelRuntime
        from panel_authorization import PanelConfig
        if not args.panel_control or not args.panel_origin: parser.error('panel-control and panel-origin required')
        config=PanelConfig(args.panel_origin,loopback_test=args.panel_loopback_test)
        if config.test and (args.panel_tls_cert or args.panel_tls_key): parser.error('test mode does not accept TLS flags')
        if not config.test and (not args.panel_tls_cert or not args.panel_tls_key): parser.error('production panel requires TLS cert and key')
        if config.test and config.host not in (f'127.0.0.1:{args.port}',f'localhost:{args.port}'):
            parser.error('test origin must match loopback listen port')
        # Guard is durable before creating any listening HTTP socket. No lazy init.
        runtime=PanelRuntime(PanelIdentity(args.data/'registry.db',args.panel_control))
    args.data.mkdir(mode=0o700,parents=True,exist_ok=True)
    tokenfile=args.data/'admin-token'
    if not tokenfile.exists():
        with os.fdopen(os.open(tokenfile,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as file:
            file.write(secrets.token_urlsafe(32))
    server_type=PanelHTTPServer if runtime else ThreadingHTTPServer
    server=server_type(('127.0.0.1',args.port),Handler)
    server.legacy_origin=f'http://127.0.0.1:{server.server_port}'
    if runtime:
        server.panel_config=config; server.panel_runtime=runtime
        server.panel_admin_enabled=True
        if not config.test:
            import ssl
            context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.minimum_version=ssl.TLSVersion.TLSv1_2
            context.load_cert_chain(args.panel_tls_cert,args.panel_tls_key)
            server.socket=context.wrap_socket(server.socket,server_side=True)
    server.registry=Registry(args.data/'registry.db')
    from speedtest import SpeedTests
    server.speed_tests=SpeedTests(server.registry,local_server_id=args.speed_local_server_id,xray_binary=args.speed_xray_binary)
    server.token=tokenfile.read_text().strip()
    scheme='https' if runtime and not config.test else 'http'
    print(f'Independent API: {scheme}://127.0.0.1:{server.server_port}/health; administrator token stored in {tokenfile}',flush=True)
    import signal
    def stop(_signal,_frame): raise KeyboardInterrupt
    signal.signal(signal.SIGTERM,stop)
    orderly=False
    try:
        server.serve_forever()
        orderly=True
    except KeyboardInterrupt: orderly=True
    finally:
        if runtime:
            try:
                if not orderly: raise Error('面板服务异常停止',503)
                server.server_close()
                server.speed_tests.close()
                runtime.close()
            except BaseException:
                # Interrupted/failed drain must never become a clean restart.
                os._exit(78)
        else:
            server.speed_tests.close()
            server.server_close()

if __name__=='__main__': main()
