"""Credential classification before every Handler method and namespace."""
import re
from urllib.parse import urlsplit

from core import Error

PRODUCTION = ('__Host-mmw_panel', '__Host-mmw_panel_challenge')
TEST = ('mmw_panel_test', 'mmw_panel_test_challenge')


class PanelConfig:
    def __init__(self, origin, *, loopback_test=False):
        value = urlsplit(origin)
        if (value.scheme != ('http' if loopback_test else 'https') or not value.netloc or
            value.username or value.password or value.path or value.query or value.fragment or
            (loopback_test and value.hostname not in ('127.0.0.1', 'localhost', '::1'))):
            raise Error('必须配置固定HTTPS来源或显式loopback测试来源', 503)
        try: value.port
        except ValueError: raise Error('来源端口无效', 503)
        self.origin = origin
        self.host = value.netloc
        self.test = loopback_test
        self.session, self.challenge = TEST if loopback_test else PRODUCTION


def classify(handler):
    headers = handler.headers
    auth = headers.get_all('Authorization', [])
    cookie_headers = headers.get_all('Cookie', [])
    if len(auth)>1 or len(cookie_headers)>1: raise Error('重复身份请求头', 400)
    cookies = {}
    if cookie_headers:
        for item in cookie_headers[0].split(';'):
            item = item.strip()
            match = re.fullmatch(r"([!#$%&'*+.^_`|~0-9A-Za-z-]+)=([!#-:<-\[\]-~]*)", item)
            if not match or match[1] in cookies: raise Error('Cookie格式无效或重复', 400)
            cookies[match[1]] = match[2]
    panel = {k:v for k,v in cookies.items() if k.startswith(('__Host-mmw_panel','mmw_panel_test'))}
    if auth and panel: raise Error('ambiguous_credentials', 400)
    path = urlsplit(handler.path).path
    if auth and not re.fullmatch(r'Bearer [A-Za-z0-9._~+/\-]+=*', auth[0]):
        # Preserve legacy missing-token 401, still before any business dispatch.
        status=401 if auth[0] in ('Bearer','Bearer ') and not path.startswith('/panel/') else 400
        raise Error('身份凭据格式无效', status)
    config = getattr(handler.server, 'panel_config', None)
    origins = headers.get_all('Origin', [])
    if len(origins)>1: raise Error('来源重复', 400)
    trusted=config.origin if config else getattr(handler.server,'legacy_origin',None)
    if origins and origins[0] != trusted: raise Error('来源不允许', 403)
    admin_cookie=bool(panel) and path.startswith('/api/') and getattr(handler.server,'panel_admin_enabled',False)
    if path.startswith('/panel/') or admin_cookie:
        if auth: raise Error('面板不接受Bearer身份', 403)
        if config is None: raise Error('面板身份未配置', 503)
        hosts = headers.get_all('Host', [])
        if hosts != [config.host]: raise Error('Host不允许', 403)
        if headers.get_all('Sec-Fetch-Site', []) not in ([], ['same-origin'], ['none']):
            raise Error('跨站请求不允许', 403)
        if set(panel)-{config.session,config.challenge}: raise Error('Cookie命名空间不允许', 403)
        if admin_cookie and set(panel)!={config.session}: raise Error('管理接口只接受面板会话Cookie',403)
        if admin_cookie and handler.command not in ('GET','POST','PUT','DELETE'):
            raise Error('面板管理方法不允许',403)
        if any(not re.fullmatch('[A-Za-z0-9_-]{43}', value) for value in panel.values()):
            raise Error('面板Cookie无效', 401)
        if handler.command != 'GET' and origins != [config.origin]: raise Error('需要同源Origin', 403)
    elif panel:
        # P2a deliberately denies even admin sessions before legacy business.
        raise Error('面板Cookie不能访问此命名空间', 403)
    return cookies
