/* Explicit administrator transport. No credential discovery, persistence or retries. */
(function (root, factory) {
  const values = typeof module === 'object' && module.exports ? require('./servers-model.js') : root.ServersModel;
  const api = factory(values);
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.ServersHTTP = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function (values) {
  'use strict';
  class TransportError extends Error {
    constructor(code, status = null) {
      const messages = {input: 'HTTP配置或请求无效', auth: '管理员凭据无效或已失效',
        forbidden: '当前操作没有权限', conflict: '服务器配置已变化，请重新读取后再编辑', http: '服务器未接受请求', protocol: '服务器响应格式无效', network: '连接失败，结果可能未确认', timeout: '请求超时，结果可能未确认'};
      super(messages[code]); this.name = 'ServersHTTPError'; this.code = code; this.status = status;
    }
  }
  function create({baseURL, token, timeoutMs = 10000, fetchImpl = globalThis.fetch} = {}) {
    let base;
    try {base = new URL(baseURL);} catch (_) {throw new TransportError('input');}
    if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password || base.search || base.hash ||
        base.pathname !== '/' || (base.protocol === 'http:' && !['127.0.0.1', '[::1]', 'localhost'].includes(base.hostname)) ||
        typeof token !== 'string' || !/^[\x21-\x7e]+$/.test(token) ||
        !Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30000 || typeof fetchImpl !== 'function') throw new TransportError('input');
    const origin = base.origin;
    return async function request(spec) {
      let body;
      let list = false;
      try {
        const edit = typeof spec?.path === 'string' && /^\/api\/manage\/servers\/([1-9]\d*)$/.exec(spec.path);
        const catalog = spec?.path === '/api/manage/servers';
        const isEdit = edit && Number.isSafeInteger(Number(edit[1])) && spec.method === 'PUT';
        const valid = catalog && ['GET','POST'].includes(spec.method) || isEdit;
        const keys = ['method','path',...(['POST','PUT'].includes(spec?.method)?['body']:[]),...(isEdit?['ifMatch']:[])].sort().join(',');
        if(!valid || Object.keys(spec).sort().join(',') !== keys || isEdit && !values.validVersion(spec.ifMatch)) throw new Error();
        list = spec.method === 'GET';
        if(['POST','PUT'].includes(spec.method)) body=JSON.stringify(values.body(spec.body));
      } catch (_) {throw new TransportError('input');}
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const response = await fetchImpl(origin + spec.path, {method: spec.method,
          headers: {Authorization: 'Bearer ' + token, Accept: 'application/json', ...(body === undefined ? {} : {'Content-Type': 'application/json'}),
            ...(Object.hasOwn(spec,'ifMatch') ? {'If-Match':spec.ifMatch} : {})},
          ...(body === undefined ? {} : {body}), credentials: 'omit', redirect: 'error', cache: 'no-store', signal: controller.signal});
        if (!response.ok) throw new TransportError(response.status === 401 ? 'auth' : response.status === 403 ? 'forbidden' : response.status === 412 ? 'conflict' : 'http', response.status);
        if (!/^application\/(?:json|[^;\s]+\+json)(?:\s*;|$)/i.test(response.headers.get('content-type') || '')) throw new TransportError('protocol');
        const text = await response.text();
        if (text.length > (list ? 1048576 : 16384)) throw new TransportError('protocol');
        let data;
        try {data = JSON.parse(text);} catch (_) {throw new TransportError('protocol');}
        if (!data || typeof data !== 'object' || Array.isArray(data) !== list) throw new TransportError('protocol');
        return data;
      } catch (error) {
        if (error instanceof TransportError) throw error;
        throw new TransportError(controller.signal.aborted ? 'timeout' : 'network');
      } finally {clearTimeout(timer);}
    };
  }
  return Object.freeze({create, TransportError});
});
