(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.PanelSession = factory();
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';

  var ROUTES = Object.freeze({
    challenge: ['login-challenge', 'GET'], login: ['login', 'POST'], me: ['me', 'GET'],
    password: ['password', 'POST'], logout: ['logout', 'POST']
  });

  function SessionError(code, message, detail) {
    this.name = 'PanelSessionError'; this.code = code; this.message = message; this.detail = detail;
    if (Error.captureStackTrace) Error.captureStackTrace(this, SessionError);
  }
  SessionError.prototype = Object.create(Error.prototype);
  SessionError.prototype.constructor = SessionError;

  function timeoutSignal(ms, external) {
    var controller = typeof AbortController === 'function' ? new AbortController() : null;
    var timer = null;
    var cancelResolve;
    var cancelPromise = new Promise(function (resolve) { cancelResolve = resolve; });
    var signal = controller ? controller.signal : external;
    function cancel(reason) { if (controller) controller.abort(); cancelResolve(reason); }
    if (external) {
      if (external.aborted) cancel('cancelled');
      else external.addEventListener('abort', function () { cancel('cancelled'); }, { once: true });
    }
    if (ms > 0) timer = setTimeout(function () { cancel('timeout'); }, ms);
    return { signal: signal, promise: cancelPromise, abort: function () { cancel('cancelled'); }, clear: function () { if (timer) clearTimeout(timer); } };
  }

  function create(options) {
    options = options || {};
    var fetchImpl = options.fetch || (typeof fetch === 'function' ? fetch.bind(globalThis) : null);
    if (!fetchImpl) throw new Error('fetch不可用');
    var browserOrigin = typeof location !== 'undefined' ? location.origin : null;
    var expected = browserOrigin || options.origin || null;
    if (!expected) throw new Error('必须提供可信origin');
    var expectedUrl;
    try { expectedUrl = new URL(expected); } catch (_) { throw new Error('origin无效'); }
    if (expectedUrl.username || expectedUrl.password || expectedUrl.pathname !== '/' || expectedUrl.search || expectedUrl.hash) throw new Error('origin无效');
    if (expectedUrl.protocol !== 'https:' && !(options.allowLoopbackHttp === true && expectedUrl.protocol === 'http:' && (expectedUrl.hostname === '127.0.0.1' || expectedUrl.hostname === '::1' || expectedUrl.hostname === '[::1]'))) throw new Error('仅允许HTTPS或显式loopback HTTP');
    var base = options.baseUrl || '';
    if (base) {
      var parsed;
      try { parsed = new URL(base, expectedUrl.origin); } catch (_) { throw new Error('baseUrl无效'); }
      if (parsed.username || parsed.password || parsed.pathname !== '/' || parsed.search || parsed.hash) throw new Error('baseUrl必须是无路径同源origin');
      if (parsed.origin !== expectedUrl.origin) throw new Error('baseUrl必须匹配受信origin');
      base = parsed.origin;
    }
    var timeout = Number.isFinite(options.timeoutMs) ? options.timeoutMs : 10000;
    var disposed = false, serial = Promise.resolve(), writeBusy = false, csrf = null, snapshot = null, generation = 0, active = new Set();
    var listeners = [];

    function emit(status, extra) {
      var state = Object.assign({ status: status, account: snapshot ? Object.assign({}, snapshot) : null, generation: generation }, extra || {});
      listeners.slice().forEach(function (fn) { try { fn(state); } catch (_) {} });
      return state;
    }
    function guard() { if (disposed) throw new SessionError('disposed', '会话控制器已销毁'); }
    function path(name) {
      var route = ROUTES[name];
      if (!route) throw new SessionError('route', '身份路由不允许');
      return base.replace(/\/$/, '') + '/panel/api/v1/' + route[0];
    }
    function bodyFor(name, data) {
      var allowed = { login: ['login', 'password'], password: ['old_password', 'new_password'], logout: [] }[name];
      if (!allowed || !data || Object.keys(data).length !== allowed.length || allowed.some(function (k) { return typeof data[k] !== 'string'; }))
        throw new SessionError('protocol', '请求字段无效');
      var out = {}; allowed.forEach(function (k) { out[k] = data[k]; }); return out;
    }
    function invalidate(status) { csrf = null; snapshot = null; generation += 1; emit(status || 'anonymous'); }
    function check(signal, op) { if (disposed || (signal && signal.aborted) || (op !== undefined && generation !== op)) throw new SessionError(disposed ? 'disposed' : 'cancelled', '请求结果已失效'); }
    async function request(name, data, signal, op) {
      guard();
      check(signal, op);
      var route = ROUTES[name], timer = timeoutSignal(timeout, signal), headers = {};
      if (timer.signal && timer.signal.addEventListener) active.add(timer);
      if (route[1] === 'POST') { headers['Content-Type'] = 'application/json'; headers['X-CSRF-Token'] = csrf || ''; }
      try {
        check(signal, op);
        var requestBody = route[1] === 'POST' ? JSON.stringify(bodyFor(name, data || {})) : undefined;
        check(signal, op);
        var fetchPromise = fetchImpl(path(name), { method: route[1], credentials: 'same-origin', redirect: 'error', cache: 'no-store', headers: headers, body: requestBody, signal: timer.signal });
        fetchPromise.catch(function () {});
        var response = await Promise.race([fetchPromise, timer.promise.then(function (reason) { throw new SessionError(reason, reason === 'timeout' ? '身份请求超时' : '请求已取消'); })]);
        check(signal, op);
        var payload;
        try { payload = await Promise.race([response.json(), timer.promise.then(function (reason) { throw new SessionError(reason, reason === 'timeout' ? '身份请求超时' : '请求已取消'); })]); } catch (error) { if (error instanceof SessionError) throw error; throw new SessionError('protocol', '响应不是JSON', response.status); }
        check(signal, op);
        if (!response.ok) throw new SessionError('http', '身份请求失败', { status: response.status });
        if (!payload || typeof payload !== 'object' || Array.isArray(payload)) throw new SessionError('protocol', '响应结构无效');
        return payload;
      } catch (error) {
        if (error instanceof SessionError) throw error;
        if (error && error.name === 'AbortError') throw new SessionError(signal && signal.aborted ? 'cancelled' : 'timeout', signal && signal.aborted ? '请求已取消' : '身份请求超时');
        throw new SessionError('network', '身份请求失败');
      } finally { timer.clear(); active.delete(timer); }
    }
    function enqueue(label, fn, writing) {
      if (disposed) return Promise.reject(new SessionError('disposed', '会话控制器已销毁'));
      if (writing && writeBusy) return Promise.reject(new SessionError('busy', label + '请求正在进行中'));
      var run = function () { if (disposed) throw new SessionError('disposed', '会话控制器已销毁'); return fn(); };
      if (writing) writeBusy = true;
      var next = serial.then(run, run);
      serial = next.then(function () { if (writing) writeBusy = false; }, function () { if (writing) writeBusy = false; });
      return next;
    }
    async function challenge(signal, op) {
      var p = await request('challenge', null, signal, op);
      check(signal, op);
      if (typeof p.csrf_token !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(p.csrf_token)) throw new SessionError('protocol', '挑战响应无效');
      csrf = p.csrf_token; emit('challenge'); return { ok: true };
    }
    async function login(loginValue, password, signal) {
      return enqueue('登录', async function () {
        var op = generation; emit('login_pending'); try {
        await challenge(signal, op); check(signal, op);
        var p = await request('login', { login: loginValue, password: password }, signal, op);
        check(signal, op);
        if (p.authenticated !== true) throw new SessionError('protocol', '登录响应无效');
        var identity = await request('me', null, signal, op); check(signal, op);
        if (!identity || typeof identity.id !== 'string' || (identity.role !== 'admin' && identity.role !== 'user') || typeof identity.must_change_password !== 'boolean' || typeof identity.csrf_token !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(identity.csrf_token)) throw new SessionError('protocol', '身份响应无效');
        csrf = identity.csrf_token; snapshot = { id: identity.id, role: identity.role, must_change_password: identity.must_change_password }; generation += 1; emit(identity.must_change_password ? 'password_change_required' : 'authenticated');
        return Object.assign({}, snapshot);
        } catch (error) { invalidate('anonymous'); throw error; }
      }, true);
    }
    function me(signal) { return enqueue('身份确认', async function () { var op=generation; try { var p = await request('me', null, signal, op); check(signal, op); if (typeof p.id !== 'string' || (p.role !== 'admin' && p.role !== 'user') || typeof p.must_change_password !== 'boolean' || typeof p.csrf_token !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(p.csrf_token)) throw new SessionError('protocol', '身份响应无效'); csrf = p.csrf_token; snapshot = { id: p.id, role: p.role, must_change_password: p.must_change_password }; generation += 1; emit(p.must_change_password ? 'password_change_required' : 'authenticated'); return Object.assign({}, snapshot); } catch (e) { invalidate('anonymous'); throw e; } }, false); }
    function changePassword(oldPassword, newPassword, signal) { return enqueue('改密', async function () { var op=generation; emit('password_pending'); try { var p = await request('password', { old_password: oldPassword, new_password: newPassword }, signal, op); check(signal, op); if (p.completed !== true) throw new SessionError('protocol', '改密响应无效'); invalidate('reauth_required'); return { reauth_required: true }; } catch(e) { invalidate('reauth_required'); throw e; } }, true); }
    function logout(signal) { return enqueue('退出', async function () { var op=generation; emit('logout_pending'); try { var p = await request('logout', {}, signal, op); check(signal, op); if (p.completed !== true) throw new SessionError('protocol', '退出响应无效'); invalidate('anonymous'); return { completed: true }; } catch(e) { invalidate('anonymous'); throw e; } }, true); }
    return Object.freeze({
      challenge: function (signal) { return enqueue('挑战', function () { return challenge(signal); }, false); },
      login: login, me: me, changePassword: changePassword, logout: logout,
      getState: function () { return { status: snapshot ? (snapshot.must_change_password ? 'password_change_required' : 'authenticated') : 'anonymous', account: snapshot ? Object.assign({}, snapshot) : null, generation: generation }; },
      subscribe: function (fn) { if (typeof fn !== 'function') throw new TypeError('订阅函数无效'); listeners.push(fn); return function () { listeners = listeners.filter(function (x) { return x !== fn; }); }; },
      dispose: function () { disposed = true; generation += 1; csrf = null; snapshot = null; active.forEach(function (t) { t.abort(); }); listeners = []; serial.catch(function () {}); }
    });
  }
  return { create: create, SessionError: SessionError };
}));
