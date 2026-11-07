/* Test-host page: explicit credential entry, same-origin transport, no storage. */
(() => {
  'use strict';
  const el = id => document.getElementById(id);
  let active = null, authorized = false;
  let authorizationGeneration = 0;
  let state = {phase: 'idle', canSave: false, writing: false};
  const busy = () => state.writing || ['loading', 'rechecking'].includes(state.phase);
  function render() {
    el('editor-fields').disabled = !authorized || !state.canSave;
    el('resource').disabled = !authorized || busy();
    el('reload').disabled = !authorized || busy();
    for (const id of ['admin-token', 'authorize', 'forget']) el(id).disabled = busy();
    el('bandwidth-form').setAttribute('aria-busy', String(busy()));
    el('auth-form').setAttribute('aria-busy', String(busy()));
  }
  const editor = window.BandwidthResource.create({
    async request(spec) {
      if (!active) throw new Error('需要显式授权');
      const generation = authorizationGeneration;
      try {return await active(spec);}
      catch (error) {
        if (generation === authorizationGeneration) {
          if (error.code === 'auth') authorized = false;
          el('auth-status').textContent = error.message;
        }
        throw error;
      }
    },
    onLoad: value => window.BandwidthForm.load(value),
    onState(next) {
      state = next; render();
      el('resource-status').textContent = next.message;
      el('form-status').textContent = next.message;
    },
  });
  el('auth-form').addEventListener('submit', async event => {
    event.preventDefault();
    if (busy()) return;
    const generation = ++authorizationGeneration;
    active = null; authorized = false;
    const input = el('admin-token');
    try {
      active = window.BandwidthHTTP.create({baseURL: location.origin, token: input.value});
    } catch (error) {el('auth-status').textContent = error.message; render(); return;}
    finally {input.value = '';}
    authorized = true;
    el('auth-status').textContent = '正在验证授权…';
    const ok = await (state.resource ? editor.recheck() : editor.select({plan_id: 1, node_id: Number(el('resource').value)}));
    if (generation !== authorizationGeneration) return;
    if (ok) el('auth-status').textContent = '授权有效，仅在当前页面内存中使用。';
    else if (state.phase === 'conflict') el('auth-status').textContent = '授权有效，配置有变化；请重新读取。';
    render();
  });
  function forget() {
    if (busy()) return;
    authorizationGeneration++;
    active = null; authorized = false; el('admin-token').value = '';
    el('auth-status').textContent = '当前授权已清除，草稿保留。'; render();
  }
  el('forget').addEventListener('click', forget);
  window.addEventListener('pagehide', () => {authorizationGeneration++; active = null; authorized = false; el('admin-token').value = ''; el('auth-status').textContent = '当前授权已清除，草稿保留。'; render();});
  el('bandwidth-form').addEventListener('submit', () => {
    if (authorized && !busy()) {
      const decision = window.BandwidthForm.getPreview();
      if (decision) void editor.save(decision);
    }
  });
  el('bandwidth-form').addEventListener('input', () => {
    if (state.canSave && authorized) el('resource-status').textContent = '草稿已修改，尚未保存。';
  });
  el('resource').addEventListener('change', () => {
    if (authorized && !busy()) void editor.select({plan_id: 1, node_id: Number(el('resource').value)});
  });
  el('reload').addEventListener('click', () => {if (authorized && !busy()) void editor.reload();});
  render();
})();
