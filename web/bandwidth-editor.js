/* Explicit in-memory fixture. This page never constructs a network transport. */
(() => {
  'use strict';
  const copy = value => JSON.parse(JSON.stringify(value));
  const stored = new Map([[101, {upload_bps: 125000, download_bps: null}], [102, null]]);
  const fixture = {calls: [], failNext: null, delay: 100};
  async function request(spec) {
    fixture.calls.push(copy(spec));
    await new Promise(resolve => setTimeout(resolve, fixture.delay));
    const match = /^\/api\/plans\/1\/nodes\/(101|102)\/bandwidth$/.exec(spec.path);
    if (!match || !['GET', 'PUT', 'DELETE'].includes(spec.method)) throw new Error('夹具资源不存在');
    if (fixture.failNext === spec.method) { fixture.failNext = null; throw new Error('模拟请求失败'); }
    const node = Number(match[1]);
    if (spec.method === 'PUT') stored.set(node, copy(spec.body));
    if (spec.method === 'DELETE') stored.set(node, null);
    return {plan_id: 1, node_id: node, override: copy(stored.get(node))};
  }
  const el = id => document.getElementById(id);
  let currentState;
  const editor = window.BandwidthResource.create({
    request,
    onLoad: value => window.BandwidthForm.load(value),
    onState(state) {
      currentState = state;
      el('editor-fields').disabled = !state.canSave;
      el('resource').disabled = state.writing;
      el('reload').disabled = state.writing || state.phase === 'loading';
      el('bandwidth-form').setAttribute('aria-busy', String(state.writing || state.phase === 'loading'));
      el('resource-status').textContent = state.message;
      el('form-status').textContent = state.message;
    },
  });
  el('bandwidth-form').addEventListener('submit', () => {
    const decision = window.BandwidthForm.getPreview();
    if (decision) void editor.save(decision);
  });
  el('bandwidth-form').addEventListener('input', () => {
    if (currentState.canSave) el('resource-status').textContent = '草稿已修改，尚未保存。';
  });
  el('resource').addEventListener('change', () => void editor.select({plan_id: 1, node_id: Number(el('resource').value)}));
  el('reload').addEventListener('click', () => void editor.reload());
  window.BandwidthFixture = fixture;
  window.BandwidthEditor = editor;
  void editor.select({plan_id: 1, node_id: 101});
})();
