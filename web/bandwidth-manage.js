/* Independent API management; explicit in-memory Bearer, authenticated catalog. */
(() => {
  'use strict';
  const el = id => document.getElementById(id), catalog = window.BandwidthCatalog;
  let active = null, authorized = false, generation = 0, catalogBusy = false, catalogReady = false;
  let plans = [], nodes = [], available = [];
  let state = {phase: 'idle', canSave: false, writing: false};
  const busy = () => catalogBusy || state.writing || ['loading', 'rechecking'].includes(state.phase);
  const target = () => ({plan_id: Number(el('plan').value), node_id: Number(el('resource').value)});
  const matches = () => catalogReady && plans.some(p => p.id === target().plan_id) && available.some(n => n.id === target().node_id) &&
    state.resource?.plan_id === target().plan_id && state.resource?.node_id === target().node_id;
  function render() {
    el('editor-fields').disabled = !authorized || busy() || !matches() || !state.canSave;
    el('plan').disabled = !authorized || busy() || !catalogReady || !plans.length;
    el('resource').disabled = !authorized || busy() || !catalogReady || !available.length;
    el('reload').disabled = !authorized || busy() || !matches();
    el('refresh-resources').disabled = !authorized || busy();
    for (const id of ['admin-token', 'authorize', 'forget']) el(id).disabled = busy();
    el('bandwidth-form').setAttribute('aria-busy', String(busy()));
    el('auth-form').setAttribute('aria-busy', String(busy()));
  }
  function options(id, rows, selected, placeholder) {
    const select = el(id); select.replaceChildren(new Option(placeholder, ''));
    for (const row of rows) select.add(new Option(row.name, String(row.id)));
    select.value = rows.some(row => row.id === selected) ? String(selected) : '';
  }
  async function request(spec) {
    if (!active) throw new Error('请先授权');
    const ticket = generation;
    try {return await active(spec);}
    catch (error) {
      if (ticket === generation) {
        if (error.code === 'auth') authorized = false;
        el('auth-status').textContent = error.message;
      }
      throw error;
    }
  }
  const editor = window.BandwidthResource.create({request, requireVersion: true,
    onLoad: value => window.BandwidthForm.load(value),
    onState(next) {state = next; render(); el('resource-status').textContent = next.message; el('form-status').textContent = next.message;},
  });
  async function refresh() {
    const ticket = generation;
    const wantedPlan = Number(el('plan').value) || state.resource?.plan_id;
    const wantedNode = Number(el('resource').value) || (state.resource?.plan_id === wantedPlan ? state.resource?.node_id : null);
    catalogBusy = true; catalogReady = false; render();
    el('resource-status').textContent = '正在读取套餐与节点列表…';
    try {
      const raw = await Promise.all([request({method: 'GET', path: '/api/plans'}), request({method: 'GET', path: '/api/nodes'})]);
      if (ticket !== generation || !authorized) return false;
      const nextPlans = catalog.list(raw[0]), nextNodes = catalog.list(raw[1]);
      const pid = nextPlans.some(p => p.id === wantedPlan) ? wantedPlan : state.resource ? null : nextPlans[0]?.id;
      const nextAvailable = pid ? catalog.eligible(nextNodes, await request({method: 'GET', path: '/api/plans/'+pid+'/nodes'}), pid) : [];
      if (ticket !== generation || !authorized) return false;
      plans = nextPlans; nodes = nextNodes; available = nextAvailable;
      options('plan', plans, pid, plans.length ? '请选择套餐' : '暂无套餐');
      const nid = available.some(n => n.id === wantedNode) ? wantedNode : state.resource ? null : available[0]?.id;
      options('resource', available, nid, available.length ? '请选择节点' : '暂无可用节点');
      catalogReady = true;
      if (!pid || !nid) {
        el('resource-status').textContent = !plans.length ? '暂无套餐，请创建套餐后刷新。' :
          !available.length && pid ? '该套餐暂无可用节点。草稿保留且不能保存。' : '原资源已不在列表中，请重新选择。草稿保留且不能保存。';
        return false;
      }
      const result = state.resource?.plan_id === pid && state.resource?.node_id === nid ? await editor.recheck() : await editor.select({plan_id: pid, node_id: nid});
      if (!result && state.phase === 'conflict') el('resource-status').textContent = state.message;
      return result;
    } catch (_) {
      if (ticket === generation) el('resource-status').textContent = '资源列表读取失败，请重新授权或刷新。草稿保留且不能保存。';
      return false;
    } finally {if (ticket === generation) {catalogBusy = false; render();}}
  }
  async function choosePlan() {
    if (busy() || !authorized || !catalogReady) return;
    const pid = Number(el('plan').value), ticket = generation;
    available = []; options('resource', [], null, '正在读取节点…');
    if (!plans.some(p => p.id === pid)) {options('resource', [], null, '请先选择套餐'); el('resource-status').textContent = '请选择套餐，草稿保留且不能保存。'; render(); return;}
    catalogBusy = true; render();
    try {
      const nextAvailable = catalog.eligible(nodes, await request({method: 'GET', path: '/api/plans/'+pid+'/nodes'}), pid);
      if (ticket !== generation || !authorized) return;
      available = nextAvailable;
      options('resource', available, available[0]?.id, '暂无可用节点');
      if (available.length) await editor.select({plan_id: pid, node_id: available[0].id});
      else el('resource-status').textContent = '该套餐暂无可用节点。草稿保留且不能保存。';
    } catch (_) {if (ticket === generation) {catalogReady = false; el('resource-status').textContent = '节点范围读取失败，请刷新资源列表。草稿保留且不能保存。';}}
    finally {if (ticket === generation) {catalogBusy = false; render();}}
  }
  el('auth-form').addEventListener('submit', async event => {
    event.preventDefault(); if (busy()) return;
    const ticket = ++generation; active = null; authorized = false; catalogReady = false;
    try {active = window.BandwidthHTTP.create({baseURL: location.origin, token: el('admin-token').value});}
    catch (error) {el('auth-status').textContent = error.message; render(); return;}
    finally {el('admin-token').value = '';}
    authorized = true; el('auth-status').textContent = '正在验证授权并读取资源…';
    await refresh();
    if (ticket !== generation) return;
    if (authorized && catalogReady) el('auth-status').textContent = '授权有效，仅在当前页面内存中使用。';
    render();
  });
  function clear() {
    generation++; active = null; authorized = false; catalogReady = false; catalogBusy = false;
    el('admin-token').value = ''; el('auth-status').textContent = '当前授权已清除，草稿保留。'; render();
  }
  el('forget').addEventListener('click', () => {if (!busy()) clear();});
  window.addEventListener('pagehide', clear);
  el('refresh-resources').addEventListener('click', () => {if (authorized && !busy()) void refresh();});
  el('plan').addEventListener('change', () => void choosePlan());
  el('resource').addEventListener('change', () => {
    if (authorized && !busy() && catalogReady && available.some(n => n.id === target().node_id)) void editor.select(target());
  });
  el('reload').addEventListener('click', () => {if (authorized && !busy() && matches()) void editor.reload();});
  el('bandwidth-form').addEventListener('submit', () => {
    if (authorized && !busy() && matches() && state.canSave) {
      const decision = window.BandwidthForm.getPreview(); if (decision) void editor.save(decision);
    }
  });
  el('bandwidth-form').addEventListener('input', () => {
    if (authorized && matches()) el('resource-status').textContent = '草稿已修改，尚未保存。';
  });
  render();
})();
