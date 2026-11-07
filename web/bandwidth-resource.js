/* Resource contract only. No default transport, credentials, or HTTP connection. */
(function (root, factory) {
  const values = typeof module === 'object' && module.exports ? require('./field-values.js') : root.MiaomiaowuFieldValues;
  const api = factory(values);
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BandwidthResource = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function (values) {
  'use strict';
  const clone = value => JSON.parse(JSON.stringify(value));
  function identity(resource) {
    if (!resource || !Number.isSafeInteger(resource.plan_id) || resource.plan_id <= 0 ||
        !Number.isSafeInteger(resource.node_id) || resource.node_id <= 0) throw new Error('资源编号无效');
    return {plan_id: resource.plan_id, node_id: resource.node_id};
  }
  function create({request, onLoad, onState, requireVersion = false}) {
    if (typeof request !== 'function' || typeof onLoad !== 'function' || typeof onState !== 'function') throw new Error('需要显式数据源和视图');
    let resource = null, generation = 0, phase = 'idle', writing = false;
    let baseline, hasBaseline = false, version = null;
    const equal = (a, b) => a === null ? b === null : b !== null && a.upload_bps === b.upload_bps && a.download_bps === b.download_bps;
    function state(next, message) {
      phase = next;
      onState({phase, resource: clone(resource), canSave: phase === 'ready', writing, message});
    }
    const path = target => '/api/plans/' + target.plan_id + '/nodes/' + target.node_id + '/bandwidth';
    function decode(data, target) {
      if (!data || data.plan_id !== target.plan_id || data.node_id !== target.node_id ||
          !Object.hasOwn(data, 'override')) throw new Error('资源响应不匹配');
      values.bandwidthToForm(data.override);
      if (requireVersion && (typeof data.version !== 'string' || !/^"bw1-[0-9a-f]{64}"$/.test(data.version))) throw new Error('资源版本无效');
      return clone(data.override);
    }
    async function select(next) {
      const target = identity(next);
      if (writing) return false;
      const ticket = ++generation;
      resource = target;
      hasBaseline = false;
      state('loading', '正在读取所选资源…');
      try {
        const data = await request({method: 'GET', path: path(target)});
        if (ticket !== generation) return false;
        const override = decode(data, target);
        baseline = clone(override); hasBaseline = true; version = data.version;
        onLoad(override);
        state('ready', '配置已读取。');
        return true;
      } catch (_) {
        if (ticket === generation) state('error', '读取失败，当前草稿保留但不能保存。请重新读取。');
        return false;
      }
    }
    async function save(decision) {
      if (phase !== 'ready' || writing) return false;
      // Revalidate at the adapter boundary; a preview is not a request body.
      if (!decision || typeof decision.inherit !== 'boolean' ||
          Object.keys(decision).sort().join(',') !== (decision.inherit ? 'inherit' : 'bandwidth,inherit')) throw new Error('保存决策无效');
      if (!decision.inherit) {
        if (decision.bandwidth === null) throw new Error('覆写必须为对象');
        values.bandwidthToForm(decision.bandwidth);
      }
      const target = clone(resource);
      const inherit = decision.inherit;
      const body = inherit ? undefined : clone(decision.bandwidth);
      writing = true;
      state('saving', '正在保存并重新读取，请稍候…');
      try {
        await request({method: inherit ? 'DELETE' : 'PUT', path: path(target), ...(body === undefined ? {} : {body}),
          ...(requireVersion ? {ifMatch: version} : {})});
        const data = await request({method: 'GET', path: path(target)});
        const override = decode(data, target);
        // An ACK alone is insufficient; the reread must equal the submitted value.
        const expected = inherit ? null : body;
        if (override === null ? expected !== null : expected === null ||
            override.upload_bps !== expected.upload_bps || override.download_bps !== expected.download_bps) throw new Error('重读不一致');
        baseline = clone(override); hasBaseline = true; version = data.version;
        onLoad(override);
        writing = false;
        state('ready', '保存后已重新读取并确认。');
        return true;
      } catch (error) {
        writing = false;
        if (error.code === 'conflict') state('conflict', '服务器配置已变化，草稿已保留。请重新读取配置后再编辑。');
        else state('unconfirmed', '保存结果未确认，草稿已保留。请先重新读取核对；不会自动重复写入。');
        return false;
      }
    }
    async function recheck() {
      if (!resource || writing || phase === 'conflict') return false;
      if (!hasBaseline) return select(resource);
      const ticket = ++generation, target = clone(resource);
      state('rechecking', '正在重新验证配置，草稿保留…');
      try {
        const data = await request({method: 'GET', path: path(target)});
        const override = decode(data, target);
        if (ticket !== generation) return false;
        if (!equal(override, baseline) || requireVersion && data.version !== version) {
          state('conflict', '服务器配置已变化，草稿已保留。请重新读取配置后再编辑。');
          return false;
        }
        state('ready', '重新验证通过，草稿未改动。');
        return true;
      } catch (_) {
        if (ticket === generation) state('error', '重新验证失败，草稿保留且不能保存。请重新授权或重新读取。');
        return false;
      }
    }
    return Object.freeze({select, save, recheck, reload: () => resource ? select(resource) : Promise.resolve(false)});
  }
  return Object.freeze({create});
});
