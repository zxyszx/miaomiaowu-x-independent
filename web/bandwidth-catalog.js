(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BandwidthCatalog = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';
  const validID = id => Number.isSafeInteger(id) && id > 0;
  function list(rows) {
    if (!Array.isArray(rows)) throw new Error('资源列表格式无效');
    const seen = new Set();
    return rows.map(row => {
      if (!row || !validID(row.id) || seen.has(row.id) || typeof row.name !== 'string' || !row.name.trim()) throw new Error('资源列表格式无效');
      seen.add(row.id); return {id: row.id, name: row.name};
    });
  }
  function eligible(nodes, scope, planID) {
    if (!validID(planID) || !scope || scope.plan_id !== planID || typeof scope.restricted !== 'boolean' || !Array.isArray(scope.node_ids) ||
        !scope.node_ids.every(validID) || new Set(scope.node_ids).size !== scope.node_ids.length) throw new Error('节点范围格式无效');
    const known = new Set(nodes.map(node => node.id));
    if (scope.node_ids.some(id => !known.has(id))) throw new Error('节点列表已变化，请刷新');
    return nodes.filter(node => !scope.restricted || scope.node_ids.includes(node.id));
  }
  return Object.freeze({list, eligible});
});
