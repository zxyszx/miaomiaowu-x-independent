/* Isolated interaction only: no fetch, storage, credentials, or API writes. */
(() => {
  'use strict';
  const values = window.MiaomiaowuFieldValues;
  const form = document.getElementById('bandwidth-form');
  const el = id => document.getElementById(id);
  const directions = ['upload', 'download'];
  let baseline;
  let preview = null;
  const clone = value => JSON.parse(JSON.stringify(value));
  const inherited = () => form.elements.source.value === 'inherit';

  function clearError(direction) {
    el(direction + '-error').textContent = '';
    el(direction + '-rate').removeAttribute('aria-invalid');
  }

  function invalidate() {
    preview = null;
    el('preview').hidden = true;
    el('preview-text').textContent = '';
    el('form-errors').textContent = '';
    el('form-status').textContent = '设置已修改，请重新生成预览。';
  }

  function sync() {
    for (const direction of directions) {
      el(direction + '-group').disabled = inherited();
      el(direction + '-rate').disabled = inherited() || el(direction + '-kind').value === 'unlimited';
      if (el(direction + '-rate').disabled) clearError(direction);
    }
  }

  function validate(direction) {
    clearError(direction);
    if (el(direction + '-rate').disabled) return true;
    try { values.mbpsToBytes(el(direction + '-rate').value); return true; }
    catch (error) {
      el(direction + '-error').textContent = error.message;
      el(direction + '-rate').setAttribute('aria-invalid', 'true');
      return false;
    }
  }

  function load(override) {
    // Validate the entire incoming value before touching the displayed state.
    const state = values.bandwidthToForm(override);
    baseline = clone(override);
    form.elements.source.value = state.mode;
    for (const direction of directions) {
      const rate = state.mode === 'inherit' ? null : state[direction + 'Mbps'];
      el(direction + '-kind').value = rate === null ? 'unlimited' : 'limited';
      el(direction + '-rate').value = rate === null ? '' : rate;
      clearError(direction);
    }
    invalidate(); sync();
    el('form-status').textContent = '示例配置已载入，尚未保存任何更改。';
  }

  form.addEventListener('input', invalidate);
  form.addEventListener('change', () => { invalidate(); sync(); });
  for (const direction of directions) {
    el(direction + '-rate').addEventListener('blur', () => validate(direction));
  }
  form.addEventListener('submit', event => {
    event.preventDefault();
    invalidate();
    const invalid = directions.filter(direction => !validate(direction));
    if (invalid.length) {
      el('form-errors').textContent = '有 ' + invalid.length + ' 个限速字段需要修正，请检查提示。';
      el('form-status').textContent = '';
      el(invalid[0] + '-rate').focus();
      return;
    }
    const state = inherited() ? {mode: 'inherit'} : {
      mode: 'override',
      ...Object.fromEntries(directions.map(direction => [direction + 'Mbps',
        el(direction + '-kind').value === 'unlimited' ? null : el(direction + '-rate').value])),
    };
    preview = values.bandwidthSaveDecision(state);
    const label = direction => preview.bandwidth[direction + '_bps'] === null
      ? '不限速' : values.bytesToMbps(preview.bandwidth[direction + '_bps']) + ' Mbps';
    el('preview-text').textContent = preview.inherit ? '继承上级设置。' : '上传：' + label('upload') + '；下载：' + label('download') + '。';
    el('preview').hidden = false;
    el('form-status').textContent = '预览已生成；未提交或保存。';
  });
  form.addEventListener('reset', event => { event.preventDefault(); load(baseline); });
  window.BandwidthForm = Object.freeze({load, getPreview: () => clone(preview)});
  load({upload_bps: 125000, download_bps: null});
})();
