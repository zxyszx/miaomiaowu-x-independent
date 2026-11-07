/* Pure form values. No requests, persistence, policy resolution, or UI mounting. */
(function (root, factory) {
  'use strict';
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.MiaomiaowuFieldValues = factory();
})(typeof globalThis === 'object' ? globalThis : this, function () {
  'use strict';
  const MAX_BPS = 1073741824;
  const KEYS = ['upload_bps', 'download_bps'];

  function bytesRate(value) {
    if (!Number.isInteger(value) || value < 1 || value > MAX_BPS) {
      throw new RangeError('限速必须为 1–1073741824 字节/秒的整数；0 不表示不限速或继承');
    }
    return value;
  }

  function mbpsToBytes(text) {
    if (typeof text !== 'string' || text.length > 64) {
      throw new TypeError('请输入 Mbps 十进制文本');
    }
    const value = text.trim();
    // Do not coerce blank, booleans, hex, exponents, locale commas, or units.
    if (!/^(?:\d+(?:\.\d*)?|\.\d+)$/.test(value)) {
      throw new TypeError('请输入正数 Mbps，不附单位');
    }
    const [whole = '', rawFraction = ''] = value.split('.');
    const fraction = rawFraction.replace(/0+$/, '');
    if (fraction.length > 6) throw new RangeError('Mbps 最多保留六位有效小数，且必须对应整数字节/秒');
    const scale = 10n ** BigInt(fraction.length);
    const numerator = (BigInt(whole || '0') * scale + BigInt(fraction || '0')) * 125000n;
    if (numerator % scale !== 0n) throw new RangeError('Mbps 必须对应整数字节/秒，不能自动舍入');
    const bytes = numerator / scale;
    if (bytes < 1n || bytes > BigInt(MAX_BPS)) throw new RangeError('限速超出 1–1073741824 字节/秒范围');
    return Number(bytes);
  }

  function bytesToMbps(value) {
    bytesRate(value);
    const micros = BigInt(value) * 8n;
    const whole = micros / 1000000n;
    const fraction = String(micros % 1000000n).padStart(6, '0').replace(/0+$/, '');
    return String(whole) + (fraction ? '.' + fraction : '');
  }

  function exactObject(value, keys) {
    if (!value || typeof value !== 'object' || Array.isArray(value) ||
        Object.keys(value).length !== keys.length ||
        !keys.every(key => Object.prototype.hasOwnProperty.call(value, key))) {
      throw new TypeError('字段结构无效');
    }
  }

  // API null here means the entire override record is absent. Directional
  // null inside an override object means unlimited, never inheritance.
  function bandwidthToForm(override) {
    if (override === null) return {mode: 'inherit'};
    exactObject(override, KEYS);
    return {
      mode: 'override',
      uploadMbps: override.upload_bps === null ? null : bytesToMbps(override.upload_bps),
      downloadMbps: override.download_bps === null ? null : bytesToMbps(override.download_bps),
    };
  }

  // This is a save decision, not an HTTP request body. Each page must map the
  // decision to its resource-specific endpoint only after normal authorization.
  function bandwidthSaveDecision(form) {
    if (form && form.mode === 'inherit') {
      exactObject(form, ['mode']);
      return {inherit: true};
    }
    exactObject(form, ['mode', 'uploadMbps', 'downloadMbps']);
    if (form.mode !== 'override') throw new TypeError('请选择继承或覆写');
    return {
      inherit: false,
      bandwidth: {
        upload_bps: form.uploadMbps === null ? null : mbpsToBytes(form.uploadMbps),
        download_bps: form.downloadMbps === null ? null : mbpsToBytes(form.downloadMbps),
      },
    };
  }

  return Object.freeze({MAX_BPS, mbpsToBytes, bytesToMbps, bandwidthToForm, bandwidthSaveDecision});
});
