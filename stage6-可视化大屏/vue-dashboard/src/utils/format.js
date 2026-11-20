// 数值格式化工具

// 千分位整数
export function fmtInt(v) {
  const n = Number(v)
  if (isNaN(n)) return '0'
  return Math.round(n).toLocaleString('en-US')
}

// 千分位小数
export function fmtNum(v, digits = 2) {
  const n = Number(v)
  if (isNaN(n)) return '0'
  return n.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

// 比率 -> 百分比：兼容 0.32 与 32.5 两种口径
export function fmtRatio(v, digits = 2) {
  const n = Number(v)
  if (isNaN(n) || v === null || v === undefined) return '--'
  const pct = n <= 1 ? n * 100 : n
  return pct.toFixed(digits) + '%'
}

export function ratioToNum(v) {
  const n = Number(v)
  if (isNaN(n) || v === null || v === undefined) return 0
  return n <= 1 ? n * 100 : n
}

// 日期 2017-11-25 -> 11-25
export function shortDate(dt) {
  if (!dt) return ''
  return dt.length >= 10 ? dt.slice(5) : dt
}

// 字段兼容取值
export function pick(obj, keys, dft) {
  if (!obj) return dft
  for (const k of keys) {
    const v = obj[k]
    if (v !== undefined && v !== null && v !== '') return v
  }
  return dft
}

// 数值字段兼容取值：跳过无法转数字的键，默认 0（与 chart.js 同语义）
export function pickNum(obj, keys) {
  if (!obj) return 0
  for (const k of keys) {
    const v = obj[k]
    if (v !== undefined && v !== null && v !== '' && !isNaN(Number(v))) return Number(v)
  }
  return 0
}

// 字符串字段兼容取值：默认 ''
export function pickStr(obj, keys) {
  if (!obj) return ''
  for (const k of keys) {
    if (obj[k] !== undefined && obj[k] !== null && obj[k] !== '') return String(obj[k])
  }
  return ''
}
