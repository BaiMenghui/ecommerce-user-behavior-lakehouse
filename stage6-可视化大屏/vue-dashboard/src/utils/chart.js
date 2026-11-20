import * as echarts from 'echarts'

// 大屏统一配色
export const COLORS = {
  bg: '#0B1020',
  card: '#111827',
  border: '#1E293B',
  cyan: '#00D4FF',
  purple: '#7C3AED',
  green: '#34D399',
  orange: '#F59E0B',
  pink: '#EC4899',
  blue: '#3B82F6',
  title: '#E2E8F0',
  text: '#93A4C3',
  split: '#1E293B'
}

// 行为类型 -> 中文名 + 颜色（兼容常见字段命名）
export const BEHAVIOR_META = {
  pv:    { name: '浏览',  color: COLORS.cyan },
  fav:   { name: '收藏',  color: COLORS.purple },
  cart:  { name: '加购',  color: COLORS.orange },
  buy:   { name: '购买',  color: COLORS.pink }
}

const registered = []

// 统一初始化：所有实例共用 window.resize 自适应
export function createChart(el) {
  const chart = echarts.init(el)
  const onResize = () => chart.resize()
  window.addEventListener('resize', onResize)
  registered.push({ chart, onResize })
  return chart
}

export function disposeChart(chart) {
  const i = registered.findIndex(r => r.chart === chart)
  if (i >= 0) {
    window.removeEventListener('resize', registered[i].onResize)
    registered.splice(i, 1)
  }
  chart.dispose()
}

// 公共 tooltip / 轴样式
export const tooltipStyle = {
  backgroundColor: 'rgba(13, 20, 40, 0.94)',
  borderColor: 'rgba(0, 212, 255, 0.35)',
  borderWidth: 1,
  padding: [8, 12],
  textStyle: { color: COLORS.title, fontSize: 12 },
  extraCssText: 'box-shadow: 0 8px 24px rgba(0, 0, 0, 0.45); border-radius: 8px;'
}

export function axisStyle() {
  return {
    axisLine: { lineStyle: { color: COLORS.split } },
    axisTick: { show: false },
    axisLabel: { color: COLORS.text, fontSize: 11 },
    splitLine: { lineStyle: { color: COLORS.split, type: 'dashed' } }
  }
}

// 纵向渐变（柱状）
export function vGrad(top, bottom) {
  return new echarts.graphic.LinearGradient(0, 0, 0, 1, [
    { offset: 0, color: top },
    { offset: 1, color: bottom }
  ])
}

// 横向渐变（条形）
export function hGrad(left, right) {
  return new echarts.graphic.LinearGradient(0, 0, 1, 0, [
    { offset: 0, color: left },
    { offset: 1, color: right }
  ])
}

// 数值取值：兼容常见键名
export function pickNum(obj, keys) {
  if (!obj) return 0
  for (const k of keys) {
    const v = obj[k]
    if (v !== undefined && v !== null && v !== '' && !isNaN(Number(v))) return Number(v)
  }
  return 0
}

export function pickStr(obj, keys) {
  if (!obj) return ''
  for (const k of keys) {
    if (obj[k] !== undefined && obj[k] !== null && obj[k] !== '') return String(obj[k])
  }
  return ''
}
