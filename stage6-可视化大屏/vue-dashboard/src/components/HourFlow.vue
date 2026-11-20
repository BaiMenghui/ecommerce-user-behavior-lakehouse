<script setup>
import { computed } from 'vue'
import { useEChart } from '../composables/useEChart.js'
import { COLORS, tooltipStyle, axisStyle, BEHAVIOR_META } from '../utils/chart.js'
import { pickStr, pickNum, fmtInt } from '../utils/format.js'

const props = defineProps({
  rows: { type: Array, default: () => [] }
})

const BEHAVIOR_KEYS = {
  pv: ['pv', 'pv_cnt', 'pvCount', 'pv_count', 'view_cnt'],
  fav: ['fav', 'fav_cnt', 'favCount', 'fav_count', 'favorite_cnt'],
  cart: ['cart', 'cart_cnt', 'cartCount', 'cart_count'],
  buy: ['buy', 'buy_cnt', 'buyCount', 'buy_count', 'pay_cnt']
}

const ALIAS = {
  pv: 'pv', click: 'pv', view: 'pv', browse: 'pv',
  fav: 'fav', favorite: 'fav', collect: 'fav',
  cart: 'cart', addcart: 'cart', add_cart: 'cart',
  buy: 'buy', order: 'buy', pay: 'buy', purchase: 'buy', trade: 'buy'
}

/**
 * 24 小时行为量归一化，兼容两种结构：
 * 宽表 {hour, pv, fav, cart, buy}(含 _cnt 变体) / 长表 {hour, action, cnt}
 */
function normalizeHour(rows) {
  const arr = Array.isArray(rows) ? rows : []
  const empty = { hours: [], seriesMap: { pv: [], fav: [], cart: [], buy: [] } }
  if (!arr.length) return empty
  const first = arr[0] || {}

  const hourOf = r => {
    const raw = pickStr(r, ['hour', 'hour_of_day', 'dt_hour', 'dtHour', 'h', 'time'])
    if (raw) {
      const m = raw.match(/\d{1,2}/)
      if (m) return String(Number(m[0])).padStart(2, '0')
    }
    return String(Math.floor(pickNum(r, ['hour', 'hour_of_day', 'dt_hour', 'dtHour', 'h']))).padStart(2, '0')
  }

  const isWide = ['pv', 'fav', 'cart', 'buy']
    .some(t => BEHAVIOR_KEYS[t].some(k => first[k] !== undefined))

  if (isWide) {
    return {
      hours: arr.map(hourOf),
      seriesMap: Object.fromEntries(
        ['pv', 'fav', 'cart', 'buy'].map(t => [t, arr.map(r => pickNum(r, BEHAVIOR_KEYS[t]))])
      )
    }
  }

  // 长表聚合
  const actionKey = ['action', 'action_type', 'actionType', 'behavior', 'behavior_type', 'behaviorType', 'type', 'event']
    .find(k => first[k] !== undefined)
  const cntKey = ['cnt', 'count', 'num', 'value', 'amount', 'total']
    .find(k => first[k] !== undefined)
  if (!actionKey || !cntKey) return empty

  const hours = [...new Set(arr.map(hourOf))].sort()
  const cell = {}
  arr.forEach(r => {
    const a = ALIAS[String(r[actionKey]).toLowerCase()]
    if (a) {
      const key = hourOf(r) + '#' + a
      cell[key] = (cell[key] || 0) + (Number(r[cntKey]) || 0)
    }
  })
  return {
    hours,
    seriesMap: Object.fromEntries(
      ['pv', 'fav', 'cart', 'buy'].map(t => [t, hours.map(h => cell[h + '#' + t] || 0)])
    )
  }
}

const norm = computed(() => normalizeHour(props.rows))

// 24 小时按行为类型分色的堆叠柱状
function option() {
  const { hours, seriesMap } = norm.value
  return {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      ...tooltipStyle,
      valueFormatter: v => fmtInt(v)
    },
    legend: {
      top: 0, right: 4, itemWidth: 12, itemHeight: 8,
      textStyle: { color: COLORS.text, fontSize: 11 }
    },
    grid: { left: 6, right: 10, top: 28, bottom: 0, containLabel: true },
    xAxis: {
      type: 'category',
      data: hours,
      ...axisStyle(),
      splitLine: { show: false },
      axisLabel: { color: COLORS.text, fontSize: 10, interval: 1 }
    },
    yAxis: {
      type: 'value',
      ...axisStyle(),
      axisLabel: { color: COLORS.text, fontSize: 10, formatter: v => fmtInt(v) }
    },
    series: ['pv', 'fav', 'cart', 'buy']
      .filter(t => seriesMap[t] && seriesMap[t].some(v => v > 0)) // 无数据的行为类型不渲染（如当日无收藏维度）
      .map(t => ({
      name: BEHAVIOR_META[t].name,
      type: 'bar',
      stack: 'total',
      barMaxWidth: 18,
      data: seriesMap[t],
      itemStyle: { color: BEHAVIOR_META[t].color },
      emphasis: { focus: 'series' }
    }))
  }
}

const { el } = useEChart(option, [norm])
</script>

<template>
  <div class="panel">
    <div class="panel-title">
      24小时行为流量
      <span class="panel-sub">按行为类型堆叠</span>
    </div>
    <div ref="el" class="chart-box"></div>
  </div>
</template>
