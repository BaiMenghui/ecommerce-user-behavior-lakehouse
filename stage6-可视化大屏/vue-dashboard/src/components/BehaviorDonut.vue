<script setup>
import { computed } from 'vue'
import { useEChart } from '../composables/useEChart.js'
import { COLORS, tooltipStyle, BEHAVIOR_META } from '../utils/chart.js'
import { pickStr, pickNum, fmtInt } from '../utils/format.js'

const props = defineProps({
  rows: { type: Array, default: () => [] }
})

// 与 HourFlow 同源：hour-heatmap 数据按行为类型求和 -> 行为分布环形图
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

function normalizeTotals(rows) {
  const arr = Array.isArray(rows) ? rows : []
  if (!arr.length) return { total: 0, items: [] }
  const first = arr[0] || {}

  const isWide = ['pv', 'fav', 'cart', 'buy']
    .some(t => BEHAVIOR_KEYS[t].some(k => first[k] !== undefined))

  let sums = { pv: 0, fav: 0, cart: 0, buy: 0 }
  if (isWide) {
    for (const r of arr) {
      for (const t of ['pv', 'fav', 'cart', 'buy']) sums[t] += pickNum(r, BEHAVIOR_KEYS[t])
    }
  } else {
    const actionKey = ['action', 'action_type', 'actionType', 'behavior', 'behavior_type', 'behaviorType', 'type', 'event']
      .find(k => first[k] !== undefined)
    const cntKey = ['cnt', 'count', 'num', 'value', 'amount', 'total']
      .find(k => first[k] !== undefined)
    if (actionKey && cntKey) {
      for (const r of arr) {
        const a = ALIAS[String(r[actionKey]).toLowerCase()]
        if (a) sums[a] += Number(r[cntKey]) || 0
      }
    }
  }

  const items = ['pv', 'fav', 'cart', 'buy']
    .map(t => ({
      name: BEHAVIOR_META[t].name,
      value: sums[t],
      itemStyle: { color: BEHAVIOR_META[t].color }
    }))
    .filter(x => x.value > 0)
  const total = items.reduce((s, x) => s + x.value, 0)
  return { total, items }
}

const data = computed(() => normalizeTotals(props.rows))

function option() {
  const d = data.value
  return {
    backgroundColor: 'transparent',
    tooltip: {
      ...tooltipStyle,
      trigger: 'item',
      formatter: p => p.name + '：' + fmtInt(p.value) +
        '（' + (d.total > 0 ? ((p.value / d.total) * 100).toFixed(2) : '0.00') + '%）'
    },
    legend: {
      bottom: 0, left: 'center', itemWidth: 10, itemHeight: 7,
      icon: 'circle',
      textStyle: { color: COLORS.text, fontSize: 11 }
    },
    color: ['#00D4FF', '#7C3AED', '#F59E0B', '#EC4899'],
    series: [{
      type: 'pie',
      radius: ['48%', '70%'],
      center: ['50%', '46%'],
      minAngle: 3,
      itemStyle: { borderColor: '#0B1020', borderWidth: 2 },
      label: { show: false },
      labelLine: { show: false },
      emphasis: {
        scale: false,
        label: {
          // hover 标签显示在环中心空洞，不会被扇区遮挡
          show: true,
          position: 'center',
          color: COLORS.title,
          fontSize: 13,
          fontWeight: 600,
          formatter: p => p.name + '\n' +
            (d.total > 0 ? ((p.value / d.total) * 100).toFixed(2) : '0.00') + '%'
        }
      },
      data: d.items
    }]
  }
}

const { el } = useEChart(option, [data])
</script>

<template>
  <div class="panel">
    <div class="panel-title">
      用户行为分布
      <span class="panel-sub">浏览 / 收藏 / 加购 / 购买</span>
    </div>
    <div ref="el" class="chart-box"></div>
  </div>
</template>
