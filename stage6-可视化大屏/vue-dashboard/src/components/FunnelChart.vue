<script setup>
import { computed } from 'vue'
import { useEChart } from '../composables/useEChart.js'
import { COLORS, tooltipStyle } from '../utils/chart.js'
import { pick, fmtInt, fmtRatio } from '../utils/format.js'

const props = defineProps({
  funnel: { type: Object, default: null }
})

// 字段兼容映射（阶段5联调字段名可能略有差异）
const data = computed(() => {
  const f = props.funnel || {}
  const pv = Number(pick(f, ['pv_cnt', 'pvCount', 'pv_count', 'pv'], 0)) || 0
  const fav = Number(pick(f, ['fav_cnt', 'favCount', 'fav_count', 'fav'], 0)) || 0
  const cart = Number(pick(f, ['cart_cnt', 'cartCount', 'cart_count', 'cart'], 0)) || 0
  const buy = Number(pick(f, ['buy_cnt', 'buyCount', 'buy_count', 'buy'], 0)) || 0
  // 三段转化率：优先接口字段，缺失则前端计算
  const r1 = pick(f, ['pv2cart_rate', 'pv2cartRate', 'pv_to_cart_rate'], null)
  const r2 = pick(f, ['cart2buy_rate', 'cart2buyRate', 'cart_to_buy_rate'], null)
  const r3 = pick(f, ['pv2buy_rate', 'pv2buyRate', 'pv_to_buy_rate'], null)
  const calc = (a, b) => (a > 0 ? b / a : 0)
  return {
    pv, fav, cart, buy,
    pv2cart: r1 !== null ? Number(r1) : calc(pv, cart),
    cart2buy: r2 !== null ? Number(r2) : calc(cart, buy),
    pv2buy: r3 !== null ? Number(r3) : calc(pv, buy)
  }
})

function option() {
  const d = data.value
  return {
    backgroundColor: 'transparent',
    tooltip: {
      ...tooltipStyle,
      trigger: 'item',
      formatter: p => p.name + '：' + fmtInt(p.value) +
        '（相对浏览 ' + (d.pv > 0 ? ((p.value / d.pv) * 100).toFixed(2) : '0.00') + '%）'
    },
    series: [{
      type: 'funnel',
      left: '10%', right: '10%', top: 8, bottom: 4,
      minSize: '30%',
      sort: 'descending',
      gap: 3,
      label: {
        show: true,
        position: 'inside',
        color: '#0B1020',
        fontSize: 10,
        fontWeight: 600,
        formatter: p => p.name + '  ' + fmtInt(p.value)
      },
      itemStyle: { borderColor: 'rgba(148, 197, 255, 0.18)', borderWidth: 1 },
      emphasis: { label: { fontSize: 12 } },
      data: [
        { name: '浏览 pv', value: d.pv, itemStyle: { color: '#00D4FF' } },
        { name: '收藏 fav', value: d.fav, itemStyle: { color: '#38BDF8' } },
        { name: '加购 cart', value: d.cart, itemStyle: { color: '#7C3AED' } },
        { name: '购买 buy', value: d.buy, itemStyle: { color: '#EC4899' } }
      ]
    }]
  }
}

const { el } = useEChart(option, [data])
</script>

<template>
  <div class="panel">
    <div class="panel-title">
      转化漏斗
      <span class="panel-sub">
        浏览→加购
        <span class="rate-tag">{{ fmtRatio(data.pv2cart) }}</span>
        加购→购买
        <span class="rate-tag">{{ fmtRatio(data.cart2buy) }}</span>
        浏览→购买
        <span class="rate-tag">{{ fmtRatio(data.pv2buy) }}</span>
      </span>
    </div>
    <div ref="el" class="chart-box"></div>
  </div>
</template>

<style scoped>
.rate-tag {
  display: inline-block;
  padding: 0 6px;
  margin: 0 2px 0 1px;
  border: 1px solid rgba(0, 212, 255, 0.35);
  border-radius: 9px;
  background: rgba(0, 212, 255, 0.08);
  color: var(--cyan);
  font-size: 11px;
  font-family: Consolas, monospace;
}
</style>
