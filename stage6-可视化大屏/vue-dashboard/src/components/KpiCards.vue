<script setup>
import { computed, ref, watch, onBeforeUnmount } from 'vue'
import { fmtInt, fmtNum, ratioToNum } from '../utils/format.js'

const props = defineProps({
  kpi: { type: Object, default: null },
  dt: { type: String, default: '' }
})

// ===== 数字滚动动画（easeOutCubic，600ms）=====
const progress = ref(1)
let rafId = null
function animate() {
  if (rafId) cancelAnimationFrame(rafId)
  const start = performance.now()
  const DURATION = 600
  const step = t => {
    const p = Math.min(1, (t - start) / DURATION)
    progress.value = 1 - Math.pow(1 - p, 3)
    if (p < 1) rafId = requestAnimationFrame(step)
  }
  progress.value = 0
  rafId = requestAnimationFrame(step)
}
watch(() => props.kpi, animate, { immediate: true })
onBeforeUnmount(() => rafId && cancelAnimationFrame(rafId))

// 6 张 KPI 卡：DAU / 新增 / 老客 / 订单量 / GMV / 客单价，副标注新老占比等
const cards = computed(() => {
  const k = props.kpi || {}
  const P = progress.value
  const dau = (Number(k.dau) || 0) * P
  const newUser = (Number(k.new_user_cnt) || 0) * P
  const oldUser = (Number(k.old_user_cnt) || 0) * P
  const orderCnt = (Number(k.order_cnt) || 0) * P
  const gmv = (Number(k.gmv) || 0) * P
  const avgTicket = (Number(k.avg_ticket) || 0) * P

  const newRatio = ratioToNum(k.new_user_ratio)
  const oldRatio = newRatio > 0 ? 100 - newRatio : ((Number(k.dau) || 0) > 0 ? ((Number(k.old_user_cnt) || 0) / Number(k.dau)) * 100 : 0)
  const perUserOrder = (Number(k.dau) || 0) > 0 ? (Number(k.order_cnt) || 0) / Number(k.dau) : 0

  return [
    { label: 'DAU 活跃用户', value: fmtInt(Math.round(dau)), sub: props.dt ? ('数据日期 ' + props.dt) : '当日活跃', color: '#00D4FF' },
    { label: '新增用户', value: fmtInt(Math.round(newUser)), sub: '新客占比 ' + (newRatio ? newRatio.toFixed(2) + '%' : '--'), color: '#34D399' },
    { label: '老客用户', value: fmtInt(Math.round(oldUser)), sub: '老客占比 ' + (oldRatio ? oldRatio.toFixed(2) + '%' : '--'), color: '#3B82F6' },
    { label: '订单量', value: fmtInt(Math.round(orderCnt)), sub: '人均 ' + perUserOrder.toFixed(2) + ' 单', color: '#F59E0B' },
    { label: 'GMV 销售额', value: '¥ ' + fmtNum(Math.round(gmv), 0), sub: '当日成交总额', color: '#EC4899' },
    { label: '客单价', value: '¥ ' + fmtNum(avgTicket, 2), sub: 'GMV / 订单量', color: '#7C3AED' }
  ]
})
</script>

<template>
  <div class="kpi-col">
    <div v-for="c in cards" :key="c.label" class="kpi-card" :style="{ '--kc': c.color }">
      <div class="kpi-label">{{ c.label }}</div>
      <div class="kpi-value" :style="{ color: c.color, textShadow: '0 0 14px ' + c.color + '77' }">{{ c.value }}</div>
      <div class="kpi-sub">{{ c.sub }}</div>
    </div>
  </div>
</template>

<style scoped>
.kpi-col {
  height: 100%;
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-height: 0;
}

.kpi-card {
  flex: 1;
  min-height: 0;
  background:
    linear-gradient(120deg, color-mix(in srgb, var(--kc) 9%, transparent), transparent 55%),
    var(--card);
  border: 1px solid var(--card-border);
  border-radius: 10px;
  padding: 8px 14px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 2px;
  position: relative;
  overflow: hidden;
}

/* 左侧专属彩色光条 */
.kpi-card::before {
  content: '';
  position: absolute;
  left: 0;
  top: 16%;
  bottom: 16%;
  width: 3px;
  border-radius: 2px;
  background: linear-gradient(180deg, var(--kc), transparent);
  box-shadow: 0 0 8px var(--kc);
}

/* 右上角同色光晕 */
.kpi-card::after {
  content: '';
  position: absolute;
  right: -18px;
  top: -18px;
  width: 56px;
  height: 56px;
  border-radius: 50%;
  background: radial-gradient(circle, var(--kc), transparent 70%);
  opacity: 0.12;
}

.kpi-label {
  font-size: 12px;
  color: var(--text-sub);
  letter-spacing: 1px;
}

.kpi-value {
  font-family: Consolas, 'Courier New', monospace;
  font-size: 24px;
  font-weight: 700;
  line-height: 1.15;
  white-space: nowrap;
}

.kpi-sub {
  font-size: 11px;
  color: var(--text-sub);
}
</style>
