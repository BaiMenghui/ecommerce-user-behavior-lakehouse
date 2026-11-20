<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue'
import HeaderBar from './components/HeaderBar.vue'
import KpiCards from './components/KpiCards.vue'
import TrendChart from './components/TrendChart.vue'
import FunnelChart from './components/FunnelChart.vue'
import TopItems from './components/TopItems.vue'
import CategoryPie from './components/CategoryPie.vue'
import HourFlow from './components/HourFlow.vue'
import BehaviorDonut from './components/BehaviorDonut.vue'
import {
  getHealth, getDates, getKpiSummary, getDauTrend,
  getFunnel, getTopN, getCategoryShare, getHourHeatmap
} from './api.js'

const health = ref(null)
const dates = ref([])
const dt = ref('')
const kpi = ref(null)
const trend = ref([])
const funnel = ref(null)
const topn = ref([])
const categoryShare = ref([])
const hourData = ref([])
const loading = ref(false)
const errMsg = ref('')

// 健康状态轮询（30s），驱动全局状态灯
async function refreshHealth() {
  try {
    health.value = await getHealth()
    errMsg.value = ''
  } catch (e) {
    health.value = { status: 'DOWN' }
  }
}

// 按日期拉取当日指标；趋势接口与 dt 无关，一并刷新
async function loadByDt(date) {
  if (!date) return
  loading.value = true
  try {
    const [k, t, f, tn, cs, hh] = await Promise.all([
      getKpiSummary(date),
      getDauTrend(),
      getFunnel(date),
      getTopN(date),
      getCategoryShare(date),
      getHourHeatmap(date)
    ])
    kpi.value = k
    trend.value = Array.isArray(t) ? t : []
    funnel.value = f
    topn.value = Array.isArray(tn) ? tn : []
    categoryShare.value = Array.isArray(cs) ? cs : []
    hourData.value = Array.isArray(hh) ? hh : []
    errMsg.value = ''
  } catch (e) {
    errMsg.value = (e && e.message) || String(e)
  } finally {
    loading.value = false
  }
}

function onDtChange(date) {
  dt.value = date
  loadByDt(date)
}

let healthTimer = null
onMounted(async () => {
  await refreshHealth()
  try {
    dates.value = (await getDates()) || []
  } catch (e) {
    errMsg.value = (e && e.message) || String(e)
  }
  // 当前 dt 默认取日期数组末位
  dt.value = dates.value[dates.value.length - 1] || ''
  await loadByDt(dt.value)
  healthTimer = setInterval(refreshHealth, 30000)
})

onBeforeUnmount(() => {
  if (healthTimer) clearInterval(healthTimer)
})
</script>

<template>
  <div class="dashboard">
    <HeaderBar
      class="area-header"
      :health="health"
      :dates="dates"
      :dt="dt"
      :loading="loading"
      @change-dt="onDtChange"
    />
    <KpiCards class="area-kpi" :kpi="kpi" :dt="dt" />
    <TrendChart class="area-trend" :rows="trend" />
    <FunnelChart class="area-funnel" :funnel="funnel" />
    <TopItems class="area-topn" :rows="topn" />
    <CategoryPie class="area-pie" :rows="categoryShare" />
    <HourFlow class="area-hour" :rows="hourData" />
    <BehaviorDonut class="area-behavior" :rows="hourData" />
    <transition name="fade">
      <div v-if="errMsg" class="error-bar">数据加载失败：{{ errMsg }}</div>
    </transition>
  </div>
</template>

<style scoped>
/* ===== 4+2+1 大屏布局：头部通栏 / 中部三列（KPI竖排 + 2图 + 2图）/ 底部两列 ===== */
.dashboard {
  position: relative;
  height: 100%;
  display: grid;
  gap: 10px;
  padding: 10px;
  grid-template-columns: 290px minmax(0, 1.1fr) minmax(0, 1fr);
  grid-template-rows: 62px minmax(0, 1.05fr) minmax(0, 1.1fr) 235px;
  grid-template-areas:
    'header header   header'
    'kpi    trend    topn'
    'kpi    funnel   pie'
    'hour   hour     behavior';
}

.area-header { grid-area: header; }
.area-kpi { grid-area: kpi; }
.area-trend { grid-area: trend; }
.area-funnel { grid-area: funnel; }
.area-topn { grid-area: topn; }
.area-pie { grid-area: pie; }
.area-hour { grid-area: hour; }
.area-behavior { grid-area: behavior; }

.error-bar {
  position: absolute;
  left: 50%;
  bottom: 8px;
  transform: translateX(-50%);
  background: rgba(239, 68, 68, 0.15);
  border: 1px solid rgba(239, 68, 68, 0.5);
  color: #FCA5A5;
  padding: 4px 14px;
  border-radius: 6px;
  font-size: 12px;
  z-index: 10;
}

.fade-enter-active, .fade-leave-active { transition: opacity 0.3s; }
.fade-enter-from, .fade-leave-to { opacity: 0; }
</style>
