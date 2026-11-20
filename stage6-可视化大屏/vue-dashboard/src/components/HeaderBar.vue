<script setup>
import { ref, computed, onMounted, onBeforeUnmount } from 'vue'

const props = defineProps({
  health: { type: Object, default: null },
  dates: { type: Array, default: () => [] },
  dt: { type: String, default: '' },
  loading: { type: Boolean, default: false }
})

const emit = defineEmits(['change-dt'])

// 实时时钟：每秒刷新；强制按北京时间（Asia/Shanghai）渲染，
// 避免浏览器运行环境时区不是东八区时（如 UTC 云电脑）时间偏差 8 小时
const weekMap = { Sun: '日', Mon: '一', Tue: '二', Wed: '三', Thu: '四', Fri: '五', Sat: '六' }
const bjFmt = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'Asia/Shanghai',
  hourCycle: 'h23',
  year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', second: '2-digit', weekday: 'short'
})
const now = ref('')
let timer = null
function tick() {
  const parts = {}
  for (const p of bjFmt.formatToParts(new Date())) parts[p.type] = p.value
  const week = weekMap[parts.weekday] || ''
  now.value = parts.year + '-' + parts.month + '-' + parts.day +
    ' ' + parts.hour + ':' + parts.minute + ':' + parts.second +
    ' 星期' + week
}
onMounted(() => {
  tick()
  timer = setInterval(tick, 1000)
})
onBeforeUnmount(() => timer && clearInterval(timer))

const up = computed(() => !!(props.health && props.health.status === 'UP'))
</script>

<template>
  <div class="header">
    <div class="left">
      <div class="logo">◈</div>
      <div class="title-wrap">
        <div class="title">电商用户行为湖仓分析平台</div>
        <div class="subtitle">USER BEHAVIOR LAKEHOUSE ANALYSIS · DATA DASHBOARD</div>
      </div>
    </div>

    <div class="center">
      <span class="clock">{{ now }}</span>
    </div>

    <div class="right">
      <span v-if="loading" class="badge loading-badge">加载中…</span>
      <span v-if="health && health.cacheType" class="badge cache-badge">缓存 · {{ health.cacheType }}</span>
      <span class="status" :class="up ? 'ok' : 'bad'">
        <span class="dot"></span>
        {{ up ? '服务正常' : '服务异常' }}
      </span>
      <select
        class="dt-select"
        :value="dt"
        title="切换数据日期"
        @change="e => emit('change-dt', e.target.value)"
      >
        <option v-for="d in dates" :key="d" :value="d">{{ d }}</option>
      </select>
    </div>
  </div>
</template>

<style scoped>
.header {
  position: relative;
  height: 100%;
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 0 16px;
  background: var(--card);
  border: 1px solid var(--card-border);
  border-radius: 10px;
}

/* 底部扫光分隔带 */
.header::after {
  content: '';
  position: absolute;
  left: 14px;
  right: 14px;
  bottom: 0;
  height: 2px;
  border-radius: 2px;
  background: linear-gradient(90deg, transparent, rgba(0, 212, 255, 0.55), rgba(124, 58, 237, 0.55), transparent);
  filter: blur(0.4px);
}

.left { display: flex; align-items: center; gap: 10px; }

.logo {
  font-size: 24px;
  color: var(--cyan);
  text-shadow: 0 0 14px rgba(0, 212, 255, 0.9);
}

.title {
  font-size: 20px;
  font-weight: 700;
  letter-spacing: 2px;
  color: #F1F5F9;
  text-shadow: 0 0 12px rgba(0, 212, 255, 0.55);
}

.subtitle {
  margin-top: 2px;
  font-size: 10px;
  letter-spacing: 2px;
  color: var(--text-sub);
}

.center { flex: 1; text-align: center; }

.clock {
  font-family: Consolas, 'Courier New', monospace;
  font-size: 16px;
  color: var(--cyan);
  text-shadow: 0 0 10px rgba(0, 212, 255, 0.6);
  letter-spacing: 1px;
}

.right { display: flex; align-items: center; gap: 10px; }

.badge {
  font-size: 11px;
  padding: 2px 8px;
  border-radius: 9px;
  border: 1px solid rgba(124, 58, 237, 0.45);
  background: rgba(124, 58, 237, 0.12);
  color: #C4B5FD;
}

.loading-badge {
  border-color: rgba(245, 158, 11, 0.45);
  background: rgba(245, 158, 11, 0.12);
  color: #FCD34D;
}

.status { display: flex; align-items: center; gap: 6px; font-size: 12px; }
.status .dot { width: 9px; height: 9px; border-radius: 50%; }
.status.ok { color: #34D399; }
.status.ok .dot { background: #34D399; box-shadow: 0 0 8px #34D399; animation: pulse 2s infinite; }
.status.bad { color: #F87171; }
.status.bad .dot { background: #F87171; box-shadow: 0 0 8px #F87171; }

@keyframes pulse {
  0% { opacity: 1; }
  50% { opacity: 0.45; }
  100% { opacity: 1; }
}

.dt-select {
  background: #0B1020;
  border: 1px solid var(--card-border);
  color: var(--cyan);
  border-radius: 6px;
  padding: 4px 8px;
  font-size: 12px;
  outline: none;
  cursor: pointer;
}
.dt-select:focus { border-color: var(--cyan); }
</style>
