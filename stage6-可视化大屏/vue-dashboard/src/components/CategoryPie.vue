<script setup>
import { computed } from 'vue'
import { useEChart } from '../composables/useEChart.js'
import { COLORS, tooltipStyle } from '../utils/chart.js'
import { pickStr, pickNum, fmtInt } from '../utils/format.js'

const props = defineProps({
  rows: { type: Array, default: () => [] }
})

function pickRatio(r) {
  for (const k of ['buy_share_ratio', 'share', 'ratio', 'percent', 'percentage', 'share_rate', 'proportion', 'cnt_ratio', 'gmv_ratio']) {
    const v = r[k]
    if (v !== undefined && v !== null && v !== '' && !isNaN(Number(v))) return Number(v)
  }
  return null
}

/**
 * 类目占比聚合：接口返回 3 万+ 全量类目，前端必须先聚合再渲染。
 * 策略：优先使用接口占比字段（自动识别 小数/百分数 口径）；
 * 无占比字段时回退用销量求和归一化；最终取 Top15，其余合并为“其他”。
 */
function buildPie(rows) {
  const norm = (rows || [])
    .map(r => ({
      name: pickStr(r, ['category_id', 'categoryId', 'cate_id', 'cat_id', 'id']),
      ratio: pickRatio(r),
      cnt: pickNum(r, ['buy_cnt', 'buyCnt', 'sale_cnt', 'sold_cnt', 'cnt', 'count', 'order_cnt'])
    }))
    .filter(x => x.name)
  if (!norm.length) return []

  const hasRatio = norm.some(x => x.ratio !== null)
  let items
  if (hasRatio) {
    const sumRatio = norm.reduce((s, x) => s + x.ratio, 0)
    const scale = sumRatio > 0 && sumRatio < 3 ? 100 : 1 // 总和≈1 => 小数口径，换算为百分数
    items = norm.map(x => ({ name: x.name, value: x.ratio * scale }))
  } else {
    const total = norm.reduce((s, x) => s + x.cnt, 0)
    if (total <= 0) return []
    items = norm.map(x => ({ name: x.name, value: (x.cnt / total) * 100 }))
  }

  items.sort((a, b) => b.value - a.value)
  const top = items.slice(0, 15)
  const rest = items.slice(15)
  const restVal = rest.reduce((s, x) => s + x.value, 0)
  if (restVal > 0) top.push({ name: '其他(' + rest.length + '类)', value: restVal })
  return top
}

const pieData = computed(() => buildPie(props.rows))

function option() {
  const data = pieData.value
  const others = data.find(x => x.name.startsWith('其他'))
  const legendNames = others ? [...data.slice(0, 15).map(x => x.name), others.name] : data.map(x => x.name)
  return {
    backgroundColor: 'transparent',
    tooltip: {
      ...tooltipStyle,
      trigger: 'item',
      formatter: p => '类目 ' + p.name + '<br/>占比 ' + Number(p.value).toFixed(3) + '%'
    },
    legend: {
      type: 'scroll',
      bottom: 0,
      left: 'center',
      itemWidth: 10,
      itemHeight: 7,
      pageIconColor: COLORS.cyan,
      pageTextStyle: { color: COLORS.text },
      textStyle: { color: COLORS.text, fontSize: 10 },
      data: legendNames
    },
    color: [
      '#00D4FF', '#7C3AED', '#34D399', '#F59E0B', '#EC4899',
      '#3B82F6', '#22D3EE', '#A78BFA', '#FBBF24', '#F472B6',
      '#60A5FA', '#2DD4BF', '#C084FC', '#FB923C', '#E879F9',
      '#475569'
    ],
    graphic: [{
      type: 'text',
      left: 'center',
      top: 'middle',
      style: {
        text: fmtInt(props.rows.length) + '\n购买类目',
        fill: COLORS.title,
        fontSize: 12,
        fontWeight: 600,
        lineHeight: 17,
        align: 'center',
        verticalAlign: 'middle',
        fontFamily: 'Consolas, Microsoft YaHei, sans-serif'
      }
    }],
    series: [{
      type: 'pie',
      radius: ['36%', '56%'],
      center: ['50%', '50%'],
      minAngle: 2,
      avoidLabelOverlap: true,
      itemStyle: { borderColor: '#0B1020', borderWidth: 1 },
      label: { show: false },
      labelLine: { show: false },
      emphasis: {
        label: {
          show: true,
          color: COLORS.title,
          fontSize: 10,
          // 深色底托保证标签压在任意扇区上都清晰可读
          backgroundColor: 'rgba(11, 16, 32, 0.92)',
          borderColor: 'rgba(0, 212, 255, 0.4)',
          borderWidth: 1,
          borderRadius: 4,
          padding: [4, 8],
          formatter: p => p.name + '\n' + Number(p.value).toFixed(2) + '%'
        }
      },
      data
    },
    {
      // 外圈装饰细环（官方大屏常见手法）
      type: 'pie',
      radius: ['60.5%', '62%'],
      center: ['50%', '50%'],
      silent: true,
      label: { show: false },
      labelLine: { show: false },
      data: [1],
      itemStyle: { color: 'rgba(0, 212, 255, 0.14)' }
    }]
  }
}

const { el } = useEChart(option, [pieData])
</script>

<template>
  <div class="panel">
    <div class="panel-title">
      类目销售占比
      <span class="panel-sub">Top15 + 其他聚合（共 {{ (rows || []).length.toLocaleString() }} 类）</span>
    </div>
    <div ref="el" class="chart-box"></div>
  </div>
</template>
