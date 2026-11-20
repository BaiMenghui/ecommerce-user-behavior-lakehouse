<script setup>
import { computed } from 'vue'
import { useEChart } from '../composables/useEChart.js'
import { COLORS, tooltipStyle, axisStyle, hGrad } from '../utils/chart.js'
import { pick, pickStr, pickNum, fmtInt } from '../utils/format.js'

const props = defineProps({
  rows: { type: Array, default: () => [] }
})

// Top10 横向条形（按销量降序），字段兼容 item_id/category_id/buy_cnt
const list = computed(() => {
  const items = (props.rows || [])
    .map(r => ({
      name: pickStr(r, ['item_id', 'itemId', 'item', 'id']),
      cat: pickStr(r, ['category_id', 'categoryId', 'cate_id', 'cat_id']),
      cnt: pickNum(r, ['buy_cnt', 'buyCnt', 'sale_cnt', 'sold_cnt', 'cnt', 'count'])
    }))
    .filter(x => x.name)
    .sort((a, b) => b.cnt - a.cnt)
    .slice(0, 10)
  // echarts 横向类目轴自下而上绘制，反转使第 1 名显示在最上方
  return [...items].reverse()
})

function option() {
  const data = list.value
  return {
    backgroundColor: 'transparent',
    tooltip: {
      ...tooltipStyle,
      trigger: 'item',
      formatter: p => {
        const it = data[p.dataIndex] || {}
        return '商品 ' + it.name +
          (it.cat ? '<br/>类目 ' + it.cat : '') +
          '<br/>销量 ' + fmtInt(it.cnt)
      }
    },
    grid: { left: 4, right: 34, top: 8, bottom: 0, containLabel: true },
    xAxis: {
      type: 'value',
      ...axisStyle(),
      splitLine: { lineStyle: { color: COLORS.split, type: 'dashed' } },
      axisLabel: { color: COLORS.text, fontSize: 10, formatter: v => fmtInt(v) }
    },
    yAxis: {
      type: 'category',
      data: data.map(i => i.name.length > 12 ? i.name.slice(0, 12) + '…' : i.name),
      ...axisStyle(),
      splitLine: { show: false },
      axisLabel: { color: COLORS.text, fontSize: 10 }
    },
    series: [{
      type: 'bar',
      barMaxWidth: 10,
      showBackground: true, // 官方排行榜样式：淡色背景轨道
      backgroundStyle: { color: 'rgba(148, 163, 184, 0.08)', borderRadius: [0, 4, 4, 0] },
      data: data.map(i => i.cnt),
      itemStyle: {
        borderRadius: [0, 4, 4, 0],
        color: hGrad('rgba(0,212,255,0.95)', 'rgba(124,58,237,0.30)')
      },
      label: {
        show: true,
        position: 'right',
        color: COLORS.cyan,
        fontSize: 10,
        fontFamily: 'Consolas',
        formatter: p => fmtInt(p.value)
      }
    }]
  }
}

const { el } = useEChart(option, [list])
</script>

<template>
  <div class="panel">
    <div class="panel-title">
      商品销量 Top10
      <span class="panel-sub">按当日购买量降序</span>
    </div>
    <div ref="el" class="chart-box"></div>
  </div>
</template>
