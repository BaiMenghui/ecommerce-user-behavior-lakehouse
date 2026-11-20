<script setup>
import { useEChart } from '../composables/useEChart.js'
import { COLORS, tooltipStyle, axisStyle, vGrad } from '../utils/chart.js'
import { shortDate, fmtInt, fmtNum } from '../utils/format.js'
const props = defineProps({
  rows: { type: Array, default: () => [] }
})

// DAU 柱（左轴）+ GMV 折线（右轴）：双 Y 轴 24 天趋势
function option() {
  const rows = props.rows || []
  return {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      ...tooltipStyle,
      valueFormatter: v => (v == null ? '-' : fmtInt(v))
    },
    legend: {
      top: 0, right: 4, itemWidth: 14, itemHeight: 8,
      textStyle: { color: COLORS.text, fontSize: 11 }
    },
    grid: { left: 6, right: 6, top: 46, bottom: 0, containLabel: true },
    xAxis: {
      type: 'category',
      data: rows.map(r => shortDate(r.dt)),
      ...axisStyle(),
      splitLine: { show: false }
    },
    yAxis: [
      {
        type: 'value', name: 'DAU',
        ...axisStyle(),
        nameTextStyle: { color: COLORS.cyan, fontSize: 11 },
        axisLabel: { color: COLORS.text, fontSize: 11, formatter: v => fmtInt(v) }
      },
      {
        type: 'value', name: 'GMV(¥)',
        ...axisStyle(),
        splitLine: { show: false },
        nameTextStyle: { color: COLORS.purple, fontSize: 11 },
        axisLabel: { color: COLORS.text, fontSize: 11, formatter: v => fmtNum(v, 0) }
      }
    ],
    series: [
      {
        name: 'DAU',
        type: 'bar',
        barMaxWidth: 14,
        data: rows.map(r => Number(r.dau) || 0),
        itemStyle: {
          borderRadius: [3, 3, 0, 0],
          color: vGrad('rgba(0,212,255,0.95)', 'rgba(0,212,255,0.10)')
        }
      },
      {
        name: 'GMV',
        type: 'line',
        yAxisIndex: 1,
        smooth: true,
        symbol: 'circle',
        symbolSize: 5,
        data: rows.map(r => Number(r.gmv) || 0),
        lineStyle: { color: COLORS.purple, width: 2 },
        itemStyle: { color: COLORS.purple },
        areaStyle: { color: vGrad('rgba(124,58,237,0.30)', 'rgba(124,58,237,0.02)') },
        markLine: {
          silent: true,
          symbol: 'none',
          lineStyle: { color: 'rgba(196,181,253,0.45)', type: 'dashed', width: 1 },
          // 标注放图内线端上方，避免压住右轴刻度数字
          label: {
            position: 'insideEndTop',
            color: '#C4B5FD', fontSize: 10,
            formatter: p => 'GMV均值 ¥' + fmtInt(Math.round(p.value))
          },
          data: [{ type: 'average', name: '均值' }]
        }
      }
    ]
  }
}

const { el } = useEChart(option, [() => props.rows])
</script>

<template>
  <div class="panel">
    <div class="panel-title">
      DAU·GMV 24天趋势
      <span class="panel-sub">柱=DAU(左轴) · 线=GMV(右轴)</span>
    </div>
    <div ref="el" class="chart-box"></div>
  </div>
</template>
