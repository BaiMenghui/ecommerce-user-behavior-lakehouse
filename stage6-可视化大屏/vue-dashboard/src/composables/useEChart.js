import { ref, onMounted, onBeforeUnmount, watch } from 'vue'
import { createChart, disposeChart } from '../utils/chart.js'

/**
 * 图表统一封装：初始化 / setOption / window.resize / 销毁
 * @param {Function} optionFn 返回 ECharts option 的函数
 * @param {Array} deps 需要监听重绘的响应式依赖数组
 */
export function useEChart(optionFn, deps = []) {
  const el = ref(null)
  let chart = null
  let ro = null

  function render() {
    if (chart && el.value) chart.setOption(optionFn(), true)
  }

  onMounted(() => {
    if (!el.value) return
    chart = createChart(el.value)
    render()
    // 容器晚于挂载才有尺寸（首帧 0×0 / 布局折叠）时，尺寸变化后自动 resize 补渲染
    if (typeof ResizeObserver !== 'undefined') {
      ro = new ResizeObserver(() => {
        if (chart && el.value && el.value.clientWidth > 0 && el.value.clientHeight > 0) chart.resize()
      })
      ro.observe(el.value)
    }
  })

  if (deps.length) {
    watch(deps, render, { deep: true })
  }

  onBeforeUnmount(() => {
    if (ro) ro.disconnect()
    if (chart) disposeChart(chart)
    chart = null
  })

  return { el, render }
}
