import axios from 'axios'

// axios 实例：走 Vite 代理，/api -> http://localhost:8080
const http = axios.create({
  baseURL: '/api',
  timeout: 30000,
  headers: { Accept: 'application/json' }
})

// 统一解包后端响应 {code:0,msg:"ok",data:...} -> 直接返回 data
async function unwrap(promise) {
  const res = await promise
  const body = res.data
  if (body && typeof body === 'object' && Object.prototype.hasOwnProperty.call(body, 'code')) {
    if (body.code !== 0) throw new Error(body.msg || ('后端返回 code=' + body.code))
    return body.data
  }
  return body
}

// 1. 健康检查（含 cacheType/dataDir/tables）
export const getHealth = () => unwrap(http.get('/health'))

// 2. 可用日期列表（无参，24 天）
export const getDates = () => unwrap(http.get('/dates'))

// 3. 单日 KPI 汇总
export const getKpiSummary = (dt) => unwrap(http.get('/kpi/summary', { params: { dt } }))

// 4. DAU/GMV/订单 24 天趋势（无参）
export const getDauTrend = () => unwrap(http.get('/dau-trend'))

// 5. 单日转化漏斗（pv/fav/cart/buy）
export const getFunnel = (dt) => unwrap(http.get('/funnel', { params: { dt } }))

// 6. 单日商品销量 Top10
export const getTopN = (dt) => unwrap(http.get('/topn', { params: { dt } }))

// 7. 单日类目销售占比（全量 3 万+，前端聚合 Top15+其他）
export const getCategoryShare = (dt) => unwrap(http.get('/category-share', { params: { dt } }))

// 8. 单日 24 小时行为量热力/流量数据
export const getHourHeatmap = (dt) => unwrap(http.get('/hour-heatmap', { params: { dt } }))
