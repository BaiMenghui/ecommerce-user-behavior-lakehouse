# lakehouse-api — 阶段5 数据服务层（SpringBoot RESTful API + Redis 缓存）

电商用户行为湖仓分析平台的数据服务层：读取 ADS 指标层导出的 CSV（Hudi 导出产物），提供 5 组指标 RESTful 接口 + ECharts 联调预览页。

## 技术栈
- SpringBoot 3.2.10 / Java 17 / Maven
- 缓存：Spring Cache 双实现（`app.cache-type: redis|caffeine`），TTL 5 分钟
  - redis：Spring Data Redis + RedisCacheManager（key 前缀 `ads:<cacheName>:`）
  - caffeine：本地 Caffeine CacheManager
  - Redis 启动时 ping 失败自动降级 caffeine；运行期 Redis 异常按未命中处理，不阻断接口
- X-Cache 响应头：HandlerInterceptor preHandle 置探针 HIT → Controller 方法体（=缓存未命中时才执行）置 MISS → afterCompletion 写响应头

## 目录约定
- 数据目录：`app.data-dir`（默认 `/home/BaiMenghui/hudi-bigdata/export/ads`），下含 5 张表：
  `ads_dau_trend / ads_funnel / ads_item_topn / ads_category_share / ads_hour_heatmap`（每表读所有 `part-*.csv`，按首行表头解析，兼容 BOM 与尾部空列）

## 构建运行（WSL）
```bash
# 拷贝到 WSL 后构建
cp -r /mnt/d/毕设/阶段5-脚本/springboot-service ~/hudi-bigdata/
cd ~/hudi-bigdata/springboot-service
source ~/.bashrc   # JAVA_HOME/M2_HOME 指向 ~/tools
mvn -q -DskipTests package

# 启动（redis 模式；若 Redis 不可用自动降级 caffeine）
nohup java -jar target/lakehouse-api-1.0.0.jar --app.cache-type=redis \
  > ~/hudi-bigdata/logs/stage5/app.log 2>&1 &
```

## 接口表（均返回 `{code:0,msg:"ok",data:...}`；dt 缺省取最新一天）
| 接口 | 说明 |
|---|---|
| GET /api/dates | 可用日期列表 |
| GET /api/kpi/summary?dt= | 单日 DAU/新增/老客/购买人数/订单/GMV/客单价 |
| GET /api/dau-trend | 全部日期 [dt,dau,gmv,order_cnt] 趋势 |
| GET /api/funnel?dt= | 漏斗 pv/fav/cart/buy + pv2cart/cart2buy/pv2buy 转化率 |
| GET /api/topn?dt= | Top10 热销商品 |
| GET /api/category-share?dt=&limit= | 类目购买占比（降序，limit 可选截断，缓存存全量） |
| GET /api/hour-heatmap?dt= | 24 小时行为量 |
| GET /api/health | 健康检查（status/cacheType/dataDir/tables） |
| GET /index.html | ECharts 联调预览页 |

## 缓存切换说明
- `--app.cache-type=redis`（默认配置值）：需 Redis 可达（默认 127.0.0.1:6379）；可用 `redis-cli --scan --pattern 'ads*'` 查看 key
- `--app.cache-type=caffeine`：纯本地缓存，无需 Redis
- 验证 HIT：同一接口连续请求两次，第二次响应头 `X-Cache: HIT`

## 常见问题
- **Redis 连不上**：redis 模式启动时自动降级 caffeine，日志有 `Redis 连接失败...自动降级` 记录；`/api/health` 的 cacheType 可确认实际生效类型
- **端口占用**：`server.port` 默认 8080，可用 `--server.port=xxxx` 覆盖
- **数据目录不对**：`--app.data-dir=/path/to/ads` 覆盖
- **CSV 数值带 .0**：解析层已按表头转换（计数取整、比率取 double），无需处理源文件
