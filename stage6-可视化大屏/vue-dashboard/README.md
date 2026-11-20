# 电商用户行为湖仓分析平台 · 数据大屏（阶段6 前端）

本科毕设"湖仓电商用户行为分析"的阶段6交付：基于 **Vue 3 + Vite 5 + ECharts 5 + axios** 的深色科技风可视化大屏，对接 SpringBoot 数据服务（8 个 REST 接口）的 24 天 ADS 层数据。

## 一、技术栈

| 类别 | 组件 | 版本 |
| --- | --- | --- |
| 框架 | Vue 3（Composition API） | ^3.4 |
| 构建 | Vite 5 | ^5.4 |
| 可视化 | ECharts 5 | ^5.5 |
| HTTP | axios 1.x（统一解包 `{code,msg,data}`） | ^1.7 |
| 布局 | CSS Grid（4+2+1 大屏布局） | — |

## 二、启动步骤

### 1. 启动后端（WSL 内）

```bash
# 进入 WSL 后执行；日志输出到 ~/hudi-bigdata/logs/stage6/app.log
mkdir -p ~/hudi-bigdata/logs/stage6
~/tools/jdk-17.0.21+9/bin/java -jar \
  ~/hudi-bigdata/springboot-service/target/lakehouse-api-1.0.0.jar \
  > ~/hudi-bigdata/logs/stage6/app.log 2>&1 &
# 验证：应返回 {"code":0,...,"data":{"status":"UP","cacheType":"caffeine",...}}
curl http://127.0.0.1:8080/api/health
```

注意：若在 `wsl -e bash -c "..."` 单次会话中用 `nohup ... &` 启动，会话退出后进程可能被 WSL 回收；建议在常驻终端（或已启用 systemd 的 WSL 中用服务单元）运行。

### 2. 安装前端依赖（Windows PowerShell）

```powershell
cd D:\毕设\阶段6-脚本\vue-dashboard
npm.cmd install --registry=https://registry.npmmirror.com
```

### 3. 启动开发服务器

```powershell
npm.cmd run dev
# 浏览器打开 http://localhost:5173
```

生产构建与预览（备选）：

```powershell
npm.cmd run build
npm.cmd run preview -- --port 5173
```

## 三、接口依赖表

所有接口 GET，返回 `{code:0,msg:"ok",data:...}`，前端 axios 实例统一解包。

| # | 接口 | 参数 | data 结构 | 用途 |
| --- | --- | --- | --- | --- |
| 1 | `/api/health` | 无 | `{status,cacheType,dataDir,tables}` | 头部全局状态灯（UP 绿 / 异常红，30s 轮询） |
| 2 | `/api/dates` | 无 | 字符串数组（24 天） | 日期下拉；当前 dt 默认取数组末位 |
| 3 | `/api/kpi/summary` | `dt` | `{dau,new_user_cnt,old_user_cnt,new_user_ratio,order_cnt,gmv,avg_ticket,...}` | 左列 6 张 KPI 卡（新老占比副标注） |
| 4 | `/api/dau-trend` | 无 | 数组 24 项 `{dt,dau,gmv,order_cnt}` | 中上：DAU 柱（左轴）+ GMV 线（右轴）双 Y 轴 |
| 5 | `/api/funnel` | `dt` | `{pv,fav,cart,buy,pv2cart_rate,cart2buy_rate,pv2buy_rate}` | 中下：四级漏斗 + 三段转化率标注 |
| 6 | `/api/topn` | `dt` | 数组 10 项 `{rank_no,item_id,category_id,buy_cnt,gmv,...}` | 右上：Top10 横向条形（按销量降序） |
| 7 | `/api/category-share` | `dt` | 数组（每日 3281~3556 类，逐日不等）`{category_id,buy_cnt,buy_share_ratio,...}` | 右下：**前端聚合 Top15+其他** 后画环状饼图，严禁全量渲染 |
| 8 | `/api/hour-heatmap` | `dt` | 数组 24 项 `{hour_of_day,pv_cnt,fav_cnt,cart_cnt,buy_cnt,...}` | 底部：24 小时堆叠柱 + 行为分布环形（按行为类型求和） |

字段兼容：`src/utils/format.js` 与各图表组件内置键名映射（如 `pv_cnt/pv`、`hour/hour_of_day`、`buy_share_ratio/share/ratio`），后端字段名微调无需改前端主逻辑。

## 四、目录结构

```
vue-dashboard/
├── index.html                  # 深色底 #0B1020，title「电商用户行为湖仓分析平台·数据大屏」
├── package.json                # scripts: dev / build / preview
├── vite.config.js              # port 5173 + host + /api 代理
└── src/
    ├── main.js                 # 挂载入口
    ├── styles.css              # 全局深色科技风（卡片 #111827 / 边框 #1E293B / 青 #00D4FF / 紫 #7C3AED）
    ├── api.js                  # axios 实例 + 8 个接口方法（统一解包 code/msg/data）
    ├── App.vue                 # 4+2+1 Grid 布局 + 全局状态（dates/dt/时钟/健康轮询）
    ├── composables/useEChart.js  # 图表统一封装：init / setOption / resize / dispose
    ├── utils/chart.js          # 配色、渐变、tooltip/轴公共样式、字段取值
    ├── utils/format.js         # 千分位、比率口径兼容（0.32 与 32.5）、字段 pick
    └── components/
        ├── HeaderBar.vue       # 平台名 + 每秒时钟 + 状态灯 + 日期切换
        ├── KpiCards.vue        # 6 卡竖排（DAU/新增/老客/订单/GMV/客单价）
        ├── TrendChart.vue      # DAU 柱 + GMV 线，双 Y 轴
        ├── FunnelChart.vue     # pv→fav→cart→buy 漏斗 + 三段转化率
        ├── TopItems.vue        # Top10 横向条形（降序，最大在顶部）
        ├── CategoryPie.vue     # Top15 + 其他 聚合饼图
        ├── HourFlow.vue        # 24 小时按行为类型堆叠柱
        └── BehaviorDonut.vue   # 行为分布环形（小时数据按类型求和）
```

## 五、大屏布局（4+2+1，CSS Grid）

```
┌──────────────────────────────────────────────┐
│ 头部通栏：标题 · 实时时钟 · 状态灯 · 日期切换  │
├────────┬──────────────────┬──────────────────┤
│ KPI 卡 │  DAU·GMV 趋势    │  商品销量 Top10  │
│ 6 张   ├──────────────────┼──────────────────┤
│ 竖排   │  转化漏斗        │  类目销售占比    │
├────────┴──────────────────┼──────────────────┤
│  24 小时行为流量（堆叠）   │  用户行为分布    │
└───────────────────────────┴──────────────────┘
```

图表统一经 `useEChart` 封装初始化，`window.resize` 自适应；`category-share` 接口的 3000+ 类目先在前端聚合为 Top15 + 「其他」再渲染。

## 六、常见问题（FAQ）

1. **接口 404 / 代理不通（8080 冲突）**
   宿主 Windows 若有其他进程占用 8080，WSL2 的 localhost 转发会落到该进程。本工程代理目标当前配置为 WSL eth0 IP（`vite.config.js` 中 `http://<WSL_IP>:8080`）。WSL 重启后 IP 可能变化，用 `wsl hostname -I` 重新获取并更新；若 `http://localhost:8080/api/health` 能直达 SpringBoot，可改回 `http://localhost:8080`。
2. **WSL 后台进程被回收**
   `wsl -e bash -c "nohup java -jar ... &"` 在会话退出后可能被终止；请在常驻 WSL 终端运行，或启用 systemd 后注册服务。
3. **npm 安装慢 / 失败**
   使用国内镜像：`npm.cmd install --registry=https://registry.npmmirror.com`；仍失败可换 `https://registry.npmmirror.com` 的 taobao 兼容源或检查代理。
4. **端口冲突（5173）**
   `npm.cmd run dev -- --port 5174` 更换端口，或释放占用进程；备选方案 `npm.cmd run build` + `npm.cmd run preview -- --port 5173`。
5. **快照数据**
   `D:\毕设\阶段6-脚本\大屏展示数据\` 存有 8 个接口 × 2 个日期（2017-11-25 与 2017-12-03）共 16 个 JSON 快照（无 dt 参数的 health/dates/dau-trend 两个时点内容一致），可用于离线演示与一致性核对：`kpi-summary_dt2017-11-25.json` 中 `dau=7308`、`gmv=556190.0`、`avg_ticket=408.36`，`dates` 长度 24、末位 `2017-12-03`。
