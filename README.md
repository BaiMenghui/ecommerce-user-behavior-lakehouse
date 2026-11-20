# 电商用户行为湖仓分析平台

> 基于 Apache Hudi + Spark 的湖仓一体电商用户行为分析系统：从 100 万+ 条真实行为数据出发，打通「清洗 → 入湖 → 分层建模 → 指标计算 → 服务接口 → 可视化大屏」全链路，六道质量闸门全部通过，全链路脚本化可一键重跑。

- **数据规模**：1,048,576 条行为记录 · 25 个日期分区 · 约 1 万独立用户
- **湖仓表**：10 张 Hudi 表（ODS/DIM/DWD/DWS/ADS 五层）+ 5 份 ADS 导出 CSV
- **服务**：SpringBoot 8 个 REST 接口 · Redis→Caffeine 自动降级
- **大屏**：Vue3 + ECharts，4+2+1 布局 7 面板联动

## 技术栈

| 层 | 技术 |
|---|---|
| 存储 | HDFS 3.3 · Apache Hudi 0.14.1（MOR/COW 混选）· Hive Metastore |
| 计算 | Spark 3.3（Spark SQL / spark-submit）· Kafka + Structured Streaming |
| 服务 | SpringBoot（JDK 17）· Redis / Caffeine |
| 展示 | Vue3 · Vite · ECharts |
| 清洗探查 | Python3 · pandas |

## 仓库结构（严格按照《毕设时间规划与操作步骤》阶段 0~8 组织）

```
├── docs/                          # 项目规划表、项目介绍、逻辑图、架构图
├── stage0-环境搭建/               # docker-compose、Hudi/HMS 连通性验证、元数据修复
├── stage1-数据源准备/             # 探查/清洗脚本、清洗报告、异常样本
├── stage2-数据采集入湖/           # ODS 批量入湖 + Kafka 流式入湖（批流一体）
├── stage3-湖仓分层建模/           # DIM/DWD/DWS 建表与 ETL、三角核对、一键重跑
├── stage4-业务指标计算/           # ADS 五张指标表、21 项交叉校验、CSV 导出
├── stage5-数据服务层/             # SpringBoot API、CSV 数据包、数据修复脚本
├── stage6-可视化大屏/             # vue-dashboard 前端、大屏数据样例、验收截图
├── stage7-系统测试与创新实验/     # 全链路检查报告、性能对比实验设计
└── stage8-论文与答辩/             # 开题材料、答辩讲稿、文献素材
```

每个阶段目录内附该阶段的《操作过程汇报》，记录目标、步骤、踩坑与验收结论。

## 全链路数据流

```
UserBehavior.csv (104万条)
   │  阶段1 pandas 八步清洗（缺失/去重/异常/时间戳→dt 分区）
   ▼
data/cleaned/dt=YYYY-MM-DD/ ×25
   │  阶段2 Spark 批量入湖（MOR，主键 behavior_id，预合并 ts）
   ▼
ods_user_behavior_log ──▶ 阶段2b Kafka+Structured Streaming 流式入湖（微批 upsert）
   │  阶段3 DIM(3) + DWD(1) + DWS(3, COW)，逐分区三角核对
   ▼
dwd_user_behavior_fact / dws_*_action_day
   │  阶段4 ADS(5) 指标表，21 项交叉校验 → export CSV
   ▼
ads_dau_trend / ads_funnel / ads_item_topn / ads_category_share / ads_hour_heatmap
   │  阶段5 SpringBoot 读 CSV 建内存索引，Redis/Caffeine 二级缓存
   ▼
8 个 REST 接口 ──▶ 阶段6 Vue3+ECharts 大屏（7 面板，切日期全联动）
```

## 质量闸门（六道，全 PASS）

1. **清洗四项校验**：记录数 / 缺失值 / 重复值 / 行为类型
2. **入湖分区校验**：25 分区逐分区行数与清洗层一致
3. **分区级三角核对**：明细 = Σ用户 = Σ商品 = Σ类目（四种行为分别对账）
4. **21 项多口径交叉校验**：三层 DAU/GMV/漏斗一致、新老守恒、TopN 排序等
5. **接口冒烟**：8 接口全部 200，数值与 ADS 逐位一致
6. **大屏验收**：7 面板渲染、KPI 核对、服务状态灯正常

## 核心指标基线（校验基准日 vs 大屏默认日）

| 指标 | 2017-11-25 | 2017-12-03 |
|---|---|---|
| DAU | 7,308 | **9,522** |
| 浏览 pv | 99,250 | 113,054 |
| 购买 buy | 2,043 | **2,510** |
| GMV | 556,190 | **695,041** |
| 客单价 | ≈272 | 402.46 |

## 快速开始

> 完整环境与逐条命令见各阶段目录内的操作过程汇报。

1. **数据集**：从[阿里天池 UserBehavior 数据集](https://tianchi.aliyun.com/dataset/649)下载，放入 `stage1-数据源准备/data/`（体积原因不入库，详见该目录说明）
2. **环境**：`stage0-环境搭建/docker-compose.yml` 一键拉起 HDFS/Hive/Spark，验证脚本见同目录
3. **全链路**：依次执行 stage1 清洗 → stage2 入湖 → stage3 分层（`rerun_stage3.sh`）→ stage4 指标（`rerun_stage4.sh` + `export_ads_csv.py`）
4. **服务**：`stage5-数据服务层/springboot-service`（`mvn package` 后运行，端口 8080）
5. **大屏**：`stage6-可视化大屏/vue-dashboard`（`npm install && npm run dev`，端口 5173）

## 创新点

1. **MOR/COW 按层混选**：ODS/DWD 写多读少走 MOR，DIM/DWS/ADS 读多写少走 COW，并以 `test_hudi_mor` / `test_hudi_cow` 对照实验实证
2. **批流一体双链路入湖**：批量贴源与流式贴源双表口径对齐，Hudi 增量查询精确到 commit 级
3. **贯穿六阶段的质量闸门体系**：数据质量做成工程闸门而非事后抽检，重跑结果与基准逐位一致
4. **异常数据治理闭环**：游离分区 dt=2018-08-28「保留血缘 → 边界过滤 → 口径收敛」，服务层支持无 Hadoop 集群部署与缓存自动降级

## 环境说明

- 本项目在 Windows + WSL2（Ubuntu）混合环境开发，Hadoop/Hudi/Spark 运行于 WSL 侧，`~/hudi-bigdata` 为运行根目录
- spark-sql 需在 `~/hudi-bigdata` 目录内启动（Derby 元数据库锁），并携带 `--conf spark.sql.catalogImplementation=hive`

## 许可

仅用于学习与毕业设计展示。
