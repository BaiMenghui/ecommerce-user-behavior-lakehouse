# PPT 英文词汇汇总

> 来源：《20232102540-白梦辉-开题报告-电商用户行为湖仓分析平台-v8.pptx》（20 页）
> 统计口径：唯一词条 104 个，总出现 383 次；页码为该词出现的位置

---

## 一、技术组件与工具（14 个）

| 词汇 | 次数 | 出现页码 |
|---|---|---|
| Hudi | 31 | 1,2,3,4,5,7,8,10,12,13,15,16,17,18 |
| Spark | 13 | 1,3,7,12,13,15,16,17,19 |
| Hive | 14 | 2,3,8,15,16,17 |
| Kafka | 8 | 3,7,8,13,15,18 |
| Vue3 | 7 | 1,3,7,10,11,13,15 |
| Apache | 7 | 1,3,8,13,15 |
| HDFS | 6 | 3,5,8,12,13,15 |
| Redis | 6 | 3,7,10,11,13,15 |
| ECharts | 6 | 3,7,11,13,15 |
| Docker / Compose | 6 | 5,6,15,17 |
| SpringBoot | 5 | 3,10,11,13,15 |
| UserBehavior（含 UserBehavior.csv） | 6 | 3,4,7,8,13 |
| Metastore | 3 | 3,8,15 |
| Flume / DataX / Hadoop / Parquet / ORC / CSV | 1/1/1/2/2/3 | 3,8,13,15 |

## 二、湖仓/数仓分层（9 个）

| 词汇 | 次数 | 出现页码 |
|---|---|---|
| ADS | 17 | 3,6,8,9,10,11,12,17,19 |
| ODS | 10 | 3,6,8,9,11,12,18 |
| DWD | 8 | 3,6,8,9,11,12,14 |
| DWS | 8 | 3,6,8,9,11,12,19 |
| DIM | 6 | 6,8,9,11,12 |
| ods / dim / dwd / dws / ads（小写路径示意） | 各1 | 7 |

## 三、核心概念与术语（26 个）

| 词汇 | 次数 | 出现页码 | 含义速记 |
|---|---|---|---|
| COW（Copy on Write） | 18 | 7,8,9,11,12,13,16 | 写时复制表类型 |
| MOR（Merge on Read） | 17 | 7,8,9,11,12,13,16 | 读时合并表类型 |
| ACID | 6 | 5,7,8,13,15 | 事务四特性 |
| Kimball | 9 | 1,3,5,7,11,12,14 | 维度建模方法论 |
| API | 9 | 3,7,10,11,13,15 | 接口 |
| SQL | 6 | 3,6,15,16 | 结构化查询 |
| RESTful / REST | 5+2 | 3,7,10,12,13,15 | 接口风格 |
| DAU | 5 | 6,9,10,11,17 | 日活跃用户 |
| GMV | 5 | 6,9,10,11,17 | 成交总额 |
| ETL | 4 | 3,6,13,15 | 抽取-转换-加载 |
| vs | 4 | 2,16 | 对比 |
| HMS（Hive Metastore） | 2 | 12,13 | 元数据服务 |
| Upsert | 2 | 16 | 更新+插入 |
| RFM | 2 | 19 | 用户价值模型 |
| K-means | 2 | 19 | 聚类算法 |
| Delta / Iceberg / DLI / Lake | 各1 | 4 | 竞品/云服务 |
| SaaS | 1 | 4 | 软件即服务 |
| GrowingIO | 1 | 4 | 埋点分析产品 |
| MaxCompute | 1 | 4 | 阿里云计算引擎 |
| Timeline | 1 | 18 | Hudi 时间线 |
| TTL | 1 | 10 | 缓存过期时间 |
| Top-N | 1 | 16 | 排行榜查询 |
| GET | 2 | 10,11 | HTTP 方法 |
| Web / App | 各1 | 8 | 端侧 |

## 四、字段名与表名（20 个）

| 词汇 | 次数 | 出现页码 |
|---|---|---|
| dt（分区日期） | 8 | 3,6,8,9,10,11,16 |
| dau（小写字段） | 4 | 10,11 |
| start / end | 2+1 | 10 |
| metric / code / data / api（小写） | 各2 | 10,11 |
| user_id / item_id / category_id / behavior_type / ts | 各1 | 13 |
| dim_user / dim_item / dim_date | 各2 | 14 |
| dwd_user / behavior_fact / session | 各1 | 14 |
| pv / cart / fav / buy（行为枚举） | 各1 | 4 |
| gap / lag / s / SRC | 各1 | 3,12 |

## 五、图表标注与模板残留（15 个）

| 词汇 | 次数 | 出现页码 | 说明 |
|---|---|---|---|
| x | 13 | 15,16 | 倍数"3x"或坐标轴 |
| DATA | 5 | 8,11 | "DATA STORAGE/PROCESSING"标签 |
| J | 6 | 19 | 单字母标注 |
| LAYER | 2 | 8 | 架构图分层标签 |
| D / M | 2+1 | 19,11 | 日期格式/缩写 |
| ADS（小写 ads） | 1 | 7 | 路径示意 |
| COLLECTION / STORAGE | 各1 | 8 | 标签 |
| PRESENTATION / PROCESSING / SERVICE | 各1 | 11 | "DATA PROCESSING SERVICE"标签 |
| SCENARIO / METHOD / OBJECTIVE / GROUPS / EXPECTED | 各1 | 16 | 对比实验表头 |

---

## 附：答辩高频词发音与含义提示

| 词汇 | 发音参考 | 含义 |
|---|---|---|
| Hudi | "胡迪"（/ˈhuːdi/） | 湖仓存储引擎 |
| MOR | 读全称 Merge on Read | 读时合并，查询快写放大低 |
| COW | 读全称 Copy on Write | 写时复制，写入快读放大低 |
| Upsert | /ˈʌp.sɜːrt/ | Update + Insert |
| Kimball | /ˈkɪm.bəl/ | 维度建模方法论创立者 |
| RFM | 读字母 R-F-M | 最近消费/频率/金额 |
| DAU | 读字母 D-A-U | 日活跃用户数 |
| GMV | 读字母 G-M-V | 商品交易总额 |
| HMS | 读字母 H-M-S | Hive Metastore 元数据服务 |
| TTL | 读字母 T-T-L | 缓存存活时间 |
| Parquet | /pɑːrˈkeɪ/ "帕凯" | 列式存储格式 |
| ORC | 读字母 O-R-C | 列式存储格式 |
| Kafka | /ˈkɑːf.kə/ "卡夫卡" | 消息队列 |
| ECharts | "一-查茨" | 百度开源可视化库 |
| RESTful | /ˈrest.fəl/ | 接口设计风格 |
