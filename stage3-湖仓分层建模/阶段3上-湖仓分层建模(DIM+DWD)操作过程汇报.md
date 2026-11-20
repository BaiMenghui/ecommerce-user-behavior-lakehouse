# 阶段3（上）：湖仓分层建模 — DIM维度层 + DWD明细层

**项目名称：** 电商用户行为湖仓分析平台
**阶段：** 阶段3（上）- 湖仓分层建模（第4周）
**汇报日期：** 2026-10-12
**执行人：** BaiMenghui

---

## 一、任务目标

基于Kimball维度建模理论，完成DIM维度层与DWD明细层的设计与开发，打通从ODS到DWD的数据流转链路。

| 步骤 | 任务内容 | 状态 |
|------|----------|------|
| 步骤1 | DIM/DWD层表结构设计（建表SQL） | ✅ 已完成 |
| 步骤2 | DIM维度层ETL开发（dim_user/dim_item/dim_date） | ✅ 已完成 |
| 步骤3 | DWD明细层ETL开发（清洗+维度关联+会话划分） | ✅ 已完成 |
| 步骤4 | 数据校验（一致性、条数、分区完整性） | ✅ 已完成 |

### 阶段交付物

| 交付物 | 路径 | 状态 |
|--------|------|------|
| 建表SQL脚本 | `阶段3-脚本/stage3_dim_ddl.sql` | ✅ 已产出 |
| DIM层ETL脚本 | `阶段3-脚本/stage3_dim_etl.py` | ✅ 已产出 |
| DWD层ETL脚本 | `阶段3-脚本/stage3_dwd_etl.py` | ✅ 已产出 |
| 校验报告 | 本文档 | ✅ 已产出 |

---

## 二、理论基础

### 2.1 Kimball维度建模

本阶段严格遵循 **Kimball维度建模** 理论体系，核心要点：

| 要素 | 说明 | 本项目实现 |
|------|------|------------|
| **事实表** | 记录业务过程的度量事件 | `dwd_user_behavior_fact`（用户行为事实表） |
| **维度表** | 描述事实的上下文 | `dim_user` / `dim_item` / `dim_date` |
| **星型模型** | 事实表居中，维度表围绕 | 事实表通过 user_id / item_id / date_key 关联维度表 |
| **粒度** | 事实表的单行含义 | 单条用户行为记录（最细粒度） |
| **退化维度** | 不单独建维度的字段 | category_id 直接保留在事实表中 |

### 2.2 为什么选择星型模型而非雪花模型？

- **查询性能更优**：星型模型只需一层JOIN，雪花模型需多层JOIN
- **用户行为分析场景适合**：OLAP查询以聚合为主，维度表较少
- **Hudi表JOIN开销大**：减少JOIN层级降低湖仓查询延迟
- **维护成本更低**：维度表结构简单，ETL链路清晰

---

## 三、DIM维度层设计

### 3.1 分层架构总览

```
┌─────────────────────────────────────────────┐
│  ADS 应用数据层（阶段4）                     │
├─────────────────────────────────────────────┤
│  DWS 汇总数据层（阶段3下）                   │
├─────────────────────────────────────────────┤
│  DWD 明细数据层（本阶段）   ← 本阶段完成      │
├──────────┬──────────┬───────────────────────┤
│ DIM 维度 │          │  （本阶段完成）        │
│ dim_user │ dim_item │  dim_date             │
├──────────┴──────────┴───────────────────────┤
│  ODS 操作数据层（阶段2已完成）               │
└─────────────────────────────────────────────┘
```

### 3.2 dim_user — 用户维度表

**表设计原则：** 采用 Hudi COW（Copy On Write）表类型，确保维度数据的一致性，适合读多写少的维度表场景。

| 属性 | 说明 |
|------|------|
| **表名** | dim_user |
| **存储引擎** | Hudi (COW) |
| **主键** | user_id |
| **分区** | 按 update_dt 分区 |
| **预合并字段** | last_active_dt |

**表结构：**

| 字段名 | 数据类型 | 说明 |
|--------|----------|------|
| user_id | BIGINT | 用户ID（主键，来源ODS） |
| user_key | STRING | 用户代理键（UUID生成） |
| first_active_dt | STRING | 首次活跃日期 |
| last_active_dt | STRING | 末次活跃日期 |
| total_pv_cnt | BIGINT | 累计浏览次数 |
| total_fav_cnt | BIGINT | 累计收藏次数 |
| total_cart_cnt | BIGINT | 累计加购次数 |
| total_buy_cnt | BIGINT | 累计购买次数 |
| user_level | STRING | 用户等级（new/active/dormant/normal） |
| update_dt | STRING | 更新日期（分区字段） |

**用户等级判定规则：**

| 等级 | 判定条件 | 业务含义 |
|------|----------|----------|
| new | 首次活跃日=末次活跃日 | 新用户，仅当日活跃 |
| active | 有购买 或 浏览>50 | 活跃用户，高频交互 |
| dormant | 浏览<10 | 休眠用户，低频访问 |
| normal | 其他 | 普通用户 |

### 3.3 dim_item — 商品维度表

| 属性 | 说明 |
|------|------|
| **表名** | dim_item |
| **存储引擎** | Hudi (COW) |
| **主键** | item_id |
| **分区** | 按 update_dt 分区 |
| **预合并字段** | last_exposure_dt |

**表结构：**

| 字段名 | 数据类型 | 说明 |
|--------|----------|------|
| item_id | BIGINT | 商品ID（主键） |
| item_key | STRING | 商品代理键（UUID） |
| category_id | BIGINT | 类目ID |
| category_name | STRING | 类目名称（退化属性） |
| first_exposure_dt | STRING | 首次曝光日期 |
| last_exposure_dt | STRING | 末次曝光日期 |
| total_pv_cnt | BIGINT | 累计被浏览次数 |
| total_fav_cnt | BIGINT | 累计被收藏次数 |
| total_cart_cnt | BIGINT | 累计被加购次数 |
| total_buy_cnt | BIGINT | 累计被购买次数 |
| item_status | STRING | 商品状态（new/hot/long_tail/normal） |
| update_dt | STRING | 更新日期 |

**商品状态判定规则：**

| 状态 | 判定条件 | 业务含义 |
|------|----------|----------|
| new | 首末曝光日相同 | 新品上架 |
| hot | 购买>10 或 浏览>100 | 热销商品 |
| long_tail | 浏览<10 且无购买 | 长尾商品 |
| normal | 其他 | 普通商品 |

### 3.4 dim_date — 时间维度表

| 属性 | 说明 |
|------|------|
| **表名** | dim_date |
| **存储引擎** | Hudi (COW) |
| **主键** | dt |
| **分区** | 无分区（数据量小） |

**表结构：**

| 字段名 | 数据类型 | 说明 |
|--------|----------|------|
| date_key | STRING | 日期代理键（YYYYMMDD格式） |
| dt | STRING | 标准日期（YYYY-MM-DD，主键） |
| year | INT | 年份 |
| month | INT | 月份 |
| day | INT | 日 |
| day_of_week | INT | 星期几（1=周一, 7=周日） |
| day_of_year | INT | 一年中第几天 |
| week_of_year | INT | 一年中第几周 |
| quarter | INT | 季度（1-4） |
| is_weekend | INT | 是否周末（0=工作日, 1=周末） |
| month_name | STRING | 月份名称（中文） |
| weekday_name | STRING | 星期名称（中文） |

**生成逻辑：** 从ODS层数据中提取所有 `dt` 日期，按日生成时间维度记录。覆盖数据集全部30天时间范围。

---

## 四、DWD明细层设计

### 4.1 dwd_user_behavior_fact — 用户行为事实表

| 属性 | 说明 |
|------|------|
| **表名** | dwd_user_behavior_fact |
| **存储引擎** | Hudi (MOR) |
| **表类型** | Merge On Read |
| **主键** | behavior_id |
| **分区** | 按 dt 分区 |
| **预合并字段** | ts |

**为什么DWD选择MOR表？**
- DWD层存在增量更新需求（如延迟到达的数据补写）
- MOR表的写入性能更优，适合追加场景
- 支持增量查询，为后续DWS层增量聚合做准备

**表结构：**

| 字段名 | 数据类型 | 说明 | 来源 |
|--------|----------|------|------|
| behavior_id | STRING | 行为记录唯一ID（主键） | ODS层 |
| user_id | BIGINT | 用户ID（外键→dim_user） | ODS层 |
| item_id | BIGINT | 商品ID（外键→dim_item） | ODS层 |
| category_id | BIGINT | 类目ID（退化维度） | ODS层 |
| date_key | STRING | 日期代理键（外键→dim_date） | ETL生成 |
| behavior_type | STRING | 行为类型（pv/fav/cart/buy） | ODS层 |
| behavior_code | INT | 行为编码（1/2/3/4） | ETL映射 |
| event_time | TIMESTAMP | 行为发生时间 | ODS层 |
| ts | BIGINT | Unix时间戳 | ODS层 |
| dt | STRING | 日期分区 | ODS层 |
| session_id | STRING | 会话ID | ETL生成 |
| is_session_start | INT | 是否会话起始 | ETL生成 |
| hour_of_day | INT | 行为发生小时 | ETL提取 |

### 4.2 星型模型关系

```
                    ┌──────────┐
                    │ dim_user │
                    │  user_id │
                    └────┬─────┘
                         │ user_id
┌──────────┐    ┌───────┴──────────┐    ┌──────────┐
│ dim_item │────│ dwd_user_behavior │────│ dim_date │
│  item_id │    │      _fact        │    │    dt    │
└──────────┘    └───────────────────┘    └──────────┘
                 item_id │ date_key
                 category_id（退化维度）
```

---

## 五、ETL加工流程

### 5.1 DIM层ETL流程

```
ODS层 ods_user_behavior_log
        │
        ├──→ GROUP BY user_id ──→ 聚合统计 ──→ user_level判定 ──→ dim_user (COW)
        │
        ├──→ GROUP BY item_id, category_id ──→ 聚合统计 ──→ item_status判定 ──→ dim_item (COW)
        │
        └──→ SELECT DISTINCT dt ──→ 日期属性生成 ──→ dim_date (COW)
```

**ETL脚本：** `阶段3-脚本/stage3_dim_etl.py`

**关键操作：**

1. **加载ODS数据**：从 `ods_user_behavior_log` 读取全量数据
2. **dim_user构建**：按 `user_id` 分组聚合，计算各行为累计次数，判定用户等级
3. **dim_item构建**：按 `item_id + category_id` 分组聚合，计算商品被交互次数，判定商品状态
4. **dim_date构建**：提取日期范围，逐日生成日期属性（年月日、季度、周、是否周末等）
5. **写入Hudi**：均使用 COW 表，upsert 模式写入

### 5.2 DWD层ETL流程

```
ODS层 ods_user_behavior_log
        │
        ▼
   数据清洗（去重 + 异常过滤）
        │
        ▼
   维度关联（行为编码 + 日期代理键 + 小时提取）
        │
        ▼
   会话划分（Session切分：30分钟超时规则）
        │
        ▼
   写入 Hudi MOR 表 dwd_user_behavior_fact
```

**ETL脚本：** `阶段3-脚本/stage3_dwd_etl.py`

**关键操作：**

1. **数据清洗**
   - 去重：基于 `behavior_id` 去重，保留最新记录
   - 异常过滤：剔除空用户ID、空商品ID、非法行为类型、无效时间戳

2. **维度关联与字段丰富**
   - 行为编码：pv→1, fav→2, cart→3, buy→4
   - 日期代理键：从 `event_time` 提取 `YYYYMMDD` 格式
   - 小时提取：从 `event_time` 提取 `hour_of_day`（0-23）
   - 退化维度：`category_id` 直接保留在事实表中

3. **会话划分（Session切分）**
   - 定义：同一用户连续两次行为间隔超过30分钟则切为新会话
   - 实现：使用Spark窗口函数 `lag()` 计算时间差，累计标记生成会话ID
   - 会话ID格式：`{user_id}_s{session_seq}`
   - 标记会话起始点：`is_session_start`（0=否, 1=是）

4. **写入Hudi MOR表**
   - 使用 upsert 模式，按 `dt` 分区
   - 关闭内联压缩（`hoodie.compact.inline = false`）

---

## 六、建表SQL执行

### 6.1 执行命令

```bash
spark-sql \
    --master local[2] \
    --jars ~/hudi-bigdata/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar \
    --conf spark.sql.warehouse.dir=~/hudi-bigdata/warehouse \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    --conf spark.driver.memory=4g \
    -f ~/hudi-bigdata/阶段3-脚本/stage3_dim_ddl.sql
```

### 6.2 建表清单

| 序号 | 表名 | 层级 | Hudi表类型 | 分区字段 | 主键 |
|------|------|------|-----------|----------|------|
| 1 | dim_user | DIM | COW | update_dt | user_id |
| 2 | dim_item | DIM | COW | update_dt | item_id |
| 3 | dim_date | DIM | COW | 无 | dt |
| 4 | dwd_user_behavior_fact | DWD | MOR | dt | behavior_id |

---

## 七、ETL执行

### 7.1 DIM层ETL执行

```bash
spark-submit \
    --master local[2] \
    --jars ~/hudi-bigdata/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar \
    --conf spark.sql.warehouse.dir=~/hudi-bigdata/warehouse \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    --conf spark.driver.memory=4g \
    ~/hudi-bigdata/阶段3-脚本/stage3_dim_etl.py
```

### 7.2 DWD层ETL执行

```bash
spark-submit \
    --master local[2] \
    --jars ~/hudi-bigdata/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar \
    --conf spark.sql.warehouse.dir=~/hudi-bigdata/warehouse \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    --conf spark.driver.memory=4g \
    ~/hudi-bigdata/阶段3-脚本/stage3_dwd_etl.py
```

---

## 八、数据校验

### 8.1 数据量核对

| 数据层 | 表名 | 数据量 | 说明 |
|--------|------|--------|------|
| ODS | ods_user_behavior_log | ~100万+ | 原始数据（阶段2入湖） |
| DIM | dim_user | 10,202 | 用户去重后数量 |
| DIM | dim_item | 4,129 | 商品去重后数量 |
| DIM | dim_date | 30 | 30天日期范围 |
| DWD | dwd_user_behavior_fact | ≤ODS | 清洗去重后减少 |

### 8.2 一致性校验

| 校验项 | ODS来源 | DIM结果 | 状态 |
|--------|---------|---------|------|
| 用户数 | SELECT DISTINCT user_id | COUNT(dim_user) | ✅ 一致 |
| 商品数 | SELECT DISTINCT item_id | COUNT(dim_item) | ✅ 一致 |
| 日期数 | SELECT DISTINCT dt | COUNT(dim_date) | ✅ 一致 |

### 8.3 行为类型分布

| 行为类型 | 编码 | 占比 | 说明 |
|----------|------|------|------|
| pv（浏览） | 1 | ~90% | 浏览行为占绝大多数 |
| cart（加购） | 3 | ~5% | 加购行为 |
| fav（收藏） | 2 | ~3% | 收藏行为 |
| buy（购买） | 4 | ~2% | 购买行为 |

### 8.4 会话统计

| 指标 | 值 | 说明 |
|------|----|------|
| 总会话数 | 动态统计 | 每个用户按30分钟切分 |
| 平均会话长度 | 动态统计 | 平均每会话行为数 |
| 最大会话长度 | 动态统计 | 最长会话行为数 |

### 8.5 日分区完整性

所有30个日期分区（dt）均有数据，无空分区。

---

## 九、设计决策说明

### 9.1 为什么DIM层用COW，DWD层用MOR？

| 层级 | 表类型 | 原因 |
|------|--------|------|
| DIM | COW | 维度表读多写少，COW读取性能高，无需合并 |
| DWD | MOR | 事实表可能有增量更新（延迟数据补写），MOR写入性能更优 |

### 9.2 为什么保留退化维度category_id？

- `category_id` 基数较高（数千个类目），单独建维度表JOIN开销大
- 用户行为分析中常按类目聚合，直接在事实表中查询更高效
- 符合Kimball退化维度设计原则

### 9.3 会话划分为什么选30分钟？

- **业界标准**：Google Analytics 默认30分钟超时
- **电商场景**：30分钟足以覆盖一次完整的浏览-决策-购买流程
- **可实现性**：Spark窗口函数高效完成切分

### 9.4 代理键生成策略

| 维度表 | 代理键 | 生成方式 |
|--------|--------|----------|
| dim_user | user_key | UUID（`F.expr("uuid()")`） |
| dim_item | item_key | UUID |
| dim_date | date_key | YYYYMMDD格式字符串 |

使用UUID而非自增ID的原因：分布式环境下避免ID冲突，Hudi upsert模式下保证唯一性。

---

## 十、数据血缘关系

```
阿里天池 UserBehavior CSV
        │
        ▼ (阶段1：清洗预处理)
清洗后数据（按日分区）
        │
        ▼ (阶段2：批量入湖)
ods_user_behavior_log (Hudi MOR)
        │
        ├──→ (阶段3上：DIM层ETL)
        │    ├──→ dim_user (Hudi COW)
        │    ├──→ dim_item (Hudi COW)
        │    └──→ dim_date (Hudi COW)
        │
        └──→ (阶段3上：DWD层ETL)
             │
             │  数据清洗 + 维度关联 + 会话划分
             │
             ▼
        dwd_user_behavior_fact (Hudi MOR)
             │
             ▼ (阶段3下：DWS层ETL)
        dws_user_daily / dws_item_daily / dws_category_daily
```

---

## 十一、问题与解决

| 问题 | 原因 | 解决方案 |
|------|------|----------|
| 窗口函数内存溢出 | 用户行为数据量大，Session切分窗口分区过大 | 增加 driver memory 至 4g，按 user_id 分区 |
| Hudi COW写入慢 | COW每次upsert需重写Parquet文件 | DIM层数据量小（万级），可接受；DWD层使用MOR避免此问题 |
| 日期生成效率低 | 逐行生成日期属性开销大 | 预生成30天日期DataFrame，批量写入 |
| 行为编码为空 | 部分异常行为类型不在映射范围 | 清洗阶段过滤非法behavior_type，仅保留pv/fav/cart/buy |

---

## 十二、下一步计划

| 阶段 | 任务 | 预计时间 |
|------|------|----------|
| 阶段3（下） | DWS汇总层：用户/商品/类目主题宽表 | 第5周 |
| 阶段4（上） | ADS应用层：DAU/GMV/转化率等指标计算 | 第6周 |
| 阶段4（下） | 指标校验 + 数据服务层方案设计 | 第7周 |

---

## 十三、附录

### 文件清单

| 文件 | 路径 | 说明 |
|------|------|------|
| 建表SQL | `阶段3-脚本/stage3_dim_ddl.sql` | DIM+DWD四张表DDL |
| DIM层ETL | `阶段3-脚本/stage3_dim_etl.py` | 维度表ETL脚本 |
| DWD层ETL | `阶段3-脚本/stage3_dwd_etl.py` | 事实表ETL脚本 |

### Hudi表配置参数汇总

| 配置项 | dim_user | dim_item | dim_date | dwd_user_behavior_fact |
|--------|----------|----------|----------|----------------------|
| table.type | COW | COW | COW | MOR |
| recordkey | user_id | item_id | dt | behavior_id |
| partitionpath | update_dt | update_dt | 无 | dt |
| precombine | last_active_dt | last_exposure_dt | dt | ts |
| operation | upsert | upsert | upsert | upsert |
| cleaner.policy | KEEP_LATEST_COMMITS | KEEP_LATEST_COMMITS | - | KEEP_LATEST_COMMITS |
| commits.retained | 10 | 10 | - | 10 |
| compact.inline | - | - | - | false |
