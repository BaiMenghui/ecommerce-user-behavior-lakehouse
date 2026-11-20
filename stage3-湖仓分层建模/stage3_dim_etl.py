# -*- coding: utf-8 -*-
"""
阶段3（上）：DIM维度层 ETL脚本
从ODS层抽取数据，构建三个维度表：
  1. dim_user  — 用户维度表
  2. dim_item  — 商品维度表
  3. dim_date  — 时间维度表
"""
# ============================================================
# 【脚本总述】
#   脚本名称 : stage3_dim_etl.py
#   所属阶段 : 阶段3 湖仓分层构建 —— 第一步：DIM 维度层
#   脚本职责 : 以 ODS 层原始行为日志为唯一数据来源，构建数据仓库
#              最底层的"一致性维度"三张表：
#              (1) dim_user 用户维度  —— 从行为日志按用户反向聚合出
#                  首末活跃日、四类行为累计次数，并打上用户等级标签；
#              (2) dim_item 商品维度 —— 按(商品,类目)聚合出首末曝光
#                  日、四类行为累计次数，保留类目归属并打商品状态标签；
#              (3) dim_date 日期维度 —— 从 ODS 现存日分区反查日期
#                  范围，生成日期代理键、年月日、星期、是否周末等
#                  标准日期属性，供下游事实表关联。
#   输入表   : ods_user_behavior_log（Hudi MOR 表，ODS 原始行为明细，
#              104.86 万条、25 个日分区 dt=2017-09-11 ~ 2017-12-03）
#   输出表   : dim_user / dim_item / dim_date（均为 Hudi
#              COPY_ON_WRITE 表，落盘于 warehouse 目录下）
#   运行方式 : WSL 内执行（HoodieCatalog 依赖 Hive metastore 发现表，
#              因此必须带 --conf spark.sql.catalogImplementation=hive）：
#   spark-submit --jars hudi-spark3.3-bundle_2.12-0.14.1.jar \
#     --conf spark.sql.catalogImplementation=hive \
#     stage3_dim_etl.py
# ============================================================
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ============================================================
# Spark Session 初始化
# ------------------------------------------------------------
# 配置要点（逐项说明）：
#   * spark_catalog 指向 Hudi 的 HoodieCatalog：让 Spark SQL 能通过
#     Hive metastore 识别 ods_user_behavior_log / dim_xxx 等湖表，
#     从而直接用 spark.sql(...) 读写 Hudi 表（该能力依赖提交时的
#     --conf spark.sql.catalogImplementation=hive）；
#   * KryoSerializer：Hudi 官方要求的高效序列化器，读写湖表必备；
#   * HoodieSparkSessionExtension：注入 Hudi 的 SQL 解析/优化扩展，
#     支持针对湖表的 upsert 等写入语义；
#   * driver/executor 各 4g：单机跑 104.86 万条数据的聚合，按本机
#     资源预留内存，避免 groupBy 聚合阶段 OOM。
# ============================================================
spark = SparkSession.builder \
    .appName("stage3_dim_etl") \
    .config("spark.sql.warehouse.dir", "/home/BaiMenghui/hudi-bigdata/warehouse") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 工具函数
# ------------------------------------------------------------
# get_today()：取脚本运行当日日期字符串（格式 YYYY-MM-DD）。
# 用作三张维表的 update_dt（批次更新日期）——同一批全量重建的
# 维度记录落进同一个"批次日期"分区，便于按批追溯与整批重跑。
# ============================================================
def get_today():
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d")

TODAY = get_today()

# ============================================================
# 1. 加载 ODS 层数据
# ============================================================
print("=" * 60)
print("步骤1：加载 ODS 层数据")
print("=" * 60)

# 用 Spark SQL 直读 ODS 湖表（得益于 HoodieCatalog，Hudi 表已注册
# 进 metastore，可像普通表一样查询）。此处不做任何转换，只投影出
# 构建三张维度表所需的 8 个字段：
#   behavior_id 行为唯一键 / user_id 用户 / item_id 商品 /
#   category_id 类目 / behavior_type 行为类型(pv,fav,cart,buy) /
#   ts 秒级时间戳 / event_time 可读事件时间 / dt 日分区。
# 说明：本数据集没有独立的用户档案表/商品档案表，因此用户、商品
# 两个维度是从行为日志"反向聚合"生成的；日期维度也从日志现存
# 分区反查，保证维表覆盖范围与事实数据严格一致。
ods_df = spark.sql("""
    SELECT
        behavior_id,
        user_id,
        item_id,
        category_id,
        behavior_type,
        ts,
        event_time,
        dt
    FROM ods_user_behavior_log
""")

print(f"ODS层数据量: {ods_df.count()} 条")
ods_df.printSchema()

# ============================================================
# 2. 构建 dim_user 用户维度表
# ============================================================
print("\n" + "=" * 60)
print("步骤2：构建 dim_user 用户维度表")
print("=" * 60)

# 2.1 按 user_id 分组聚合，产出每个用户的"生命周期画像"：
#   first/last_active_dt —— 该用户在日志中首次/末次出现的日分区
#     （对 dt 取 min/max），用于判定新老用户与活跃时间跨度；
#   total_*_cnt 4 列 —— 用 sum(when(行为类型==x, 1) 否则 0) 的
#     "条件计数"写法，把四种行为的条数横向摊成 4 个计数列。
#     相比 groupBy(user_id, behavior_type) 再透视，一次聚合直接
#     得到宽格式，省掉后续 pivot 步骤。
user_agg = ods_df.groupBy("user_id").agg(
    F.min("dt").alias("first_active_dt"),
    F.max("dt").alias("last_active_dt"),
    F.sum(F.when(F.col("behavior_type") == "pv", 1).otherwise(0)).alias("total_pv_cnt"),
    F.sum(F.when(F.col("behavior_type") == "fav", 1).otherwise(0)).alias("total_fav_cnt"),
    F.sum(F.when(F.col("behavior_type") == "cart", 1).otherwise(0)).alias("total_cart_cnt"),
    F.sum(F.when(F.col("behavior_type") == "buy", 1).otherwise(0)).alias("total_buy_cnt"),
)

# 用户等级判定
# new: 新用户（首次活跃日=末次活跃日）
# active: 活跃用户（有购买行为 或 浏览>50）
# dormant: 休眠用户（仅浏览且<10）
# 2.2 等级判定细节：多分支 when 从上到下依次短路，命中即停——
#   new     : 整个观测期内只活跃过 1 天（首=末），刚注册或边缘流失；
#   active  : 产生过真实交易价值（buy>0）或高频浏览（pv>50）；
#   dormant : 未达前两条标准且总浏览 <10 次，行为极少近乎沉默；
#   normal  : 其余归为普通用户。
# 另外两项维度表标准动作：
#   user_key 用 F.expr("uuid()") 发全局唯一代理主键——业务键
#     user_id 之外另发代理键是维表惯例，可应对日后多源数据
#     user_id 冲突；
#   update_dt 打上本次构建批次日期（TODAY），作为维表分区字段。
dim_user_df = user_agg.withColumn(
    "user_level",
    F.when(F.col("first_active_dt") == F.col("last_active_dt"), "new")
    .when((F.col("total_buy_cnt") > 0) | (F.col("total_pv_cnt") > 50), "active")
    .when(F.col("total_pv_cnt") < 10, "dormant")
    .otherwise("normal")
).withColumn(
    "user_key", F.expr("uuid()")
).withColumn(
    "update_dt", F.lit(TODAY)
)

# 2.3 字段重排与收窄：主键(user_id,user_key)放最前，其后按
#     属性→度量→标签→批次日期的顺序排定列序，并只保留最终
#     需要的 10 列，避免把聚合中间结果冗余带入维表。
dim_user_df = dim_user_df.select(
    "user_id", "user_key", "first_active_dt", "last_active_dt",
    "total_pv_cnt", "total_fav_cnt", "total_cart_cnt", "total_buy_cnt",
    "user_level", "update_dt"
)

print(f"用户维度表数据量: {dim_user_df.count()} 条")
dim_user_df.show(5, truncate=False)

# 写入 Hudi COW 表（各 option 含义）：
#   table.type=COPY_ON_WRITE —— 写时复制：写入即重写列式文件，
#     读性能优先，契合维表"一次重建、多次点查/关联"的访问模式；
#   operation=upsert —— 按 recordkey(user_id) "存在则更新、
#     不存在则插入"，重跑本脚本不会产生重复主键（幂等）；
#   partitionpath.field=update_dt —— 按批次日期分区，可回溯每次重建；
#   precombine.field=last_active_dt —— 同主键出现多版本时按该
#     字段值取大者保留，确保落表的是最新记录。
dim_user_df.write \
    .format("hudi") \
    .option("hoodie.table.name", "dim_user") \
    .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .option("hoodie.datasource.write.recordkey.field", "user_id") \
    .option("hoodie.datasource.write.partitionpath.field", "update_dt") \
    .option("hoodie.datasource.write.precombine.field", "last_active_dt") \
    .mode("append") \
    .save("/home/BaiMenghui/hudi-bigdata/warehouse/dim_user")

print("dim_user 写入完成")

# ============================================================
# 3. 构建 dim_item 商品维度表
# ============================================================
print("\n" + "=" * 60)
print("步骤3：构建 dim_item 商品维度表")
print("=" * 60)

# 3.1 按 (item_id, category_id) 分组聚合商品画像：同一商品在日志
#     中类目恒定，把 category_id 放进分组键即顺手保留"类目归属"
#     属性；聚合口径与用户维度完全同构——对 dt 取 min/max 得
#     首末曝光日，四类行为用条件计数摊成 4 个累计列。
item_agg = ods_df.groupBy("item_id", "category_id").agg(
    F.min("dt").alias("first_exposure_dt"),
    F.max("dt").alias("last_exposure_dt"),
    F.sum(F.when(F.col("behavior_type") == "pv", 1).otherwise(0)).alias("total_pv_cnt"),
    F.sum(F.when(F.col("behavior_type") == "fav", 1).otherwise(0)).alias("total_fav_cnt"),
    F.sum(F.when(F.col("behavior_type") == "cart", 1).otherwise(0)).alias("total_cart_cnt"),
    F.sum(F.when(F.col("behavior_type") == "buy", 1).otherwise(0)).alias("total_buy_cnt"),
)

# 商品状态判定
# new: 新品（首末曝光日相同）
# hot: 热销（购买>10 或 浏览>100）
# long_tail: 长尾（浏览<10 且 无购买）
# 3.2 状态判定细节：同样是顺序短路的多分支 when——
#   new       : 观测期内只出现过 1 天（首=末曝光日）；
#   hot       : buy>10 或 pv>100，成交或流量维度的头部商品；
#   long_tail : pv<10 且零购买，典型长尾商品（二八定律中的"八"）；
#   normal    : 其余归为普通商品。
# 其余派生字段：item_key 用 uuid() 发代理主键（同 user_key 惯例）；
# category_name 因本数据集缺失类目名，用 "类目_<id>" 拼一个占位
# 可读名称；update_dt 打上构建批次日期。
dim_item_df = item_agg.withColumn(
    "item_status",
    F.when(F.col("first_exposure_dt") == F.col("last_exposure_dt"), "new")
    .when((F.col("total_buy_cnt") > 10) | (F.col("total_pv_cnt") > 100), "hot")
    .when((F.col("total_pv_cnt") < 10) & (F.col("total_buy_cnt") == 0), "long_tail")
    .otherwise("normal")
).withColumn(
    "item_key", F.expr("uuid()")
).withColumn(
    "category_name", F.concat(F.lit("类目_"), F.col("category_id").cast("string"))
).withColumn(
    "update_dt", F.lit(TODAY)
)

# 3.3 字段重排与收窄，列序规范与 dim_user 保持一致。
dim_item_df = dim_item_df.select(
    "item_id", "item_key", "category_id", "category_name",
    "first_exposure_dt", "last_exposure_dt",
    "total_pv_cnt", "total_fav_cnt", "total_cart_cnt", "total_buy_cnt",
    "item_status", "update_dt"
)

print(f"商品维度表数据量: {dim_item_df.count()} 条")
dim_item_df.show(5, truncate=False)

# 写入 Hudi COW 表（参数语义与 dim_user 完全一致：COW 类型 +
# upsert 幂等写，主键换为 item_id，按批次日期 update_dt 分区，
# precombine 字段换为 last_exposure_dt）。
dim_item_df.write \
    .format("hudi") \
    .option("hoodie.table.name", "dim_item") \
    .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .option("hoodie.datasource.write.recordkey.field", "item_id") \
    .option("hoodie.datasource.write.partitionpath.field", "update_dt") \
    .option("hoodie.datasource.write.precombine.field", "last_exposure_dt") \
    .mode("append") \
    .save("/home/BaiMenghui/hudi-bigdata/warehouse/dim_item")

print("dim_item 写入完成")

# ============================================================
# 4. 构建 dim_date 时间维度表
# ============================================================
print("\n" + "=" * 60)
print("步骤4：构建 dim_date 时间维度表")
print("=" * 60)

# 从ODS层获取日期范围
# 4.1 把 ODS 中实际存在的日分区去重、排序后 collect 到 driver 端，
#     得到日志覆盖的日期区间（2017-09-11 ~ 2017-12-03 共 25 天）。
#     dim_date 不凭空补造日期，只覆盖数据真实存在的日期，保证
#     维表与事实表的日期取值一一对应。
date_range = ods_df.select("dt").distinct().orderBy("dt").collect()
dates = [row["dt"] for row in date_range]
print(f"日期范围: {dates[0]} ~ {dates[-1]}, 共 {len(dates)} 天")

# 用纯 SQL 生成日期维度（绕开 Python 3.14 下 cloudpickle 序列化不兼容问题）
# 4.2 纯 SQL 一次性派生全部日期属性（仅对去重后的 25 个日期计算，
#     开销可忽略）。逐字段说明：
#   date_key       : yyyyMMdd 整数型日期代理键（数仓惯例），下游
#                    事实表的 date_key 直接关联本表；
#   year/month/day : 年、月、日整型属性；
#   day_of_week    : 星期几。注意 Spark 的 weekday() 以周一=0 计，
#                    因此 +1 校正为 周一=1 ~ 周日=7 的习惯口径；
#   day_of_year / week_of_year / quarter : 年内第几天 / 第几周 /
#                    季度，支撑"周报/季报"类汇总；
#   is_weekend     : weekday>=5 即周六/周日记 1，"周末购物高峰"
#                    类分析可直接按此列过滤；
#   month_name / weekday_name : element_at(中文数组, 序号) 把月份、
#                    星期映射成中文展示名，供报表直接输出。
dim_date_df = spark.sql("""
    SELECT
        date_format(dt, 'yyyyMMdd') AS date_key,
        dt,
        CAST(year(dt) AS INT) AS year,
        CAST(month(dt) AS INT) AS month,
        CAST(day(dt) AS INT) AS day,
        CAST(weekday(dt) + 1 AS INT) AS day_of_week,
        CAST(dayofyear(dt) AS INT) AS day_of_year,
        CAST(weekofyear(dt) AS INT) AS week_of_year,
        CAST(quarter(dt) AS INT) AS quarter,
        CAST(CASE WHEN weekday(dt) >= 5 THEN 1 ELSE 0 END AS INT) AS is_weekend,
        element_at(array('一月','二月','三月','四月','五月','六月','七月','八月','九月','十月','十一月','十二月'), CAST(month(dt) AS INT)) AS month_name,
        element_at(array('星期一','星期二','星期三','星期四','星期五','星期六','星期日'), CAST(weekday(dt) + 1 AS INT)) AS weekday_name
    FROM (SELECT DISTINCT dt FROM ods_user_behavior_log) t
""")

print(f"时间维度表数据量: {dim_date_df.count()} 条")
dim_date_df.show(5, truncate=False)

# 写入 Hudi COW 表（无分区）
# 4.3 日期维表仅 25 行，无需分区，整表一次写入；主键用 dt（日期
#     本身即天然唯一键），precombine 也用 dt，重跑幂等覆盖。
dim_date_df.write \
    .format("hudi") \
    .option("hoodie.table.name", "dim_date") \
    .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .option("hoodie.datasource.write.recordkey.field", "dt") \
    .option("hoodie.datasource.write.precombine.field", "dt") \
    .mode("append") \
    .save("/home/BaiMenghui/hudi-bigdata/warehouse/dim_date")

print("dim_date 写入完成")

# ============================================================
# 5. 验证
# ============================================================
print("\n" + "=" * 60)
print("步骤5：数据校验")
print("=" * 60)

# 5.1 表级落表核对：通过 metastore 回读三张 DIM 表各自的行数，
#     确认写入动作真实成功（而不是只看写表过程没抛异常）。
for table in ["dim_user", "dim_item", "dim_date"]:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
    print(f"{table}: {cnt} 条")

# 数据一致性校验
# 5.2 维度覆盖率核对：DIM 层实体数必须与 ODS 层去重实体数完全
#     相等——groupBy 不丢组是聚合正确性的底线；一旦出现 ODS 有
#     而 DIM 无的实体，说明聚合或写表环节出了问题，必须排查。
ods_users = ods_df.select("user_id").distinct().count()
dim_users = spark.sql("SELECT COUNT(DISTINCT user_id) FROM dim_user").collect()[0][0]
print(f"\n用户数一致性: ODS={ods_users}, DIM={dim_users}, {'通过' if ods_users == dim_users else '不一致'}")

ods_items = ods_df.select("item_id").distinct().count()
dim_items = spark.sql("SELECT COUNT(DISTINCT item_id) FROM dim_item").collect()[0][0]
print(f"商品数一致性: ODS={ods_items}, DIM={dim_items}, {'通过' if ods_items == dim_items else '不一致'}")

spark.stop()
print("\nDIM层ETL完成！")
