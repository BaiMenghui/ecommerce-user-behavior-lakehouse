# -*- coding: utf-8 -*-
# ============================================================
# 脚本名称: fix_hour_heatmap_fav.py
# 所属阶段: 阶段4 修复脚本
# 功能说明: ads_hour_heatmap 表 schema 缺 fav_cnt（收藏）列，导致大屏无法展示收藏维度、
#           且新数据写入会触发 SchemaCompatibilityException。本脚本执行：
#           DROP 旧表 → 清理残留目录 → 按含 fav_cnt 的新结构重建 → 全部日期分区
#           按 DWD 直通口径重算 → 与 ads_funnel 做守恒校验 → 重导 CSV
# 输入: dwd_user_behavior_fact（DWD 明细，重算数据源）、ads_funnel（守恒校验基准）
# 输出: 重建后的 ads_hour_heatmap（含 fav_cnt 列）+ 重导后的单文件 CSV
# 运行方式: WSL 下 spark-submit --jars hudi-spark3.3-bundle_2.12-0.14.1.jar \
#           --conf spark.sql.catalogImplementation=hive fix_hour_heatmap_fav.py
# 幂等性: insert_overwrite 动态覆盖分区，可重复执行
# ============================================================
"""
修复脚本：ads_hour_heatmap 补充 fav_cnt（收藏）维度
背景：原表缺 fav_cnt，大屏"24小时行为流量/用户行为分布"无法展示收藏类行为
动作：DROP 旧表 → 按 stage4_ads_ddl.sql 新结构重建 → 全部日期分区重算 → 重导 CSV
口径：不变，仍为 DWD 明细按 hour_of_day 直通聚合（指标口径文档条款5）
运行：cd ~/hudi-bigdata && spark-submit 阶段4-脚本/fix_hour_heatmap_fav.py
幂等：insert_overwrite 动态覆盖分区，可重复执行
"""
from pyspark.sql import SparkSession

# enableHiveSupport + catalogImplementation=hive：走 Hive metastore，才能执行 DROP/CREATE 表级 DDL
spark = SparkSession.builder \
    .appName("fix_hour_heatmap_fav") \
    .enableHiveSupport() \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir", "/home/BaiMenghui/hudi-bigdata/warehouse") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------- 1. 重建表（新结构含 fav_cnt） ----------
print("步骤1：重建 ads_hour_heatmap（新增 fav_cnt 列）")
import os
import shutil
# DROP 仅删除 metastore 元数据；Hudi 表的数据目录会残留，必须一并清理，
# 否则随后按同名建表会报 location already exists
spark.sql("DROP TABLE IF EXISTS ads_hour_heatmap")
_wh = "/home/BaiMenghui/hudi-bigdata/warehouse/ads_hour_heatmap"
if os.path.exists(_wh):
    shutil.rmtree(_wh)  # Hudi DROP 后目录残留，需清理否则建表报 location already exists
    print("  已清理残留表目录")
# 按新结构重建：与旧表相比多出 fav_cnt 列；COW 表、主键 hour_of_day、precombine=dt。
# DDL 的 OPTIONS 里直接声明 Hudi 表参数，后续写入无需再逐项指定
spark.sql("""
    CREATE TABLE ads_hour_heatmap (
        hour_of_day INT     COMMENT '小时 0-23（分区内主键）',
        action_cnt  BIGINT  COMMENT '该小时总行为量',
        pv_cnt      BIGINT  COMMENT '该小时浏览量',
        fav_cnt     BIGINT  COMMENT '该小时收藏量',
        cart_cnt    BIGINT  COMMENT '该小时加购量',
        buy_cnt     BIGINT  COMMENT '该小时购买量',
        dt                 STRING  COMMENT '日期分区'
    )
    USING hudi
    PARTITIONED BY (dt)
    OPTIONS (
        type = 'cow',
        primaryKey = 'hour_of_day',
        preCombineField = 'dt',
        hoodie.table.name = 'ads_hour_heatmap',
        hoodie.datasource.write.recordkey.field = 'hour_of_day',
        hoodie.datasource.write.partitionpath.field = 'dt',
        hoodie.datasource.write.precombine.field = 'dt',
        hoodie.datasource.write.operation = 'upsert',
        hoodie.datasource.write.table.type = 'COPY_ON_WRITE',
        hoodie.cleaner.policy = 'KEEP_LATEST_COMMITS',
        hoodie.cleaner.commits.retained = 10
    )
""")
print("  重建完成")

# ---------- 2. 全部日期分区重算（与主 ETL 步骤5 同口径） ----------
# 以 dws_user_action_day 现存分区为准遍历（与主 ETL 全量模式一致），逐分区重建热力数据
dt_list = [r["dt"] for r in spark.sql(
    "SELECT DISTINCT dt FROM dws_user_action_day ORDER BY dt").collect()]
print(f"待重算分区: {dt_list}")

def write_hudi(df, table, pk):
    # 与主 ETL 相同的 COW insert_overwrite 写法：按分区动态覆盖，重跑幂等
    df.write.format("hudi") \
        .option("hoodie.table.name", table) \
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
        .option("hoodie.datasource.write.operation", "insert_overwrite") \
        .option("hoodie.datasource.write.recordkey.field", pk) \
        .option("hoodie.datasource.write.partitionpath.field", "dt") \
        .option("hoodie.datasource.write.precombine.field", "dt") \
        .option("hoodie.cleaner.policy", "KEEP_LATEST_COMMITS") \
        .option("hoodie.cleaner.commits.retained", "10") \
        .mode("append") \
        .save(f"/home/BaiMenghui/hudi-bigdata/warehouse/{table}")

for dt in dt_list:
    # 与主 ETL 步骤5 完全同口径：DWD 明细按 hour_of_day 分组，
    # COUNT(CASE WHEN) 条件计数拆出 pv/fav/cart/buy 四列（本次重建新增 fav_cnt）
    hour_df = spark.sql(f"""
        SELECT hour_of_day,
               COUNT(*) AS action_cnt,
               COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END) AS pv_cnt,
               COUNT(CASE WHEN behavior_type = 'fav' THEN 1 END) AS fav_cnt,
               COUNT(CASE WHEN behavior_type = 'cart' THEN 1 END) AS cart_cnt,
               COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt,
               '{dt}' AS dt
        FROM dwd_user_behavior_fact
        WHERE dt = '{dt}'
        GROUP BY hour_of_day ORDER BY hour_of_day
    """)
    write_hudi(hour_df, "ads_hour_heatmap", "hour_of_day")
    print(f"  dt={dt} 写入 {hour_df.count()} 行")

# ---------- 3. 守恒校验：各行为类型合计应与漏斗表一致 ----------
print("步骤2：三角校验（heatmap 合计 vs ads_funnel 当日值）")
# 守恒逻辑：heatmap 24 小时按行为类型求和必须等于漏斗表当日对应值——
# 两者源头同为 DWD 明细、只是聚合维度不同，守恒即证明重算无丢行
chk = spark.sql("""
    SELECT h.dt,
           SUM(h.pv_cnt) AS heat_pv, f.pv_cnt AS fun_pv,
           SUM(h.fav_cnt) AS heat_fav, f.fav_cnt AS fun_fav,
           SUM(h.cart_cnt) AS heat_cart, f.cart_cnt AS fun_cart,
           SUM(h.buy_cnt) AS heat_buy, f.buy_cnt AS fun_buy
    FROM ads_hour_heatmap h JOIN ads_funnel f ON h.dt = f.dt
    GROUP BY h.dt, f.pv_cnt, f.fav_cnt, f.cart_cnt, f.buy_cnt
    ORDER BY h.dt
""")
# 只要存在任一分区的任一行为类型合计不等即判失败，直接抛异常终止（避免带坏数据导出）
bad = chk.filter(
    "heat_pv != fun_pv OR heat_fav != fun_fav OR heat_cart != fun_cart OR heat_buy != fun_buy"
).count()
if bad == 0:
    print("  ALL PASS：24 天 heatmap 四类行为合计与漏斗表完全一致")
else:
    raise RuntimeError(f"校验失败：{bad} 个分区不一致")
chk.show(30, truncate=False)

# ---------- 4. 重导 CSV ----------
print("步骤3：重导 CSV → export/ads/ads_hour_heatmap/")
# 按日期+小时排序，coalesce(1) 合并单文件带表头覆盖旧 CSV，大屏即可读到含 fav_cnt 的新结构
(spark.sql("SELECT * FROM ads_hour_heatmap ORDER BY dt, hour_of_day")
 .coalesce(1).write
 .mode("overwrite").option("header", True)
 .csv("/home/BaiMenghui/hudi-bigdata/export/ads/ads_hour_heatmap"))
print("  导出完成")

spark.stop()
print("ads_hour_heatmap 收藏维度补齐完成！")
