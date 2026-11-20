# -*- coding: utf-8 -*-
# ============================================================
# 脚本名称: repair_ads_dataframe.py
# 所属阶段: 阶段5 修复脚本
# 功能说明: 从全部 5 张 ADS 表中物理删除 dt=2018-08-28（时间戳越界）异常分区的数据。
#           历史坑：spark-sql 的 DELETE 语句在数据 commit 后会触发 Hive metastore
#           schema 同步报错（HMS 中列类型与 Hudi schema 不兼容）导致语句中断；
#           本脚本改用 Spark DataFrame API 的 delete 写操作，并显式关闭 hive_sync
#           完全绕开 HMS；删除后回读校验残留必须为 0
# 输入: 5 张 ADS Hudi 表（按 warehouse 路径直接读取，不经 metastore）
# 输出: 清理异常分区后的 5 张 ADS 表；全部成功时控制台打印 ALL_ADS_DELETE_OK
# 运行方式: WSL 下 spark-submit --jars hudi-spark3.3-bundle_2.12-0.14.1.jar repair_ads_dataframe.py
# 幂等性: 目标分区无数据时跳过（SKIP），可重复执行
# ============================================================
"""
阶段5备选修复：spark-submit DataFrame 方式删除 ADS 层异常分区 dt='2018-08-28'
背景：spark-sql DELETE 在数据 commit 后触发 metastore schema 同步失败
      （HMS 中 order_cnt 列类型与 Hudi schema 不兼容）导致语句中断。
      本脚本用 DataFrame delete 操作，显式关闭 hive_sync，绕开 HMS。
幂等：目标分区无数据时跳过；删除后回读校验残留必须为 0。
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("stage5_repair_ads_delete") \
    .config("spark.sql.warehouse.dir", "/home/BaiMenghui/hudi-bigdata/warehouse") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 时间戳越界的异常分区（上游 DWD 混入的脏数据日期），需从全部 ADS 表剔除
OUTLIER_DT = "2018-08-28"
# Hudi 表根路径：故意按路径直读而不走 spark.sql 查表，避免触发 Hive metastore 交互
BASE = "/home/BaiMenghui/hudi-bigdata/warehouse"

# 表 -> 主键（与 stage4_ads_etl.py write_hudi 一致）
# delete 操作按主键定位记录；主键不是 dt 的表还须带上分区字段 dt，才能定位到异常分区
TABLES = {
    "ads_dau_trend": "dt",
    "ads_funnel": "dt",
    "ads_item_topn": "item_id",
    "ads_category_share": "category_id",
    "ads_hour_heatmap": "hour_of_day",
}

for table, pk in TABLES.items():
    # 直接读 Hudi 表路径得到 DataFrame（不经 metastore/hive sync，绕开 schema 同步报错）
    path = BASE + "/" + table
    base = spark.read.format("hudi").load(path)
    # 先统计异常分区行数：为 0 说明此前已清理过，跳过——保证脚本幂等可重跑
    before = base.filter(F.col("dt") == OUTLIER_DT).count()
    if before == 0:
        print("[SKIP] " + table + ": no rows in " + OUTLIER_DT)
        continue
    # delete 操作只需提供主键（+分区路径字段）：主键本身是 dt 的表无需再单带 dt 列；
    # 待删集 = 异常分区过滤出的 (主键, dt) 记录，Hudi 据此经索引定位并物理删除文件内对应行
    cols = [pk] if pk == "dt" else [pk, "dt"]
    deletes = base.filter(F.col("dt") == OUTLIER_DT).select(*cols)
    # 关键点：operation=delete 执行物理删除；hive_sync 显式关闭，
    # 绕开 spark-sql DELETE 会触发的 metastore schema 同步报错（本脚本存在的意义）
    deletes.write.format("hudi") \
        .option("hoodie.table.name", table) \
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
        .option("hoodie.datasource.write.operation", "delete") \
        .option("hoodie.datasource.write.recordkey.field", pk) \
        .option("hoodie.datasource.write.partitionpath.field", "dt") \
        .option("hoodie.datasource.write.precombine.field", "dt") \
        .option("hoodie.datasource.hive_sync.enable", "false") \
        .mode("append") \
        .save(path)
    # 删除后重新读表回验：异常分区残留必须为 0，非 0 则以退出码 1 报警终止
    after = spark.read.format("hudi").load(path).filter(F.col("dt") == OUTLIER_DT).count()
    print("[DONE] " + table + ": deleted " + str(before) + " rows, remain " + str(after))
    if after != 0:
        print("[FAIL] " + table + " still has rows in " + OUTLIER_DT)
        sys.exit(1)

print("ALL_ADS_DELETE_OK")
spark.stop()
