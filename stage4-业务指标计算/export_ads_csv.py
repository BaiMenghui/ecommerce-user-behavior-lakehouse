# -*- coding: utf-8 -*-
# ============================================================
# 脚本名称: export_ads_csv.py
# 所属阶段: 阶段4 ADS 结果服务化导出（异常分区清理后的重导版本）
# 功能说明: 读取 5 张 ADS Hudi 指标表，剔除 dt=2018-08-28 时间戳越界异常分区后，
#           按日期排序、coalesce(1) 合并为单文件、带表头 overwrite 导出 CSV，
#           供 SpringBoot API 启动时快照载入（大屏服务层数据源）
# 输入: ads_dau_trend / ads_funnel / ads_item_topn / ads_category_share / ads_hour_heatmap
# 输出: /home/BaiMenghui/hudi-bigdata/export/ads/<table>/ 下单文件 CSV
# 运行方式: WSL 下 spark-submit --jars hudi-spark3.3-bundle_2.12-0.14.1.jar \
#           --conf spark.sql.catalogImplementation=hive export_ads_csv.py
# ============================================================
"""
ADS 结果 CSV 重导脚本 —— 电商用户行为湖仓分析平台
背景: 异常分区 dt=2018-08-28 已从湖仓剔除, 但此前导出的 CSV 文件仍含该分区,
      导致服务层 latestDt 口径错误。本脚本按湖仓当前数据(异常分区已清零)重导 5 份 CSV。
用法: cd ~/hudi-bigdata && ~/.local/bin/spark-submit \
        --master local[2] --jars jars/hudi-spark3.3-bundle_2.12-0.14.1.jar \
        --conf spark.sql.warehouse.dir=$HOME/hudi-bigdata/warehouse \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
        --conf spark.sql.catalogImplementation=hive \
        scripts/export_ads_csv.py
"""
from pyspark.sql import SparkSession

# 时间戳越界的异常分区（数据本身错乱），导出文件中必须剔除
OUTLIER_DT = "2018-08-28"
# CSV 导出根目录：SpringBoot API 启动时按 <EXPORT_BASE>/<表名>/ 快照载入
EXPORT_BASE = "/home/BaiMenghui/hudi-bigdata/export/ads"
TABLES = ["ads_dau_trend", "ads_funnel", "ads_item_topn",
          "ads_category_share", "ads_hour_heatmap"]

# catalogImplementation=hive：经 Hive metastore 定位 Hudi 表；
# 其余 Hudi 相关配置与建表/写入脚本保持一致，保证能正确读出 Hudi 表数据
spark = (
    SparkSession.builder
    .appName("export_ads_csv_clean")
    .config("spark.sql.warehouse.dir", "/home/BaiMenghui/hudi-bigdata/warehouse")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .config("spark.sql.catalogImplementation", "hive")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("重导 ADS 结果 CSV（剔除异常分区 %s）" % OUTLIER_DT)
print("=" * 60)

for t in TABLES:
    # ① 全表读取：湖仓侧异常分区已在修复阶段物理删除，此处读到的应为干净数据
    df = spark.sql("SELECT * FROM %s" % t)
    # 双保险: 过滤异常分区——即使湖仓残留该分区，导出文件也不含脏数据
    df = df.where("dt != '%s'" % OUTLIER_DT)
    # ② 统计有效行数（触发执行并打印留档，便于核对导出量）
    n = df.count()
    # ③ orderBy 按日期排序后 coalesce(1) 合并为单文件，带表头 overwrite 覆盖旧导出
    (df.orderBy("dt").coalesce(1).write
     .mode("overwrite").option("header", True)
     .csv("%s/%s" % (EXPORT_BASE, t)))
    print("  %s.csv 导出完成, %d 行" % (t, n))

spark.stop()
print("重导完成！")
