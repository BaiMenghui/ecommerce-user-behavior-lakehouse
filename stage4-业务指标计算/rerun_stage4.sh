#!/bin/bash
# ============================================================
# 阶段4 全量重跑脚本 —— 电商用户行为湖仓分析平台
# 全量回刷 ADS 五表(24 有效分区, insert_overwrite 幂等) + 导出 5 份 CSV
# 用法: bash ~/hudi-bigdata/scripts/rerun_stage4.sh
# ============================================================

set -e

BASE_DIR=/home/BaiMenghui/hudi-bigdata
LOG_DIR=$BASE_DIR/logs

cd $BASE_DIR

echo "========== [1/2] ADS 层 ETL 全量回刷 =========="
~/.local/bin/spark-submit \
    --master local[2] \
    --jars $BASE_DIR/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar \
    --conf spark.sql.warehouse.dir=$BASE_DIR/warehouse \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.driver.memory=2g \
    $BASE_DIR/scripts/stage4_ads_etl.py 2>&1 | tee $LOG_DIR/rerun_stage4_ads.log

echo "========== [2/2] ADS 校验(示例分区 2017-11-25) =========="
~/.local/bin/spark-submit \
    --master local[2] \
    --jars $BASE_DIR/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar \
    --conf spark.sql.warehouse.dir=$BASE_DIR/warehouse \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.driver.memory=2g \
    $BASE_DIR/scripts/stage4_ads_check.py --dt 2017-11-25 2>&1 | tee $LOG_DIR/rerun_stage4_check.log

echo "========== 阶段4 全部完成 =========="
