#!/bin/bash
# ============================================================
# 阶段3 全量重跑脚本 —— 电商用户行为湖仓分析平台
# 串行执行 DIM / DWD / DWS 三层 ETL（upsert 幂等, 可重复执行）
# 用法: bash ~/hudi-bigdata/scripts/rerun_stage3.sh
# ============================================================

set -e

BASE_DIR=/home/BaiMenghui/hudi-bigdata
LOG_DIR=$BASE_DIR/logs

cd $BASE_DIR

run_spark() {
    ~/.local/bin/spark-submit \
        --master local[2] \
        --jars $BASE_DIR/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar \
        --conf spark.sql.warehouse.dir=$BASE_DIR/warehouse \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
        --conf spark.sql.catalogImplementation=hive \
        --conf spark.driver.memory=2g \
        "$1" 2>&1 | tee "$2"
}

echo "========== [1/3] DIM 层 ETL =========="
run_spark $BASE_DIR/scripts/stage3_dim_etl.py $LOG_DIR/rerun_stage3_dim.log
echo "DIM 层完成"

echo "========== [2/3] DWD 层 ETL =========="
run_spark $BASE_DIR/scripts/stage3_dwd_etl.py $LOG_DIR/rerun_stage3_dwd.log
echo "DWD 层完成"

echo "========== [3/3] DWS 层 ETL =========="
run_spark $BASE_DIR/scripts/stage3_dws_etl.py $LOG_DIR/rerun_stage3_dws.log
echo "DWS 层完成"

echo "========== 阶段3 全部完成 =========="
