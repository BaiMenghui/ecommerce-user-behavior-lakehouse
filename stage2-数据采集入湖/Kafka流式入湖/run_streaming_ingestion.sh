#!/bin/bash
# ============================================================
# 流式入湖启动脚本 —— 电商用户行为湖仓分析平台
# 功能: Spark Structured Streaming 消费 Kafka, 增量写入 Hudi 表
#       ods_user_behavior_realtime
# 注意: 必须在 ~/hudi-bigdata 目录下运行(复用 Derby metastore),
#       且需先执行 start_kafka.sh 与建表脚本 stage2b_realtime_table.sql
# ============================================================

set -e

BASE_DIR=/home/BaiMenghui/hudi-bigdata
SPARK_SUBMIT=/home/BaiMenghui/.local/bin/spark-submit
HUDI_JAR=$BASE_DIR/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar
LOG_DIR=$BASE_DIR/logs

mkdir -p $LOG_DIR $BASE_DIR/checkpoints

cd $BASE_DIR

echo "=========================================="
echo "启动 Structured Streaming 流式入湖"
echo "开始时间: $(date "+%Y-%m-%d %H:%M:%S")"
echo "工作目录: $BASE_DIR"
echo "=========================================="

$SPARK_SUBMIT \
    --master local[2] \
    --jars $HUDI_JAR \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    --repositories https://maven.aliyun.com/repository/public \
    --conf spark.sql.warehouse.dir=$BASE_DIR/warehouse \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.driver.memory=2g \
    --conf spark.executor.memory=2g \
    scripts/streaming_to_hudi.py \
    2>&1 | tee $LOG_DIR/stage2b_streaming_ingestion.log
