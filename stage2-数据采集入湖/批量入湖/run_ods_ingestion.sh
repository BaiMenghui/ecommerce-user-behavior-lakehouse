#!/bin/bash
# ============================================================
# Stage 2: ODS层数据入湖启动脚本
# 电商用户行为湖仓分析平台
# ============================================================

set -e

BASE_DIR=~/hudi-bigdata
JAR_PATH=$BASE_DIR/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar
WAREHOUSE_DIR=$BASE_DIR/warehouse
SQL_DIR=$BASE_DIR/sql
LOG_DIR=$BASE_DIR/logs

mkdir -p $LOG_DIR

echo "=========================================="
echo "Stage 2: ODS层数据入湖"
echo "=========================================="
echo "开始时间: $(date "+%Y-%m-%d %H:%M:%S")"
echo ""

# 步骤1: 执行ODS层建表与数据入湖
echo "[1/2] 执行ODS层建表与数据入湖..."

~/.local/bin/spark-sql     --master local[2]     --jars $JAR_PATH     --conf spark.sql.warehouse.dir=$WAREHOUSE_DIR     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer     --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension     --conf spark.driver.memory=2g     --conf spark.executor.memory=2g     -f $SQL_DIR/stage2_ods_ingestion.sql     2>&1 | tee $LOG_DIR/stage2_ods_ingestion.log

echo "入湖执行完成"
echo ""

# 步骤2: 执行入湖校验
echo "[2/2] 执行入湖校验..."

~/.local/bin/spark-sql     --master local[2]     --jars $JAR_PATH     --conf spark.sql.warehouse.dir=$WAREHOUSE_DIR     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer     --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension     --conf spark.driver.memory=2g     --conf spark.executor.memory=2g     -f $SQL_DIR/stage2_ods_verification.sql     2>&1 | tee $LOG_DIR/stage2_ods_verification.log

echo ""
echo "=========================================="
echo "Stage 2: ODS层入湖完成"
echo "结束时间: $(date "+%Y-%m-%d %H:%M:%S")"
echo "日志文件: $LOG_DIR/"
echo "=========================================="
