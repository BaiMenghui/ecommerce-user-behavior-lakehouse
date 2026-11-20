#!/bin/bash
# ============================================
# Hudi 验证脚本 - Spark SQL 方式
# ============================================

export PATH=~/.local/bin:$PATH
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

cd ~/hudi-bigdata

# 使用本地仓库目录，避免权限问题
WAREHOUSE_DIR=~/hudi-bigdata/warehouse
mkdir -p $WAREHOUSE_DIR

echo "============================================"
echo "  启动 Hudi 全流程验证 (Spark SQL 方式)"
echo "============================================"
echo "Hudi Bundle: ~/hudi-bigdata/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar"
echo "Warehouse: $WAREHOUSE_DIR"
echo "============================================"
echo ""

spark-sql \
    --master local[2] \
    --jars ~/hudi-bigdata/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar \
    --conf spark.sql.warehouse.dir=$WAREHOUSE_DIR \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    -f ~/hudi-bigdata/sql/hudi_verification_sql.sql \
    2>&1 | tee ~/hudi-bigdata/verification_output.log

echo ""
echo "============================================"
echo "  验证完成，输出已保存到 verification_output.log"
echo "============================================"
