#!/bin/bash
# ============================================
# Hudi 验证脚本 - 启动器
# ============================================

export PATH=~/.local/bin:$PATH
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

cd ~/hudi-bigdata

echo "============================================"
echo "  启动 Hudi 全流程验证"
echo "============================================"
echo "Spark: $(spark-submit --version 2>&1 | head -2)"
echo "Hudi Bundle: ~/hudi-bigdata/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar"
echo "Hive Metastore: thrift://localhost:9083"
echo "============================================"
echo ""

spark-submit \
    --master local[2] \
    --jars ~/hudi-bigdata/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar \
    --conf spark.sql.warehouse.dir=/tmp/spark-warehouse \
    --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083 \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    scripts/hudi_verification.py 2>&1 | tee ~/hudi-bigdata/verification_output.log

echo ""
echo "============================================"
echo "  验证完成，输出已保存到 verification_output.log"
echo "============================================"
