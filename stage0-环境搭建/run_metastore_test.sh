#!/bin/bash
export PATH=~/.local/bin:$PATH
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

cd ~/hudi-bigdata

echo "============================================"
echo "  Hive Metastore 连通性验证"
echo "============================================"

spark-sql \
    --master local[2] \
    --jars ~/hudi-bigdata/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar \
    --conf spark.sql.warehouse.dir=~/hudi-bigdata/warehouse \
    --conf spark.hadoop.hive.metastore.uris=thrift://localhost:9083 \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    -f ~/hudi-bigdata/sql/hive_metastore_test.sql \
    2>&1 | tee ~/hudi-bigdata/metastore_test_output.log

echo ""
echo "============================================"
echo "  验证完成"
echo "============================================"
