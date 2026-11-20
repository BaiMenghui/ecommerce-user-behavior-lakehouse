#!/bin/bash
# ============================================
# Spark SQL 启动脚本（集成 Hudi）
# 用法：./start-spark-sql.sh
# ============================================

# 配置参数
SPARK_MASTER="local[2]"
HIVE_METASTORE_URI="thrift://localhost:9083"
WAREHOUSE_DIR="/tmp/spark-warehouse"
HUDI_BUNDLE_JAR="$HOME/hudi-bigdata/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar"

echo "============================================"
echo "  启动 Spark SQL (集成 Hudi)"
echo "============================================"
echo "Master: $SPARK_MASTER"
echo "Hive Metastore: $HIVE_METASTORE_URI"
echo "Hudi Bundle: $HUDI_BUNDLE_JAR"
echo "============================================"

# 检查 Hudi jar 是否存在
if [ ! -f "$HUDI_BUNDLE_JAR" ]; then
    echo "[警告] 未找到 Hudi Bundle Jar: $HUDI_BUNDLE_JAR"
    echo "请先下载 Hudi Bundle 到 jars/ 目录"
    echo "下载地址: https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.3-bundle_2.12/0.14.1/hudi-spark3.3-bundle_2.12-0.14.1.jar"
    HUDI_JAR_OPTS=""
else
    HUDI_JAR_OPTS="--jars $HUDI_BUNDLE_JAR"
    echo "[OK] 找到 Hudi Bundle Jar"
fi

echo ""
echo "正在启动 Spark SQL..."
echo "提示: 启动后可以使用 source /path/to/sql 文件来执行脚本"
echo "============================================"

# 启动 Spark SQL
spark-sql \
    --master $SPARK_MASTER \
    $HUDI_JAR_OPTS \
    --conf spark.sql.warehouse.dir=$WAREHOUSE_DIR \
    --conf spark.hadoop.hive.metastore.uris=$HIVE_METASTORE_URI \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    --conf spark.sql.hive.convertMetastoreParquet=false
