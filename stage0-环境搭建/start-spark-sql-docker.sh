#!/bin/bash
# ============================================
# Docker 方式启动 Spark SQL（集成 Hudi）
# 用法：./start-spark-sql-docker.sh
# ============================================

# 配置参数
HIVE_METASTORE_URI="thrift://hive-metastore:9083"
WAREHOUSE_DIR="/tmp/spark-warehouse"
HUDI_VERSION="0.14.1"
SPARK_VERSION="3.3"
SCALA_VERSION="2.12"
HUDI_BUNDLE="hudi-spark${SPARK_VERSION}-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"

echo "============================================"
echo "  Docker 方式启动 Spark SQL (集成 Hudi)"
echo "============================================"
echo "Spark: apache/spark:${SPARK_VERSION}.0"
echo "Hudi Bundle: ${HUDI_BUNDLE}"
echo "Hive Metastore: ${HIVE_METASTORE_URI}"
echo "============================================"

# 检查 Hudi jar 是否存在，如果不存在则下载
if [ ! -f "$(dirname $0)/../jars/${HUDI_BUNDLE}" ]; then
    echo "[下载] 正在下载 Hudi Bundle..."
    mkdir -p $(dirname $0)/../jars
    wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark${SPARK_VERSION}-bundle_${SCALA_VERSION}/${HUDI_VERSION}/${HUDI_BUNDLE} \
        -O $(dirname $0)/../jars/${HUDI_BUNDLE}
    if [ $? -eq 0 ]; then
        echo "[OK] Hudi Bundle 下载完成"
    else
        echo "[错误] Hudi Bundle 下载失败，请手动下载后放到 jars/ 目录"
        echo "下载地址: https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark${SPARK_VERSION}-bundle_${SCALA_VERSION}/${HUDI_VERSION}/${HUDI_BUNDLE}"
        exit 1
    fi
fi

echo ""
echo "正在启动 Spark SQL..."
echo "提示: 建表语句请参考 sql/hudi_basic_verification.sql"
echo "============================================"

# 用 Docker 启动 Spark SQL
docker run -it --rm \
    --name spark-sql-client \
    --network bigdata-net \
    -v $(dirname $0)/../jars/${HUDI_BUNDLE}:/opt/spark/jars-hudi/${HUDI_BUNDLE} \
    -v $(dirname $0)/../sql:/opt/sql \
    -v spark-warehouse:/opt/spark/warehouse \
    -e HIVE_METASTORE_URI=${HIVE_METASTORE_URI} \
    apache/spark:${SPARK_VERSION}.0 \
    /opt/spark/bin/spark-sql \
    --master local[2] \
    --jars /opt/spark/jars-hudi/${HUDI_BUNDLE} \
    --conf spark.sql.warehouse.dir=${WAREHOUSE_DIR} \
    --conf spark.hadoop.hive.metastore.uris=${HIVE_METASTORE_URI} \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
