# -*- coding: utf-8 -*-
"""
Spark Structured Streaming 消费端 —— 电商用户行为湖仓分析平台
功能: 消费 Kafka topic(user_behavior_stream) 中的用户行为 JSON 流,
      解析后按微批(foreachBatch)以 upsert 方式写入 Hudi MOR 表
      ods_user_behavior_realtime, 实现准实时增量入湖。
      主键/预合并键/分区与批量 ODS 表 ods_user_behavior_log 完全对齐。

运行(必须在 ~/hudi-bigdata 目录下, 以复用 Derby metastore):
    cd ~/hudi-bigdata && bash scripts/run_streaming_ingestion.sh
"""

# ============================================================
# 脚本名称: streaming_to_hudi.py
# 所属阶段: 阶段2 —— 流式入湖(加分项核心)
# 功能说明: Spark Structured Streaming 消费 Kafka 中的用户行为 JSON
#           流, 每个微批(30 秒)经 foreachBatch 解析为结构化列并派生
#           主键 behavior_id, 再以 upsert 写入 Hudi MOR 表
#           ods_user_behavior_realtime; 主键/预合并字段 ts/分区字段 dt
#           与批量 ODS 表完全对齐, 重复数据按 ts 预合并天然幂等;
#           checkpoint 记录消费位点, 作业重启后断点续传保证不丢不重
# 输入    : Kafka topic user_behavior_stream (localhost:9092)
# 输出    : Hudi MOR 表 /home/BaiMenghui/hudi-bigdata/warehouse/ods_user_behavior_realtime
#           checkpoint: /home/BaiMenghui/hudi-bigdata/checkpoints/ods_user_behavior_realtime_ck
# 运行方式: wsl bash -c "cd ~/hudi-bigdata && bash scripts/run_streaming_ingestion.sh"
# ============================================================

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# types 中显式声明消息各字段的类型, 供 from_json 严格解析
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
)

# 连接与路径常量: checkpoint 位置固定在独立目录 —— 作业重启后从上次
# 消费位点(offset)续读, 保证不丢不重; 表路径放在 warehouse 下与批量表并列
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "user_behavior_stream"
WAREHOUSE_DIR = "/home/BaiMenghui/hudi-bigdata/warehouse"
TABLE_PATH = WAREHOUSE_DIR + "/ods_user_behavior_realtime"
CHECKPOINT_DIR = "/home/BaiMenghui/hudi-bigdata/checkpoints/ods_user_behavior_realtime_ck"

# 与 Kafka 生产端 JSON 字段一一对应
# 消息 schema: 与生产端 JSON 字段一一对应, from_json 按此严格解析类型
# (ID/时间戳用长整型, 行为类型/事件时间/日期用字符串, 小时用整型)
MESSAGE_SCHEMA = StructType([
    StructField("user_id", LongType(), False),
    StructField("item_id", LongType(), False),
    StructField("category_id", LongType(), False),
    StructField("behavior_type", StringType(), False),
    StructField("ts", LongType(), False),
    StructField("event_time", StringType(), False),
    StructField("dt", StringType(), False),
    StructField("event_hour", IntegerType(), False),
])


# 构建 Spark 会话: 挂载 Hudi 的 catalog 与 SQL 扩展, 使 DataFrame API
# 能读写 Hudi 表; Kryo 序列化是 Hudi 官方要求; catalogImplementation=hive
# 复用 Hive metastore(Derby, 故必须先 cd ~/hudi-bigdata 再启动);
# session.timeZone 统一为东八区, 保证时间解析与批量链路口径一致
def build_spark_session():
    return (
        SparkSession.builder
        .appName("streaming_user_behavior_to_hudi")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .getOrCreate()
    )


def transform_batch(batch_df, batch_id):
    """微批转换: 解析 JSON -> 派生主键 -> 类型对齐批量 ODS 表"""
    # ① 解析 JSON: value 列原始为 binary, 先 cast 成 string, 再按
    #    MESSAGE_SCHEMA 用 from_json 解析, data.* 展开成独立列
    parsed = (
        batch_df
        .select(
            F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("data")
        )
        .select("data.*")
        # 主键生成逻辑与批量 ODS 表保持一致:
        # CONCAT(user_id, "_", item_id, "_", ts, "_", behavior_type)
        # ② 派生主键 behavior_id = user_id_item_id_ts_behavior_type, 口径
        #    与批量 ODS 表完全一致, 保证流批两路同键数据可合并去重
        .withColumn(
            "behavior_id",
            F.concat_ws("_",
                        F.col("user_id").cast("string"),
                        F.col("item_id").cast("string"),
                        F.col("ts").cast("string"),
                        F.col("behavior_type")),
        )
        # ③ 类型修正与脏数据过滤: event_time 字符串转 timestamp 类型;
        #    剔除 dt 为空/空串的记录, 防止分区字段缺失导致写 Hudi 失败
        .withColumn("event_time", F.to_timestamp("event_time"))
        .where(F.col("dt").isNotNull() & (F.col("dt") != ""))
    )

    # 空批直接跳过: 避免向 Hudi 提交空 commit, 保持时间线干净
    count = parsed.count()
    if count == 0:
        print("[微批 %d] 空批, 跳过写入" % batch_id)
        return

    # Hudi 写入配置: MOR + upsert, 重复数据按 ts 预合并去重, 天然幂等
    # 写入 Hudi 的关键配置逐项说明:
    #   recordkey.field + SimpleKeyGenerator: 以 behavior_id 生成 recordKey,
    #     dt 作为分区路径, 与批量 ODS 表主键/分区口径一致
    #   precombine.field=ts: 同一主键重复到达时保留 ts 更大(更新)的记录
    #     —— Kafka 重复投递或 checkpoint 重放都不会产生重复数据, 幂等
    #   table.type=MERGE_ON_READ: 写入只追加 Log/Base 文件、读时合并,
    #     写入代价小, 适合高频微批的准实时入湖场景
    #   operation=upsert: 主键已存在则更新, 不存在则插入
    #   hive_sync=false: 流式侧按表路径直接写入、不注册 metastore,
    #     校验脚本同样按路径读表
    #   mode=append: 数据追加进表, 去重由 upsert + 预合并语义保证
    (
        parsed.write
        .format("hudi")
        .option("hoodie.table.name", "ods_user_behavior_realtime")
        .option("hoodie.datasource.write.recordkey.field", "behavior_id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .option("hoodie.datasource.write.partitionpath.field", "dt")
        .option("hoodie.datasource.write.keygenerator.class",
                "org.apache.hudi.keygen.SimpleKeyGenerator")
        .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
        .option("hoodie.datasource.write.operation", "upsert")
        .option("hoodie.datasource.hive_sync.enable", "false")
        .mode("append")
        .save(TABLE_PATH)
    )
    print("[微批 %d] 成功写入 Hudi: %d 条" % (batch_id, count))


# 主流程: 建 Spark 会话 -> 定义 Kafka 流源 -> foreachBatch 微批写 Hudi ->
#         阻塞等待流查询持续运行
def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 定义 Kafka 流式数据源: 订阅 topic; startingOffsets=earliest 使首次
    # 启动从最早 offset 消费(之后由 checkpoint 接管续读); failOnDataLoss=
    # false 防止数据过期被清理时作业报错中断; maxOffsetsPerTrigger=50000
    # 限制单批最多拉取 5 万条, 避免首次消费积压数据时微批过大
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
    )

    # 启动流式查询: 每个微批交给 transform_batch 写入 Hudi;
    # checkpointLocation 固定 —— 消费位点与批次状态落盘, 作业重启后
    # 自动从上次 offset 续读, 保证不丢不重; 每 30 秒触发一次微批
    query = (
        raw_stream.writeStream
        .foreachBatch(transform_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="30 seconds")
        .outputMode("update")
        .start()
    )

    print("==========================================")
    print("Structured Streaming 已启动, 消费 topic: %s" % TOPIC)
    print("Hudi 目标表路径: %s" % TABLE_PATH)
    print("Checkpoint: %s" % CHECKPOINT_DIR)
    print("微批触发间隔: 30 秒 (Ctrl+C 退出)")
    print("==========================================")

    # 阻塞等待流查询持续运行; Ctrl+C 触发 KeyboardInterrupt 时优雅停机
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        print("流式查询已停止")
        sys.exit(0)


if __name__ == "__main__":
    main()
