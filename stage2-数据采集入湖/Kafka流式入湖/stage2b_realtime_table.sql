-- ============================================================
-- Stage 2 补充: 流式入湖目标表建表脚本
-- 电商用户行为湖仓分析平台
-- 表名: ods_user_behavior_realtime (Hudi MOR, 与批量 ODS 表结构对齐)
-- 说明: 流式数据通过 Spark Structured Streaming 按此路径写入,
--       表结构/主键/预合并键与批量表 ods_user_behavior_log 完全一致
-- ============================================================

CREATE TABLE IF NOT EXISTS ods_user_behavior_realtime (
    behavior_id STRING COMMENT "行为记录唯一ID",
    user_id BIGINT COMMENT "用户ID",
    item_id BIGINT COMMENT "商品ID",
    category_id BIGINT COMMENT "商品类目ID",
    behavior_type STRING COMMENT "行为类型",
    ts BIGINT COMMENT "Unix时间戳",
    event_time TIMESTAMP COMMENT "行为发生时间",
    dt STRING COMMENT "日期分区"
)
USING hudi
PARTITIONED BY (dt)
LOCATION '/home/BaiMenghui/hudi-bigdata/warehouse/ods_user_behavior_realtime'
OPTIONS (
    type = "mor",
    primaryKey = "behavior_id",
    preCombineField = "ts",
    hoodie.table.name = "ods_user_behavior_realtime",
    hoodie.datasource.write.recordkey.field = "behavior_id",
    hoodie.datasource.write.partitionpath.field = "dt",
    hoodie.datasource.write.precombine.field = "ts",
    hoodie.datasource.write.table.type = "MERGE_ON_READ",
    hoodie.cleaner.policy = "KEEP_LATEST_COMMITS",
    hoodie.cleaner.commits.retained = 10,
    hoodie.compact.inline = "false"
);
