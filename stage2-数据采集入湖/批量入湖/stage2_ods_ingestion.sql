-- ============================================================
-- Stage 2: ODS层数据入湖脚本
-- 电商用户行为湖仓分析平台
-- 表名: ods_user_behavior_log (Hudi MOR)
-- ============================================================

-- ============================================================
-- 步骤1: 创建 ODS 层 Hudi MOR 表
-- ============================================================
CREATE TABLE IF NOT EXISTS ods_user_behavior_log (
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
OPTIONS (
    type = "mor",
    primaryKey = "behavior_id",
    preCombineField = "ts",
    hoodie.table.name = "ods_user_behavior_log",
    hoodie.datasource.write.recordkey.field = "behavior_id",
    hoodie.datasource.write.partitionpath.field = "dt",
    hoodie.datasource.write.precombine.field = "ts",
    hoodie.datasource.write.operation = "insert",
    hoodie.datasource.write.table.type = "MERGE_ON_READ",
    hoodie.cleaner.policy = "KEEP_LATEST_COMMITS",
    hoodie.cleaner.commits.retained = 10,
    hoodie.compact.inline = "false"
);

-- ============================================================
-- 步骤2: 创建临时视图读取清洗后的CSV数据
-- ============================================================
CREATE OR REPLACE TEMPORARY VIEW user_behavior_raw (
    user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior_type STRING,
    ts BIGINT,
    event_time TIMESTAMP,
    dt STRING,
    event_hour INT
)
USING csv
OPTIONS (
    path = "/home/BaiMenghui/hudi-bigdata/data/cleaned",
    header = "true",
    mode = "PERMISSIVE"
);

-- ============================================================
-- 步骤3: 将数据写入 Hudi ODS 表
-- ============================================================
INSERT INTO ods_user_behavior_log
SELECT
    CONCAT(CAST(user_id AS STRING), "_", CAST(item_id AS STRING), "_", CAST(ts AS STRING), "_", behavior_type) AS behavior_id,
    user_id,
    item_id,
    category_id,
    behavior_type,
    ts,
    event_time,
    dt
FROM user_behavior_raw
WHERE dt IS NOT NULL AND dt != "";
