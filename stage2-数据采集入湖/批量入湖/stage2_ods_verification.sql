-- ============================================================
-- Stage 2: ODS层入湖验证脚本
-- 电商用户行为湖仓分析平台
-- ============================================================

-- 验证1: 总记录数
SELECT COUNT(*) AS ods_total_records FROM ods_user_behavior_log;

-- 验证2: 分区列表
SHOW PARTITIONS ods_user_behavior_log;

-- 验证3: 各分区记录数
SELECT dt, COUNT(*) AS record_count
FROM ods_user_behavior_log
GROUP BY dt
ORDER BY dt;

-- 验证4: 行为类型分布
SELECT behavior_type,
       COUNT(*) AS record_count,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ods_user_behavior_log), 2) AS percentage
FROM ods_user_behavior_log
GROUP BY behavior_type
ORDER BY record_count DESC;

-- 验证5: 数据样例(前10条)
SELECT behavior_id, user_id, item_id, category_id, behavior_type, ts, event_time, dt
FROM ods_user_behavior_log
ORDER BY ts
LIMIT 10;

-- 验证6: 字段完整性
SELECT
    COUNT(*) - COUNT(behavior_id) AS missing_behavior_id,
    COUNT(*) - COUNT(user_id) AS missing_user_id,
    COUNT(*) - COUNT(item_id) AS missing_item_id,
    COUNT(*) - COUNT(category_id) AS missing_category_id,
    COUNT(*) - COUNT(behavior_type) AS missing_behavior_type,
    COUNT(*) - COUNT(ts) AS missing_ts,
    COUNT(*) - COUNT(event_time) AS missing_event_time,
    COUNT(*) - COUNT(dt) AS missing_dt
FROM ods_user_behavior_log;

-- 验证7: 用户/商品/类目规模
SELECT
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT item_id) AS unique_items,
    COUNT(DISTINCT category_id) AS unique_categories
FROM ods_user_behavior_log;

-- 验证8: 时间范围
SELECT
    MIN(event_time) AS min_event_time,
    MAX(event_time) AS max_event_time,
    DATEDIFF(MAX(event_time), MIN(event_time)) AS days_span
FROM ods_user_behavior_log;
