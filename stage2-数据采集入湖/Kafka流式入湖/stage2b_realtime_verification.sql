-- ============================================================
-- Stage 2 补充: 流式入湖验证脚本
-- 表名: ods_user_behavior_realtime (Hudi MOR)
-- ============================================================

-- 1. 总条数核对
SELECT 'total_count' AS metric, COUNT(*) AS value
FROM ods_user_behavior_realtime;

-- 2. 分区分布
SELECT dt, COUNT(*) AS cnt
FROM ods_user_behavior_realtime
GROUP BY dt;

-- 3. 行为类型分布
SELECT behavior_type, COUNT(*) AS cnt
FROM ods_user_behavior_realtime
GROUP BY behavior_type
ORDER BY cnt DESC;

-- 4. 时间范围核对(应为 2017-12-03 00:00:00 ~ 16:00 之前)
SELECT MIN(event_time) AS min_time, MAX(event_time) AS max_time
FROM ods_user_behavior_realtime;

-- 5. 主键唯一性核对(重复数应为 0)
SELECT SUM(dup) AS duplicate_count
FROM (
    SELECT behavior_id, COUNT(*) AS dup
    FROM ods_user_behavior_realtime
    GROUP BY behavior_id
    HAVING COUNT(*) > 1
);
