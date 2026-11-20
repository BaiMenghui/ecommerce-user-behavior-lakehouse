-- ============================================
-- Hive Metastore 连通性验证
-- ============================================

-- 创建一个测试表（使用 Hive Metastore 管理元数据）
CREATE TABLE IF NOT EXISTS test_metastore_conn (
    id STRING,
    name STRING,
    value INT
) USING parquet;

-- 插入测试数据
INSERT INTO test_metastore_conn VALUES
    ('test001', 'hive_metastore_test', 100),
    ('test002', 'connection_verify', 200);

-- 查询验证
SELECT * FROM test_metastore_conn ORDER BY id;

-- 统计
SELECT COUNT(*) as total FROM test_metastore_conn;

-- 查看表信息
DESCRIBE FORMATTED test_metastore_conn;

-- 查看所有表
SHOW TABLES;

-- 清理测试表
DROP TABLE test_metastore_conn;

SELECT '=== Hive Metastore 连通性验证通过！✅ ===' as result;
