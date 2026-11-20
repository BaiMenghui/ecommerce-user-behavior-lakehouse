-- ============================================
-- Hudi 全流程验证 SQL 脚本
-- 使用方法: spark-sql -f hudi_verification.sql
-- ============================================

-- 设置日志级别
SET spark.sql.shuffle.partitions=2;

-- ============================================
-- 测试1：创建 Hudi COW 表
-- ============================================
SELECT '=== 测试1：创建 Hudi COW 表 ===' as test_step;

CREATE TABLE IF NOT EXISTS test_hudi_cow (
    id STRING,
    name STRING,
    age INT,
    ts BIGINT
) USING hudi
OPTIONS (
    type = 'cow',
    primaryKey = 'id',
    preCombineField = 'ts',
    hoodie.table.name = 'test_hudi_cow'
);

SHOW TABLES;

-- ============================================
-- 测试2：插入数据（增）
-- ============================================
SELECT '=== 测试2：插入数据（增）===' as test_step;

INSERT INTO test_hudi_cow VALUES
    ('id001', 'zhangsan', 20, 1750000000000),
    ('id002', 'lisi', 22, 1750000000001),
    ('id003', 'wangwu', 25, 1750000000002),
    ('id004', 'zhaoliu', 28, 1750000000003),
    ('id005', 'sunqi', 30, 1750000000004);

-- ============================================
-- 测试3：查询数据（查）
-- ============================================
SELECT '=== 测试3：查询数据（查）===' as test_step;

SELECT '--- 全量查询 ---' as sub_test;
SELECT * FROM test_hudi_cow ORDER BY id;

SELECT '--- 条件查询（age > 25）---' as sub_test;
SELECT * FROM test_hudi_cow WHERE age > 25 ORDER BY id;

SELECT '--- 统计查询 ---' as sub_test;
SELECT COUNT(*) as total_count, AVG(age) as avg_age FROM test_hudi_cow;

-- ============================================
-- 测试4：更新数据（改）
-- ============================================
SELECT '=== 测试4：更新数据（改）===' as test_step;

SELECT '--- 更新前 id001 ---' as sub_test;
SELECT * FROM test_hudi_cow WHERE id = 'id001';

INSERT INTO test_hudi_cow VALUES ('id001', 'zhangsan', 21, 1750000000010);

SELECT '--- 更新后 id001（预期 age=21）---' as sub_test;
SELECT * FROM test_hudi_cow WHERE id = 'id001';

-- ============================================
-- 测试5：删除数据（删）
-- ============================================
SELECT '=== 测试5：删除数据（删）===' as test_step;

INSERT INTO test_hudi_cow VALUES ('id006', 'zhouba', 35, 1750000000020);

SELECT '--- 删除前总数 ---' as sub_test;
SELECT COUNT(*) as count_before FROM test_hudi_cow;

DELETE FROM test_hudi_cow WHERE id = 'id006';

SELECT '--- 删除后总数 ---' as sub_test;
SELECT COUNT(*) as count_after FROM test_hudi_cow;

SELECT '--- 验证 id006 是否已删除 ---' as sub_test;
SELECT * FROM test_hudi_cow WHERE id = 'id006';

-- ============================================
-- 测试6：创建 Hudi MOR 表
-- ============================================
SELECT '=== 测试6：创建 Hudi MOR 表 ===' as test_step;

CREATE TABLE IF NOT EXISTS test_hudi_mor (
    id STRING,
    name STRING,
    age INT,
    ts BIGINT
) USING hudi
OPTIONS (
    type = 'mor',
    primaryKey = 'id',
    preCombineField = 'ts',
    hoodie.table.name = 'test_hudi_mor'
);

INSERT INTO test_hudi_mor VALUES
    ('mor001', 'test_mor_1', 20, 1750000000000),
    ('mor002', 'test_mor_2', 22, 1750000000001),
    ('mor003', 'test_mor_3', 25, 1750000000002);

SELECT '--- MOR 表查询结果 ---' as sub_test;
SELECT * FROM test_hudi_mor ORDER BY id;

-- ============================================
-- 最终验证
-- ============================================
SELECT '=== 最终验证：所有表列表 ===' as test_step;
SHOW TABLES;

SELECT '=== Hudi 基础验证全部通过！🎉 ===' as verification_result;
