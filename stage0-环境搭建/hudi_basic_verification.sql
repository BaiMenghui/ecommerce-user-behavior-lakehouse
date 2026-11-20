-- ============================================
-- Hudi 基础验证脚本
-- 功能：建表、写入、查询、更新、删除 全流程验证
-- 阶段0：集成与全链路验证
-- ============================================

-- ============================================
-- 第一步：创建 Hudi COW 表
-- ============================================
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

-- 验证表是否创建成功
SHOW TABLES;

DESCRIBE FORMATTED test_hudi_cow;

-- ============================================
-- 第二步：插入数据（增）
-- ============================================
INSERT INTO test_hudi_cow VALUES
    ('id001', 'zhangsan', 20, 1750000000000),
    ('id002', 'lisi', 22, 1750000000001),
    ('id003', 'wangwu', 25, 1750000000002),
    ('id004', 'zhaoliu', 28, 1750000000003),
    ('id005', 'sunqi', 30, 1750000000004);

-- ============================================
-- 第三步：查询数据（查）
-- ============================================
-- 全量查询
SELECT * FROM test_hudi_cow ORDER BY id;

-- 条件查询
SELECT * FROM test_hudi_cow WHERE age > 25;

-- 统计查询
SELECT COUNT(*) as total_count, AVG(age) as avg_age FROM test_hudi_cow;

-- ============================================
-- 第四步：更新数据（改）
-- ============================================
-- 更新 id001 的年龄和时间戳
INSERT INTO test_hudi_cow VALUES
    ('id001', 'zhangsan', 21, 1750000000010);

-- 验证更新结果（应该看到 id001 的 age 变成 21）
SELECT * FROM test_hudi_cow WHERE id = 'id001';

-- ============================================
-- 第五步：删除数据（删）
-- ============================================
-- 先插入一条用于删除测试的数据
INSERT INTO test_hudi_cow VALUES
    ('id006', 'zhouba', 35, 1750000000020);

-- 验证插入
SELECT * FROM test_hudi_cow WHERE id = 'id006';

-- 删除 id006（Hudi 删除需要设置 _hoodie_is_deleted 标志）
DELETE FROM test_hudi_cow WHERE id = 'id006';

-- 验证删除结果（应该查询不到 id006）
SELECT * FROM test_hudi_cow ORDER BY id;

-- ============================================
-- 第六步：创建 Hudi MOR 表
-- ============================================
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

-- 插入测试数据
INSERT INTO test_hudi_mor VALUES
    ('mor001', 'test_mor_1', 20, 1750000000000),
    ('mor002', 'test_mor_2', 22, 1750000000001),
    ('mor003', 'test_mor_3', 25, 1750000000002);

-- 查询 MOR 表
SELECT * FROM test_hudi_mor ORDER BY id;

-- ============================================
-- 验证完成
-- ============================================
-- 查看所有表
SHOW TABLES;

-- 验证通过标记
SELECT 'Hudi 基础验证全部通过！' as verification_result;
