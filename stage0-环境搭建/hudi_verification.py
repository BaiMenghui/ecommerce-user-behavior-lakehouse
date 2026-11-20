# ============================================
# Hudi 全流程自动化验证脚本
# 功能：建表、增、删、改、查 全流程验证
# 阶段0：集成与全链路验证
# ============================================

import sys
from pyspark.sql import SparkSession

# 计数器
passed = 0
failed = 0
results = []

def log_test(test_name, success, detail=""):
    global passed, failed, results
    status = "✅ PASS" if success else "❌ FAIL"
    if success:
        passed += 1
    else:
        failed += 1
    result = f"[{status}] {test_name}"
    if detail:
        result += f" - {detail}"
    results.append(result)
    print(result)

def main():
    print("=" * 60)
    print("  Hudi 全流程自动化验证")
    print("=" * 60)

    # 创建 SparkSession
    print("\n[INFO] 正在创建 SparkSession...")
    try:
        spark = SparkSession.builder \
            .appName("HudiBasicVerification") \
            .master("local[2]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
            .enableHiveSupport() \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        log_test("SparkSession 创建", True)
    except Exception as e:
        log_test("SparkSession 创建", False, str(e))
        print("\n" + "=" * 60)
        print(f"  验证结果: {passed} 通过, {failed} 失败")
        print("=" * 60)
        sys.exit(1)

    # ============================================
    # 测试1：创建 Hudi COW 表
    # ============================================
    print("\n--- 测试1：创建 Hudi COW 表 ---")
    try:
        spark.sql("""
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
            )
        """)
        log_test("创建 Hudi COW 表", True)
    except Exception as e:
        log_test("创建 Hudi COW 表", False, str(e))

    # 验证表存在
    try:
        tables = spark.sql("SHOW TABLES").collect()
        table_names = [row.tableName for row in tables]
        if "test_hudi_cow" in table_names:
            log_test("验证 COW 表存在", True, f"表列表: {table_names}")
        else:
            log_test("验证 COW 表存在", False, f"表列表: {table_names}")
    except Exception as e:
        log_test("验证 COW 表存在", False, str(e))

    # ============================================
    # 测试2：插入数据（增）
    # ============================================
    print("\n--- 测试2：插入数据（增）---")
    try:
        spark.sql("""
            INSERT INTO test_hudi_cow VALUES
                ('id001', 'zhangsan', 20, 1750000000000),
                ('id002', 'lisi', 22, 1750000000001),
                ('id003', 'wangwu', 25, 1750000000002),
                ('id004', 'zhaoliu', 28, 1750000000003),
                ('id005', 'sunqi', 30, 1750000000004)
        """)
        log_test("插入5条数据", True)
    except Exception as e:
        log_test("插入5条数据", False, str(e))

    # ============================================
    # 测试3：查询数据（查）
    # ============================================
    print("\n--- 测试3：查询数据（查）---")
    try:
        count = spark.sql("SELECT * FROM test_hudi_cow").count()
        log_test("全量查询", True, f"共 {count} 条数据")
    except Exception as e:
        log_test("全量查询", False, str(e))

    try:
        result = spark.sql("SELECT * FROM test_hudi_cow WHERE age > 25").count()
        log_test("条件查询（age > 25）", True, f"共 {result} 条数据")
    except Exception as e:
        log_test("条件查询", False, str(e))

    try:
        result = spark.sql("SELECT COUNT(*) as cnt, AVG(age) as avg_age FROM test_hudi_cow").collect()[0]
        log_test("统计查询", True, f"总数={result['cnt']}, 平均年龄={result['avg_age']}")
    except Exception as e:
        log_test("统计查询", False, str(e))

    # ============================================
    # 测试4：更新数据（改）
    # ============================================
    print("\n--- 测试4：更新数据（改）---")
    try:
        # 获取更新前的年龄
        old_age = spark.sql("SELECT age FROM test_hudi_cow WHERE id = 'id001'").collect()[0]['age']

        # 执行更新（相同主键再次插入即为更新）
        spark.sql("""
            INSERT INTO test_hudi_cow VALUES
                ('id001', 'zhangsan', 21, 1750000000010)
        """)

        # 获取更新后的年龄
        new_age = spark.sql("SELECT age FROM test_hudi_cow WHERE id = 'id001'").collect()[0]['age']

        if old_age == 20 and new_age == 21:
            log_test("数据更新", True, f"id001 年龄 {old_age} -> {new_age}")
        else:
            log_test("数据更新", False, f"预期 20->21, 实际 {old_age}->{new_age}")
    except Exception as e:
        log_test("数据更新", False, str(e))

    # ============================================
    # 测试5：删除数据（删）
    # ============================================
    print("\n--- 测试5：删除数据（删）---")
    try:
        # 先插入一条待删除的数据
        spark.sql("""
            INSERT INTO test_hudi_cow VALUES
                ('id006', 'zhouba', 35, 1750000000020)
        """)
        before_count = spark.sql("SELECT * FROM test_hudi_cow").count()

        # 执行删除
        spark.sql("DELETE FROM test_hudi_cow WHERE id = 'id006'")

        after_count = spark.sql("SELECT * FROM test_hudi_cow").count()
        id006_count = spark.sql("SELECT * FROM test_hudi_cow WHERE id = 'id006'").count()

        if after_count == before_count - 1 and id006_count == 0:
            log_test("数据删除", True, f"删除前 {before_count} 条, 删除后 {after_count} 条")
        else:
            log_test("数据删除", False, f"删除前 {before_count} 条, 删除后 {after_count} 条, id006剩余 {id006_count} 条")
    except Exception as e:
        log_test("数据删除", False, str(e))

    # ============================================
    # 测试6：创建 Hudi MOR 表
    # ============================================
    print("\n--- 测试6：创建 Hudi MOR 表 ---")
    try:
        spark.sql("""
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
            )
        """)
        log_test("创建 Hudi MOR 表", True)
    except Exception as e:
        log_test("创建 Hudi MOR 表", False, str(e))

    try:
        spark.sql("""
            INSERT INTO test_hudi_mor VALUES
                ('mor001', 'test_mor_1', 20, 1750000000000),
                ('mor002', 'test_mor_2', 22, 1750000000001),
                ('mor003', 'test_mor_3', 25, 1750000000002)
        """)
        count = spark.sql("SELECT * FROM test_hudi_mor").count()
        log_test("MOR 表写入与查询", True, f"共 {count} 条数据")
    except Exception as e:
        log_test("MOR 表写入与查询", False, str(e))

    # ============================================
    # 测试7：Hive Metastore 连通性
    # ============================================
    print("\n--- 测试7：Hive Metastore 连通性 ---")
    try:
        tables = spark.sql("SHOW TABLES").collect()
        table_list = [row.tableName for row in tables]
        log_test("Hive Metastore 表列表查询", True, f"表数量: {len(table_list)}, 表名: {table_list}")
    except Exception as e:
        log_test("Hive Metastore 连通性", False, str(e))

    # ============================================
    # 最终结果
    # ============================================
    print("\n" + "=" * 60)
    print("  验证结果汇总")
    print("=" * 60)
    for r in results:
        print(r)
    print("-" * 60)
    print(f"总计: {passed + failed} 项测试, ✅ {passed} 通过, ❌ {failed} 失败")
    print("=" * 60)

    if failed == 0:
        print("\n🎉 恭喜！所有 Hudi 基础验证全部通过！")
        print("阶段0 - 集成与全链路验证：完成 ✅")
    else:
        print(f"\n⚠️ 有 {failed} 项测试失败，请检查上述错误信息。")

    spark.stop()
    return failed == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
