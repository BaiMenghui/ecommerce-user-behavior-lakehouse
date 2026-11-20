# -*- coding: utf-8 -*-
"""
流式入湖验证脚本(纯 DataFrame API, 不依赖 Hive metastore) —— 电商用户行为湖仓分析平台
功能:
  1. 表总条数 / 分区分布 / 行为类型分布 / 时间范围 / 主键唯一性
  2. Hudi 增量查询验证: 读取第 3 个 commit 之后的增量, 应等于微批 4 条数(13951)
用法:
  cd ~/hudi-bigdata && ~/.local/bin/spark-submit --master local[2] \
      --jars jars/hudi-spark3.3-bundle_2.12-0.14.1.jar \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      scripts/hudi_realtime_verify.py
"""

# ============================================================
# 脚本名称: hudi_realtime_verify.py
# 所属阶段: 阶段2 —— 流式入湖结果校验
# 功能说明: 以纯 DataFrame API 读取流式写入的 Hudi MOR 表(不依赖
#           Hive metastore), 六项校验: 1)表总条数 2)dt 分区分布
#           3)行为类型分布 4)事件时间范围 5)主键 behavior_id 唯一性
#           6)Hudi 增量查询 —— 第 3 个 commit 之后的增量条数应等于
#           微批 4 的 13951 条; 用"总量/唯一性/增量"交叉验证流式
#           入湖的正确性, 并顺带演示 Hudi 的增量读取能力
# 输入    : Hudi 表 /home/BaiMenghui/hudi-bigdata/warehouse/ods_user_behavior_realtime
# 输出    : 控制台校验报告(不落盘)
# 运行方式: wsl bash -c "cd ~/hudi-bigdata && ~/.local/bin/spark-submit --master local[2] --jars jars/hudi-spark3.3-bundle_2.12-0.14.1.jar --conf spark.serializer=org.apache.spark.serializer.KryoSerializer scripts/hudi_realtime_verify.py"
# ============================================================
from pyspark.sql import SparkSession

# Hudi 实时表路径(与流式写入端的 TABLE_PATH 一致)
TABLE_PATH = "/home/BaiMenghui/hudi-bigdata/warehouse/ods_user_behavior_realtime"
# Timeline 第 3 个 commit(即微批 3 完成时刻), 之后增量应包含微批 4 全部数据
BEGIN_INSTANT = "20261031111330614"


def main():
    # 建会话: Kryo 序列化为 Hudi 官方要求; 时区与入湖脚本一致(东八区),
    # 保证时间字段读取不偏移; 日志降到 ERROR 保持输出整洁
    spark = (
        SparkSession.builder
        .appName("hudi_realtime_verify")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # 快照读整张 Hudi 表(默认 query type=snapshot, 读取最新合并视图,
    # 包含 MOR 表基文件与日志文件的合并结果)
    df = spark.read.format("hudi").load(TABLE_PATH)

    # 验证1: 表总条数 —— 应等于生产端重放且成功入湖的总记录数(126191)
    total = df.count()
    print("==========================================")
    print("[验证 1] 表总条数: %d (期望 126191)" % total)

    # 验证2: dt 分区分布 —— 按日分组计数并升序排列, 核对各日分区
    # 数据量与阶段1清洗产出的分区清单一致
    print("[验证 2] 分区分布:")
    df.groupBy("dt").count().orderBy("dt").show()

    # 验证3: 行为类型分布 —— 按条数降序展示, 应与源数据构成一致
    # (pv 浏览远多于 fav/cart/buy)
    print("[验证 3] 行为类型分布:")
    type_cnt = df.groupBy("behavior_type").count()
    type_cnt.orderBy(type_cnt["count"].desc()).show()

    # 验证4: 事件时间 min/max —— 确认没有异常时间混入, 时间范围
    # 与源分区(2017-12-03 当日)一致
    print("[验证 4] 时间范围(期望 2017-12-03 00:00:00 ~ 15:59:59):")
    df.agg({"event_time": "min"}).withColumnRenamed(
        "min(event_time)", "min_time").show()
    df.agg({"event_time": "max"}).withColumnRenamed(
        "max(event_time)", "max_time").show()

    # 验证5: 主键唯一性 —— 按 behavior_id 分组找出计数>1 的组数, 期望 0:
    # 证明 upsert + ts 预合并有效抵御了 Kafka 重复投递/checkpoint 重放
    # 造成的重复入湖
    dup = df.groupBy("behavior_id").count().where("count > 1").count()
    print("[验证 5] 主键重复条数: %d (期望 0)" % dup)

    # 增量查询: BEGIN_INSTANT 之后的增量记录
    # 验证6: Hudi 增量查询 —— query.type=incremental 只读取 BEGIN_INSTANT
    # 之后新 commit 的记录, 用于验证微批 4 的 13951 条全部成功入湖,
    # 同时演示 Hudi 的增量读取能力(下游可按增量消费, 无需全表扫描)
    inc = (
        spark.read
        .format("hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.begin.instanttime", BEGIN_INSTANT)
        .load(TABLE_PATH)
    )
    inc_cnt = inc.count()
    print("[验证 6] 增量查询(%s 之后): %d 条 (期望 13951)" % (BEGIN_INSTANT, inc_cnt))

    # 汇总判定: 总条数、主键唯一性、增量条数三项同时达标才输出"全部通过"
    print("==========================================")
    ok = (total == 126191) and (dup == 0) and (inc_cnt == 13951)
    print("验证结论: %s" % ("全部通过" if ok else "存在不一致, 请排查"))
    print("==========================================")
    spark.stop()


if __name__ == "__main__":
    main()
