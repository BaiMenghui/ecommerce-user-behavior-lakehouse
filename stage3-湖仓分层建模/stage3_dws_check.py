# -*- coding: utf-8 -*-
"""
阶段3（下）：DWS 汇总-明细一致性校验脚本
对指定 dt 分区执行 7 项三角核对，全部 PASS 才允许下游 ADS 引用该分区。
用法：
  spark-submit stage3_dws_check.py --dt 2017-11-25
"""
# ============================================================
# 【脚本总述】
#   脚本名称 : stage3_dws_check.py
#   所属阶段 : 阶段3 湖仓分层构建 —— DWS 层数据质量门禁（校验脚本）
#   脚本职责 : 对指定 dt 分区执行 7 项"汇总-明细"三角核对，验证
#              DWS 三张宽表与 DWD 明细层数据完全一致：
#                (1)~(4) pv/fav/cart/buy 四类行为总量：明细侧直查
#                    值分别 == Σ用户宽表 == Σ商品宽表 == Σ类目宽表；
#                (5) DAU == 用户宽表行数（去重用户数应等于宽表行数）；
#                (6) 商品数 == 商品宽表行数；
#                (7) 类目数 == 类目宽表行数；
#                另加一项行内守恒：每行 action_cnt == 四类计数之和。
#              全部 PASS 该分区才允许下游 ADS 层引用。
#   输入表   : dwd_user_behavior_fact + 三张 dws_*_action_day
#   输出     : 控制台校验报告（不落表）
#   运行方式 : WSL 内执行：
#   spark-submit --jars hudi-spark3.3-bundle_2.12-0.14.1.jar \
#     --conf spark.sql.catalogImplementation=hive \
#     stage3_dws_check.py --dt 2017-11-25
# ============================================================
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# 参数解析
# ------------------------------------------------------------
# --dt 为必填：数据校验天然是"按分区"进行的（宽表与明细按天
# 对账），缺了分区就没有可比对象，故 required=True 缺参直接报错。
# ============================================================
parser = argparse.ArgumentParser()
parser.add_argument("--dt", required=True, help="待校验日期分区，如 2017-11-25")
args = parser.parse_args()
dt = args.dt

# ============================================================
# Spark Session 初始化
# ------------------------------------------------------------
# 配置同其余阶段脚本：HoodieCatalog + hive catalog 让 spark.sql
# 能直接查询 Hudi 表；本脚本只做少量聚合对账，无需内存/并行度
# 额外调优。
# ============================================================
spark = SparkSession.builder \
    .appName("stage3_dws_check") \
    .config("spark.sql.warehouse.dir", "/home/BaiMenghui/hudi-bigdata/warehouse") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print(f"DWS 层数据一致性校验  dt = {dt}")
print("=" * 60)

# ============================================================
# 1. 加载明细侧与汇总侧
# ------------------------------------------------------------
# 读取同一个 dt 分区的四个数据集：dwd 是"明细侧基准"，user/item/
# cat 三张宽表是"汇总侧"。分区谓词下推到 Hudi 后只扫单日数据，
# 校验成本很低。
# ============================================================
dwd = spark.sql(f"SELECT * FROM dwd_user_behavior_fact WHERE dt = '{dt}'")
user = spark.sql(f"SELECT * FROM dws_user_action_day WHERE dt = '{dt}'")
item = spark.sql(f"SELECT * FROM dws_item_action_day WHERE dt = '{dt}'")
cat = spark.sql(f"SELECT * FROM dws_category_action_day WHERE dt = '{dt}'")

# ============================================================
# 2. 明细侧基准
# ------------------------------------------------------------
# 一次 agg 同时算出 7 个基准值，作为后续核对的"期望值"：
#   pv/fav/cart/buy —— 条件计数，明细侧四类行为的总条数；
#   uv/iv/cv        —— countDistinct 去重的用户数、商品数、类目数。
# collect()[0].asDict() 把结果取回 driver 端变成 Python 字典，
# 便于逐项取值比对。
# ============================================================
base = dwd.agg(
    *[F.count(F.when(F.col("behavior_type") == t, 1)).alias(t)
      for t in ["pv", "fav", "cart", "buy"]],
    F.countDistinct("user_id").alias("uv"),
    F.countDistinct("item_id").alias("iv"),
    F.countDistinct("category_id").alias("cv")
).collect()[0].asDict()


def side_sum(df):
    """汇总侧：三张宽表行为计数求和"""
    # 把一张宽表所有行的 *_cnt 纵向求和，还原该主题视角下的
    # 四类行为总量，与明细侧基准对账（与 dws_etl 中的对账函数
    # 保持同一套口径）。
    return df.agg(*[F.sum(c).alias(c) for c in
                    ["pv_cnt", "fav_cnt", "cart_cnt", "buy_cnt"]]).collect()[0].asDict()


# su/si/sc：用户/商品/类目三张宽表各自还原出的四类行为总量；
# du/di/dc：三张宽表的行数——宽表一行对应一个实体，行数应恰好
# 等于明细侧对应的去重实体数。
su, si, sc = side_sum(user), side_sum(item), side_sum(cat)
du, di, dc = user.count(), item.count(), cat.count()

# ============================================================
# 3. 七项核对
# ------------------------------------------------------------
# check：单项核对小工具——比较期望值与实际值，把
# (校验项名, 期望, 实际, PASS|FAIL) 追加进结果列表，最后统一打印。
# ============================================================
results = []


def check(name, expect, actual):
    ok = (expect == actual)
    results.append((name, expect, actual, "PASS" if ok else "FAIL"))


# (1)~(4) 四类行为总量：明细侧基准值分别与三张宽表的纵向求和
# 对比，共 4 类行为 × 3 张宽表 = 12 组比对，任何一组不等即 FAIL。
for t in ["pv", "fav", "cart", "buy"]:
    check(f"{t} 明细==Σ用户宽表", base[t], su[f"{t}_cnt"])
    check(f"{t} 明细==Σ商品宽表", base[t], si[f"{t}_cnt"])
    check(f"{t} 明细==Σ类目宽表", base[t], sc[f"{t}_cnt"])
# (5)~(7) 实体规模核对：宽表一行对应一个实体，因此三张宽表的
# 行数必须分别等于明细侧的去重用户数/商品数/类目数——多一行
# 说明有重复写入，少一行说明聚合丢组。
check("DAU == 用户宽表行数", base["uv"], du)
check("商品数 == 商品宽表行数", base["iv"], di)
check("类目数 == 类目宽表行数", base["cv"], dc)

# 行内守恒：action_cnt = pv+fav+cart+buy（违例行数应为0）
# 最后一道防线：宽表每行内部的"行为总数"列必须等于四类计数列
# 之和（一条行为必然且只能属于四类之一）。where 过滤出不守恒的
# 行并计数，期望为 0；不为 0 说明聚合各列之间自相矛盾。
bad_rows = user.where(F.col("action_cnt") !=
                      F.col("pv_cnt") + F.col("fav_cnt") +
                      F.col("cart_cnt") + F.col("buy_cnt")).count()
check("行内守恒 action_cnt=四类之和(违例行数)", 0, bad_rows)

# ============================================================
# 4. 输出校验报告
# ------------------------------------------------------------
# 逐项打印"校验项 / 期望基准 / 实际值 / PASS|FAIL"的对齐表格，
# 并统计 FAIL 项数：0 项 -> ALL PASS，该分区放行给下游 ADS 层；
# 有 FAIL -> 打印告警并明示禁止下游引用该分区。
# ============================================================
print(f"\n{'校验项':<36}{'基准':<12}{'实际':<12}结论")
print("-" * 64)
for name, e, a, r in results:
    print(f"{name:<36}{e:<12}{a:<12}{r}")

fail_cnt = sum(1 for r in results if r[3] == "FAIL")
print("-" * 64)
if fail_cnt == 0:
    print("总体结论: ALL PASS —— 分区可用，允许下游 ADS 引用")
else:
    print(f"总体结论: {fail_cnt} 项 FAIL —— 禁止下游引用该分区，请排查后重跑")

spark.stop()
