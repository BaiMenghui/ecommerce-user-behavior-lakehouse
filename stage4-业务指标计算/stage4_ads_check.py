# -*- coding: utf-8 -*-
# ============================================================
# 脚本名称: stage4_ads_check.py
# 所属阶段: 阶段4 —— ADS 层数据质量校验（三角验证）
# 功能说明: 对指定日期分区做"三方交叉核对"——DWD 明细原始聚合 ↔ DWS 宽表汇总 ↔
#           ADS 指标结果，覆盖 DAU/漏斗四步计数/GMV/TopN/类目占比/时段热力六组指标；
#           全部一致输出 ALL PASS，任一不一致输出 FAIL 并给出失败项数
# 输入: dwd_user_behavior_fact / dws_user_action_day / dws_item_action_day（DWD/DWS 只读），
#       ads_dau_trend / ads_funnel / ads_item_topn / ads_category_share / ads_hour_heatmap
# 输出: 控制台校验报告（逐项 基准值 vs 实际值 + PASS/FAIL + 总体结论）
# 运行方式: WSL 下 spark-submit --jars hudi-spark3.3-bundle_2.12-0.14.1.jar \
#           --conf spark.sql.catalogImplementation=hive stage4_ads_check.py --dt 2017-11-25
# ============================================================
"""
阶段4：ADS 指标校验脚本
多口径交叉验证：DWD 明细侧 ↔ DWS 汇总侧 ↔ ADS 指标侧 三方核对。
用法：
  spark-submit stage4_ads_check.py --dt 2017-11-25
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 模拟单价口径（与 stage4_ads_etl.py 保持一致）
SIM_PRICE_EXPR = "pmod(hash(item_id), 450) + 50"

parser = argparse.ArgumentParser()
parser.add_argument("--dt", required=True, help="待校验日期分区，如 2017-11-25")
args = parser.parse_args()
dt = args.dt

spark = SparkSession.builder \
    .appName("stage4_ads_check") \
    .config("spark.sql.warehouse.dir", "/home/BaiMenghui/hudi-bigdata/warehouse") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 64)
print(f"ADS 指标多口径交叉校验  dt = {dt}")
print("=" * 64)

results = []


def check(name, expect, actual, tol=0.0):
    """tol>0 时为浮点容差比对"""
    # 每项校验记录为 (名称, 基准值, 实际值, 结论)，先攒入 results 列表，最后统一打印成报告
    if tol > 0:
        ok = abs(float(expect) - float(actual)) <= tol
    else:
        ok = (expect == actual)
    results.append((name, expect, actual, "PASS" if ok else "FAIL"))


# ============================================================
# 1. DAU 三方核对：DWD distinct user == DWS 行数 == ads_dau_trend.dau
# ============================================================
# 三方口径链：DWD 明细按 user_id 去重（最原始事实）↔ DWS 用户宽表行数（每人恰一行）↔
# ADS 表 dau 字段（指标结果）。三者必须相等，才能证明"明细→汇总→指标"链路无丢失/重复
dwd_uv = spark.sql(f"""
    SELECT COUNT(DISTINCT user_id) AS uv
    FROM dwd_user_behavior_fact WHERE dt = '{dt}'
""").collect()[0]["uv"]
dws_dau = spark.sql(f"""
    SELECT COUNT(*) AS dau FROM dws_user_action_day WHERE dt = '{dt}'
""").collect()[0]["dau"]
ads = spark.sql(f"SELECT * FROM ads_dau_trend WHERE dt = '{dt}'").collect()
check("ads_dau_trend 存在当日记录", 1, len(ads))
if ads:
    a = ads[0].asDict()
    # DWD↔DWS、DWS↔ADS 两两比对：任一段断裂都能从报告定位到具体层级
    check("DAU: DWD == DWS", dwd_uv, dws_dau)
    check("DAU: DWS == ADS", dws_dau, a["dau"])
    # 内部自洽校验：新用户数 + 老用户数 必须恰好等于 DAU 总数（拆分维度不丢人）
    check("新老拆分守恒 dau=new+old", a["dau"],
          a["new_user_cnt"] + a["old_user_cnt"])
    # 订单量回 DWD 明细直接数 buy 行为笔数，验证 ADS 未漏计/重复计数
    check("订单量: ADS.order_cnt == DWD buy笔数",
          spark.sql(f"""SELECT COUNT(*) FROM dwd_user_behavior_fact
                        WHERE dt='{dt}' AND behavior_type='buy'""").collect()[0][0],
          a["order_cnt"])
    # GMV 复算：Σ(buy_cnt × 模拟单价) 与 ADS.gmv 一致
    # 用与 ETL 完全相同的确定性单价表达式重算一遍 GMV 再比对，确认口径未被改动
    gmv_re = spark.sql(f"""
        SELECT ROUND(SUM(buy_cnt * ({SIM_PRICE_EXPR})), 2) AS gmv
        FROM dws_item_action_day WHERE dt = '{dt}'
    """).collect()[0]["gmv"]
    check("GMV: 复算 == ADS.gmv", gmv_re, a["gmv"])

# ============================================================
# 2. 漏斗行为量三方核对
# ============================================================
# 基准取 DWD 明细的条件计数：COUNT(CASE WHEN behavior_type=... THEN 1 END) 只统计命中该类型的行，
# 等价于按类型过滤后计数；随后逐类型与 DWS 求和、ADS 漏斗表三方比对，锁定漏斗数据源头一致
base = spark.sql(f"""
    SELECT
        COUNT(CASE WHEN behavior_type='pv' THEN 1 END) AS pv,
        COUNT(CASE WHEN behavior_type='fav' THEN 1 END) AS fav,
        COUNT(CASE WHEN behavior_type='cart' THEN 1 END) AS cart,
        COUNT(CASE WHEN behavior_type='buy' THEN 1 END) AS buy
    FROM dwd_user_behavior_fact WHERE dt = '{dt}'
""").collect()[0].asDict()
fn = spark.sql(f"SELECT * FROM ads_funnel WHERE dt = '{dt}'").collect()
check("ads_funnel 存在当日记录", 1, len(fn))
if fn:
    f0 = fn[0].asDict()
    # 对 pv/fav/cart/buy 四类行为逐个做 DWD↔DWS、DWS↔ADS 两段等值比对
    for t in ["pv", "fav", "cart", "buy"]:
        dws_sum = spark.sql(f"""
            SELECT SUM({t}_cnt) AS s FROM dws_user_action_day WHERE dt = '{dt}'
        """).collect()[0]["s"]
        check(f"漏斗{t}: DWD == DWS", base[t], dws_sum)
        check(f"漏斗{t}: DWS == ADS", dws_sum, f0[f"{t}_cnt"])

# ============================================================
# 3. TopN 校验：行数、降序、Top1 与 DWS 商品宽表最大值一致
# ============================================================
# TopN 三点校验：① 行数应为 min(10, 当日商品数)（商品不足 10 个时取实际数）；
# ② 排序规则须与 ETL 的 ROW_NUMBER 一致：buy_cnt 降序、并列按 item_id 升序；
# ③ Top1 的 buy_cnt 必须等于当日商品宽表中的最大购买次数（取数正确的锚点）
topn = spark.sql(f"""
    SELECT rank_no, item_id, buy_cnt FROM ads_item_topn
    WHERE dt = '{dt}' ORDER BY rank_no
""").collect()
item_cnt_day = spark.sql(f"""
    SELECT COUNT(*) AS c FROM dws_item_action_day WHERE dt = '{dt}'
""").collect()[0]["c"]
expect_rows = min(10, item_cnt_day)
check("TopN 行数 = min(10, 当日商品数)", expect_rows, len(topn))
if topn:
    # 逐相邻对检查序列：严格降序，或数值相等时 item_id 升序——完整复现 ETL 的排序口径
    sorted_desc = all(
        topn[i]["buy_cnt"] > topn[i + 1]["buy_cnt"] or
        (topn[i]["buy_cnt"] == topn[i + 1]["buy_cnt"] and
         topn[i]["item_id"] < topn[i + 1]["item_id"])
        for i in range(len(topn) - 1)
    )
    check("TopN 排序正确(降序+平分按ID升序)", True, sorted_desc)
    max_buy = spark.sql(f"""
        SELECT MAX(buy_cnt) AS m FROM dws_item_action_day WHERE dt = '{dt}'
    """).collect()[0]["m"]
    check("Top1 buy_cnt == 当日最大购买次数", max_buy, topn[0]["buy_cnt"])

# ============================================================
# 4. 类目占比校验：占比合计 ≈ 100%（round 容差 0.5）
# ============================================================
# 各类目占比理论上合计恰为 100%，但每个占比都经 ROUND 保留 2 位，累加会累积舍入误差，
# 故用 0.5 的容差判定而非严格相等（tol>0 走浮点容差分支）
share_sum = spark.sql(f"""
    SELECT ROUND(SUM(buy_share_ratio), 2) AS s
    FROM ads_category_share WHERE dt = '{dt}'
""").collect()[0]["s"]
if share_sum is not None:
    check("类目购买占比合计≈100%", 100.0, share_sum, tol=0.5)

# ============================================================
# 5. 时段热力校验：Σ小时行为量 == DWD 明细总量
# ============================================================
# 守恒校验：热力表 24 行（24 个小时）各列求和，必须等于 DWD 明细总行数/总 pv 数——
# 直通聚合只是"换维度分组"，行数守恒即证明聚合无丢行
hour = spark.sql(f"""
    SELECT SUM(action_cnt) AS a, SUM(pv_cnt) AS p
    FROM ads_hour_heatmap WHERE dt = '{dt}'
""").collect()[0].asDict()
dwd_total = spark.sql(f"""
    SELECT COUNT(*) AS c,
           COUNT(CASE WHEN behavior_type='pv' THEN 1 END) AS p
    FROM dwd_user_behavior_fact WHERE dt = '{dt}'
""").collect()[0].asDict()
if hour["a"] is not None:
    check("热力图Σaction == DWD明细行数", dwd_total["c"], hour["a"])
    check("热力图Σpv == DWD pv总量", dwd_total["p"], hour["p"])

# ============================================================
# 输出校验报告
# ============================================================
print(f"\n{'校验项':<40}{'基准':<14}{'实际':<14}结论")
print("-" * 72)
for name, e, act, r in results:
    print(f"{name:<40}{str(e):<14}{str(act):<14}{r}")

# 汇总 FAIL 项数：0 项判 ALL PASS，否则提示定位排查后重跑 ETL
fail_cnt = sum(1 for r in results if r[3] == "FAIL")
print("-" * 72)
if fail_cnt == 0:
    print("总体结论: ALL PASS —— 指标结果可信，可交付大屏联调")
else:
    print(f"总体结论: {fail_cnt} 项 FAIL —— 请排查对应指标后重跑 stage4_ads_etl.py")

spark.stop()
