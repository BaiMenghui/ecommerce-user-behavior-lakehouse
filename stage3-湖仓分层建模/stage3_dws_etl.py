# -*- coding: utf-8 -*-
"""
阶段3（下）：DWS汇总层 主题宽表构建脚本
从 DWD 明细层读取，按"实体×天"粒度聚合，写入 3 张 Hudi COW 主题宽表：
  1. dws_user_action_day      用户主题宽表（行为计数 + 会话质量 + 新老用户）
  2. dws_item_action_day      商品主题宽表（行为计数 + 独立用户数）
  3. dws_category_action_day  类目主题宽表（行为计数 + 动销商品数）

用法：
  spark-submit stage3_dws_etl.py                    # 处理全部日期分区（9天全量回刷）
  spark-submit stage3_dws_etl.py --dt 2017-11-25    # 只处理指定日期分区

幂等性：主键=实体ID，预合并字段=dt，同分区重跑安全覆盖
"""
# ============================================================
# 【脚本总述】
#   脚本名称 : stage3_dws_etl.py
#   所属阶段 : 阶段3 湖仓分层构建 —— 第三步：DWS 汇总层
#   脚本职责 : 以 DWD 明细为输入，按"实体×天"粒度把明细行压缩成
#              三个主题的日粒度宽表（同一实体一天只一行）：
#                dws_user_action_day     用户×天（四类行为计数、
#                  行为总数、会话数、会话均深度、活跃时长、新老标记）
#                dws_item_action_day     商品×天（四类行为计数、
#                  行为总数、类目归属、浏览/购买独立用户数）
#                dws_category_action_day 类目×天（四类行为计数、
#                  行为总数、动销商品数、浏览独立用户数）
#              每个分区写完立即执行"明细 vs 三张宽表"的三角核对，
#              当场验证聚合无丢数。
#   输入表   : dwd_user_behavior_fact（Hudi MOR，DWD 明细事实层）
#   输出表   : dws_user_action_day / dws_item_action_day /
#              dws_category_action_day（Hudi COW，按 dt 分区）
#   运行方式 : WSL 内执行：
#     spark-submit --jars hudi-spark3.3-bundle_2.12-0.14.1.jar \
#       --conf spark.sql.catalogImplementation=hive \
#       stage3_dws_etl.py                 # 25 个日分区全量回刷
#     spark-submit --jars hudi-spark3.3-bundle_2.12-0.14.1.jar \
#       --conf spark.sql.catalogImplementation=hive \
#       stage3_dws_etl.py --dt 2017-11-25 # 只回刷单日分区
# ============================================================
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# Spark Session 初始化
# ------------------------------------------------------------
# 配置要点：HoodieCatalog + hive catalog 配套（同前两个脚本，
# 使 spark.sql 能直读 DWD/DWS 湖表）；额外把
# spark.sql.shuffle.partitions 从默认 200 调成 16——单机跑 25 个
# 小分区（每天约 4 万行）的聚合，200 个 shuffle 分区过于碎片化，
# 16 个足够并行且能减少小文件数量。
# ============================================================
spark = SparkSession.builder \
    .appName("stage3_dws_etl") \
    .config("spark.sql.warehouse.dir", "/home/BaiMenghui/hudi-bigdata/warehouse") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 参数解析
# ------------------------------------------------------------
# --dt 指定"只处理哪一天"：给了就单分区增量/回刷；不给则从
# DWD 表反查全部现存分区（25 天）做全量处理。采用"逐分区循环
# 处理"而非一把全表聚合，是为了让每个分区独立核对、独立重跑
# ——单日失败只需 --dt 重跑该日，不影响其他日期。
# ============================================================
parser = argparse.ArgumentParser()
parser.add_argument("--dt", default=None, help="目标日期分区，如 2017-11-25；缺省处理全部分区")
args = parser.parse_args()

if args.dt:
    dt_list = [args.dt]
else:
    dt_list = [r["dt"] for r in spark.sql(
        "SELECT DISTINCT dt FROM dwd_user_behavior_fact ORDER BY dt").collect()]

print(f"待处理日期分区: {dt_list}")

# 全历史首日（新老用户判定：用户首次活跃日 == 当前dt 即新用户）
# 全表扫一遍 DWD 求每个用户的 MIN(dt)，得到"用户→首次活跃日"
# 的小映射表；后面每个分区只做一次 left join 即可判定"当天是否
# 是该用户首次出现"，避免逐分区重复全表扫描。
first_dt_df = spark.sql("""
    SELECT user_id, MIN(dt) AS first_dt
    FROM dwd_user_behavior_fact
    GROUP BY user_id
""")

# ============================================================
# 工具函数
# ============================================================

def cnt(alias, t):
    """按行为类型计数"""
    # 条件计数：仅当 behavior_type 等于目标类型 t 时才计 1，生成
    # 该行为的次数列。三张宽表统一复用本函数，保证 pv/fav/cart/
    # buy 四列在三个主题的口径严格一致——这是后面三角核对能
    # 成立的前提。
    return F.count(F.when(F.col("behavior_type") == t, 1)).alias(alias)


def write_hudi(df, table, pk):
    """DWS 统一 COW upsert 写入（预合并字段=dt，重跑幂等）"""
    # 三张宽表共用同一写表函数，参数只差表名与主键：
    #   COW 类型——汇总表"按分区整体重算、读多写少"，写时复制
    #     换取更优的读取性能；
    #   主键=实体ID（user_id/item_id/category_id），分区=dt；
    #   预合并字段=dt——同分区重跑时同主键记录按相同的 dt 值
    #     合并覆盖，实现安全幂等，行数不会翻倍。
    df.write.format("hudi") \
        .option("hoodie.table.name", table) \
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
        .option("hoodie.datasource.write.operation", "upsert") \
        .option("hoodie.datasource.write.recordkey.field", pk) \
        .option("hoodie.datasource.write.partitionpath.field", "dt") \
        .option("hoodie.datasource.write.precombine.field", "dt") \
        .option("hoodie.cleaner.policy", "KEEP_LATEST_COMMITS") \
        .option("hoodie.cleaner.commits.retained", "10") \
        .mode("append") \
        .save(f"/home/BaiMenghui/hudi-bigdata/warehouse/{table}")


# ============================================================
# 单分区处理：聚合 -> 写入 -> 三角核对
# ============================================================
def process_one(dt):
    print("\n" + "=" * 60)
    print(f"处理分区 dt = {dt}")
    print("=" * 60)

    # 分区裁剪读取 DWD 明细
    # WHERE dt = '{dt}' 借助 Hudi 分区裁剪只扫描当天分区（约 4 万
    # 行），25 天逐日处理互不干扰；当天无数据则直接跳过，防止
    # 对空分区写出空宽表。
    dwd_day = spark.sql(f"""
        SELECT behavior_id, user_id, item_id, category_id,
               behavior_type, event_time, session_id, dt
        FROM dwd_user_behavior_fact
        WHERE dt = '{dt}'
    """)
    in_cnt = dwd_day.count()
    if in_cnt == 0:
        print(f"[SKIP] DWD 分区 dt={dt} 无数据")
        return
    print(f"DWD 明细输入行数: {in_cnt}")

    # ---------- 1. 用户主题宽表 ----------
    print("\n步骤1：构建 dws_user_action_day（用户×天）")
    # 按用户聚合当天行为，构成用户×天的行为画像主体：
    #   cnt(...) ×4 —— 四类行为各自的当日次数（复用工具函数）；
    #   count(*) —— 当天行为总数 action_cnt；
    #   countDistinct(session_id) —— 当天会话数 session_cnt；
    #   min/max(event_time) —— 当天首次/末次行为时间。
    user_df = dwd_day.groupBy("user_id").agg(
        cnt("pv_cnt", "pv"), cnt("fav_cnt", "fav"),
        cnt("cart_cnt", "cart"), cnt("buy_cnt", "buy"),
        F.count("*").alias("action_cnt"),
        F.countDistinct("session_id").alias("session_cnt"),
        F.min("event_time").alias("first_action_time"),
        F.max("event_time").alias("last_action_time")
    )
    # 在聚合结果上追加派生指标：
    #   avg_session_actions = 行为总数/会话数，"会话平均深度"，
    #     衡量单次访问的投入程度，round 保留 2 位小数；
    #   active_span_min = (末次-首次行为时间)/60，"当天活跃跨度"
    #     分钟数，近似在线时长，round 保留 1 位小数；
    #   is_new_user —— 当前分区日 == 该用户全历史首日 -> 新用户(1)，
    #     否则老用户(0)；join 完成后 drop 掉辅助列 first_dt；
    #   dt —— 补上分区常量列，同时作为宽表的分区与预合并字段。
    user_df = user_df \
        .withColumn("avg_session_actions",
                    F.round(F.col("action_cnt") / F.col("session_cnt"), 2)) \
        .withColumn("active_span_min",
                    F.round((F.unix_timestamp("last_action_time")
                             - F.unix_timestamp("first_action_time")) / 60.0, 1)) \
        .join(first_dt_df, "user_id", "left") \
        .withColumn("is_new_user",
                    F.when(F.lit(dt) == F.col("first_dt"), 1).otherwise(0)) \
        .drop("first_dt") \
        .withColumn("dt", F.lit(dt))
    print(f"用户宽表行数: {user_df.count()}")

    # ---------- 2. 商品主题宽表 ----------
    print("\n步骤2：构建 dws_item_action_day（商品×天）")
    # 按商品聚合：四类行为计数 + 行为总数，口径同用户宽表；
    #   category_id 用 max() 带出——同一商品的类目恒定，max 只是
    #     取值手段，实现"类目归属"属性随行保留；
    #   pv_user_cnt / buy_user_cnt 是"条件去重计数"——先 when 把
    #     非目标行为的 user_id 置 null，再 countDistinct（计数自动
    #     忽略 null），即得到当天浏览过/购买过该商品的独立用户数，
    #     是商品热度与转化率分析的核心指标。
    item_df = dwd_day.groupBy("item_id").agg(
        cnt("pv_cnt", "pv"), cnt("fav_cnt", "fav"),
        cnt("cart_cnt", "cart"), cnt("buy_cnt", "buy"),
        F.count("*").alias("action_cnt"),
        F.max("category_id").alias("category_id"),
        F.countDistinct(F.when(F.col("behavior_type") == "pv",
                               F.col("user_id"))).alias("pv_user_cnt"),
        F.countDistinct(F.when(F.col("behavior_type") == "buy",
                               F.col("user_id"))).alias("buy_user_cnt")
    ).withColumn("dt", F.lit(dt))
    print(f"商品宽表行数: {item_df.count()}")

    # ---------- 3. 类目主题宽表 ----------
    print("\n步骤3：构建 dws_category_action_day（类目×天）")
    # 按类目聚合：四类行为计数 + 行为总数，口径同前两张宽表；
    #   item_cnt = countDistinct(item_id)，当天该类目下"有动作的
    #     商品数"（动销商品数），衡量类目的活跃广度；
    #   pv_user_cnt 当天浏览过该类目的独立用户数（条件去重计数，
    #     写法同商品宽表）。
    cat_df = dwd_day.groupBy("category_id").agg(
        cnt("pv_cnt", "pv"), cnt("fav_cnt", "fav"),
        cnt("cart_cnt", "cart"), cnt("buy_cnt", "buy"),
        F.count("*").alias("action_cnt"),
        F.countDistinct("item_id").alias("item_cnt"),
        F.countDistinct(F.when(F.col("behavior_type") == "pv",
                               F.col("user_id"))).alias("pv_user_cnt")
    ).withColumn("dt", F.lit(dt))
    print(f"类目宽表行数: {cat_df.count()}")

    # ---------- 4. 写入 Hudi COW ----------
    print("\n步骤4：写入 3 张 COW 宽表")
    # 三张宽表逐张 upsert 落湖，主键分别为 user_id/item_id/
    # category_id，统一按 dt 分区（写入参数见 write_hudi 函数注释）。
    write_hudi(user_df, "dws_user_action_day", "user_id")
    write_hudi(item_df, "dws_item_action_day", "item_id")
    write_hudi(cat_df, "dws_category_action_day", "category_id")
    print("写入完成")

    # ---------- 5. 汇总-明细三角核对 ----------
    print("\n步骤5：三角核对（明细 == Σ用户 == Σ商品 == Σ类目）")
    # 核对原理：同一批明细无论按"用户/商品/类目"哪个维度分组，
    # 四类行为的总条数都必须与明细侧直查值相等——分组只改变行的
    # 归属，不改变总量；任何一边不等即说明聚合丢数或口径错位。
    # base：明细侧基准，直查 DWD 分区按行为类型条件计数。
    base = dwd_day.agg(
        *[F.count(F.when(F.col("behavior_type") == t, 1)).alias(t)
          for t in ["pv", "fav", "cart", "buy"]]
    ).collect()[0].asDict()

    # side_sum：把一张宽表所有行的 *_cnt 列纵向求和，还原出该
    # 主题视角下的四类行为总量，用于与明细侧基准对账。
    def side_sum(df):
        return df.agg(*[F.sum(c).alias(c) for c in
                        ["pv_cnt", "fav_cnt", "cart_cnt", "buy_cnt"]]).collect()[0].asDict()

    su, si, sc = side_sum(user_df), side_sum(item_df), side_sum(cat_df)

    # 逐行为类型对账：明细侧 vs Σ用户宽表 vs Σ商品宽表 vs Σ类目
    # 宽表，四方数值逐一相等记 PASS；任何一边不等记 FAIL，最后
    # 汇总提示排查重跑。
    all_pass = True
    for t in ["pv", "fav", "cart", "buy"]:
        ok = (base[t] == su[f"{t}_cnt"] == si[f"{t}_cnt"] == sc[f"{t}_cnt"])
        all_pass = all_pass and ok
        print(f"  {t:<5} 明细={base[t]:<8} 用户={su[f'{t}_cnt']:<8} "
              f"商品={si[f'{t}_cnt']:<8} 类目={sc[f'{t}_cnt']:<8} "
              f"{'PASS' if ok else 'FAIL'}")
    print(f"dt={dt} 核对结论: {'ALL PASS' if all_pass else 'HAS FAIL — 请排查后重跑'}")


# ============================================================
# 主流程：逐分区处理
# ------------------------------------------------------------
# 按日期顺序逐个分区执行"聚合 -> 写入 -> 三角核对"的完整闭环，
# 分区之间互不影响——单日核对失败只需用 --dt 重跑该日。
# ============================================================
for dt in dt_list:
    process_one(dt)

# ============================================================
# 总量预览（DAU 与行为总量按日分布）
# ------------------------------------------------------------
# 全量跑完后从用户宽表反查一遍：每天的 DAU（行数）与四类行为
# 总量，肉眼复核 25 天的趋势形状（如大促日 pv/buy 明显抬升），
# 作为整个 ETL 收尾的整体 sanity check。
# ============================================================
print("\n" + "=" * 60)
print("DWS 层总量预览")
print("=" * 60)
spark.sql("""
    SELECT dt, COUNT(*) AS dau,
           SUM(pv_cnt) AS pv, SUM(fav_cnt) AS fav,
           SUM(cart_cnt) AS cart, SUM(buy_cnt) AS buy
    FROM dws_user_action_day
    GROUP BY dt ORDER BY dt
""").show(truncate=False)

spark.stop()
print("\nDWS层ETL完成！")
