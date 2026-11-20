# -*- coding: utf-8 -*-
# ============================================================
# 脚本名称: stage4_ads_etl.py
# 所属阶段: 阶段4 —— ADS 应用层指标计算
# 功能说明: 从 DWS 汇总层（时段热力例外走 DWD 直通）计算 5 张 ADS 指标表，
#           按日期分区 insert_overwrite 幂等写入 Hudi，末尾统一导出单文件 CSV
# 输入: dws_user_action_day / dws_item_action_day / dws_category_action_day（DWS 汇总表），
#       dwd_user_behavior_fact（DWD 明细表，仅步骤5时段热力直通使用）
# 输出: ads_dau_trend / ads_funnel / ads_item_topn / ads_category_share /
#       ads_hour_heatmap 五张 Hudi 表 + /home/BaiMenghui/hudi-bigdata/export/ads/<table>/ 下 CSV
# 运行方式: WSL 下 spark-submit --jars hudi-spark3.3-bundle_2.12-0.14.1.jar \
#           --conf spark.sql.catalogImplementation=hive stage4_ads_etl.py [--dt 2017-11-25]
# ============================================================
"""
阶段4：ADS应用层 指标计算脚本
从 DWS 层读取（时段热力走 DWD 直通例外），按《指标口径文档》计算 5 张 ADS 指标表：
  1. ads_dau_trend       DAU趋势 + GMV + 客单价 + 新老占比（大屏：折线/数字卡）
  2. ads_funnel          转化漏斗（大屏：漏斗图）
  3. ads_item_topn       热销商品Top10（大屏：排行榜）
  4. ads_category_share  类目销售占比（大屏：饼图）
  5. ads_hour_heatmap    活跃时段热力（大屏：热力图，DWD直通）

用法：
  spark-submit stage4_ads_etl.py                    # 全部日期分区（9天回刷）+ 导出CSV
  spark-submit stage4_ads_etl.py --dt 2017-11-25    # 只算指定日期

幂等性：主键在分区内唯一，预合并字段=dt，同分区重跑安全覆盖
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# Spark Session 初始化
# ============================================================
# 挂载 Hudi catalog 与 SQL 扩展后，spark.sql 可直接读写 Hudi 表并支持 insert_overwrite/delete；
# Kryo 序列化为 Hudi 官方必配项；shuffle 分区降为 16，避免本地/小集群模式产生大量小任务
spark = SparkSession.builder \
    .appName("stage4_ads_etl") \
    .config("spark.sql.warehouse.dir", "/home/BaiMenghui/hudi-bigdata/warehouse") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 模拟单价区间：50 ~ 499 元（按 item_id 确定性生成，口径文档条款3）
# pmod(hash(item_id), 450) + 50 对同一 item_id 每次结果恒定，
# 因此校验脚本可按同一表达式复算 GMV，实现可复现验证
SIM_PRICE_EXPR = "pmod(hash(item_id), 450) + 50"

# ============================================================
# 参数解析
# ============================================================
parser = argparse.ArgumentParser()
parser.add_argument("--dt", default=None, help="目标日期分区，如 2017-11-25；缺省处理全部分区")
args = parser.parse_args()

# 传入 --dt 时只算单分区；缺省则从 dws_user_action_day 反查全部现存分区日期（整体回刷用）
if args.dt:
    dt_list = [args.dt]
else:
    dt_list = [r["dt"] for r in spark.sql(
        "SELECT DISTINCT dt FROM dws_user_action_day ORDER BY dt").collect()]

print(f"待处理日期分区: {dt_list}")


# ============================================================
# 工具函数
# ============================================================
def write_hudi(df, table, pk):
    """ADS 统一 COW upsert 写入（预合并字段=dt，重跑幂等）"""
    # insert_overwrite = 动态按分区整体覆盖：只重写本次涉及的分区，同分区重跑无重复数据（幂等）
    # recordkey=业务主键、precombine=dt：分区内主键唯一，重复记录按 dt 预合并取最新
    # cleaner 保留最近 10 个 commit：控制 COW 历史版本文件数量，防止小文件与存储膨胀
    df.write.format("hudi") \
        .option("hoodie.table.name", table) \
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
        .option("hoodie.datasource.write.operation", "insert_overwrite") \
        .option("hoodie.datasource.write.recordkey.field", pk) \
        .option("hoodie.datasource.write.partitionpath.field", "dt") \
        .option("hoodie.datasource.write.precombine.field", "dt") \
        .option("hoodie.cleaner.policy", "KEEP_LATEST_COMMITS") \
        .option("hoodie.cleaner.commits.retained", "10") \
        .mode("append") \
        .save(f"/home/BaiMenghui/hudi-bigdata/warehouse/{table}")


def export_csv(table):
    """ADS 结果导出单文件 CSV（大屏联调用，W8 服务层数据源）"""
    # 按日期排序后 coalesce(1) 强制合并为单文件，带表头 overwrite 覆盖旧导出，API 可直接解析
    (spark.sql(f"SELECT * FROM {table} ORDER BY dt")
     .coalesce(1).write
     .mode("overwrite").option("header", True)
     .csv(f"/home/BaiMenghui/hudi-bigdata/export/ads/{table}"))


# ============================================================
# 单分区指标计算
# ============================================================
def process_one(dt):
    print("\n" + "=" * 60)
    print(f"计算分区 dt = {dt}")
    print("=" * 60)

    # ---------- 1. DAU趋势 + GMV + 客单价 ----------
    print("步骤1：ads_dau_trend（DAU折线 / GMV数字卡 / 新老占比）")
    # 三个子查询 CROSS JOIN 拼成一行指标：u=从 DWS 用户宽表按天聚合出 DAU、新老拆分与四类行为量；
    # b=回 DWD 明细按 user_id 去重统计付款用户数；g=从商品宽表 Σ(buy_cnt×模拟单价) 得当日 GMV。
    # 客单价 = GMV / 付款用户数（付款人数为 0 时置 0，避免除零）
    dau_df = spark.sql(f"""
        SELECT
            u.dau, u.new_user_cnt, u.old_user_cnt, u.new_user_ratio,
            u.pv_cnt, u.fav_cnt, u.cart_cnt, u.buy_cnt, u.action_cnt,
            b.paying_user_cnt,
            u.buy_cnt AS order_cnt,
            ROUND(g.gmv, 2) AS gmv,
            CASE WHEN b.paying_user_cnt > 0
                 THEN ROUND(g.gmv / b.paying_user_cnt, 2) ELSE 0 END AS avg_ticket,
            '{dt}' AS dt
        FROM (
            SELECT COUNT(*) AS dau,
                   SUM(is_new_user) AS new_user_cnt,
                   SUM(CASE WHEN is_new_user = 0 THEN 1 ELSE 0 END) AS old_user_cnt,
                   ROUND(SUM(is_new_user) / COUNT(*) * 100, 2) AS new_user_ratio,
                   SUM(pv_cnt) AS pv_cnt, SUM(fav_cnt) AS fav_cnt,
                   SUM(cart_cnt) AS cart_cnt, SUM(buy_cnt) AS buy_cnt,
                   SUM(action_cnt) AS action_cnt
            FROM dws_user_action_day WHERE dt = '{dt}'
        ) u
        CROSS JOIN (
            SELECT COUNT(DISTINCT user_id) AS paying_user_cnt
            FROM dwd_user_behavior_fact
            WHERE dt = '{dt}' AND behavior_type = 'buy'
        ) b
        CROSS JOIN (
            SELECT SUM(buy_cnt * ({SIM_PRICE_EXPR})) AS gmv
            FROM dws_item_action_day WHERE dt = '{dt}'
        ) g
    """)
    write_hudi(dau_df, "ads_dau_trend", "dt")
    dau_df.show(truncate=False)

    # ---------- 2. 转化漏斗 ----------
    print("步骤2：ads_funnel（浏览→收藏→加购→购买）")
    # 四步行为量取自 DWS 用户宽表按天求和；
    # 全漏斗转化率 = 当步行为量 / 首步 pv 行为量（pv2fav/pv2cart/pv2buy），
    # cart2buy = 购买 / 加购，单独体现"加购→成交"最后一环的路径流失
    funnel_df = spark.sql(f"""
        SELECT
            pv_cnt, fav_cnt, cart_cnt, buy_cnt,
            CASE WHEN pv_cnt > 0 THEN ROUND(fav_cnt / pv_cnt * 100, 2) ELSE 0 END AS pv2fav_rate,
            CASE WHEN pv_cnt > 0 THEN ROUND(cart_cnt / pv_cnt * 100, 2) ELSE 0 END AS pv2cart_rate,
            CASE WHEN cart_cnt > 0 THEN ROUND(buy_cnt / cart_cnt * 100, 2) ELSE 0 END AS cart2buy_rate,
            CASE WHEN pv_cnt > 0 THEN ROUND(buy_cnt / pv_cnt * 100, 2) ELSE 0 END AS pv2buy_rate,
            '{dt}' AS dt
        FROM (
            SELECT SUM(pv_cnt) AS pv_cnt, SUM(fav_cnt) AS fav_cnt,
                   SUM(cart_cnt) AS cart_cnt, SUM(buy_cnt) AS buy_cnt
            FROM dws_user_action_day WHERE dt = '{dt}'
        )
    """)
    write_hudi(funnel_df, "ads_funnel", "dt")
    funnel_df.show(truncate=False)

    # ---------- 3. 热销商品Top10 ----------
    print("步骤3：ads_item_topn（按购买次数Top10）")
    # ROW_NUMBER() 全量排名：buy_cnt 降序、并列时按 item_id 升序打破平局，保证重跑排名稳定；
    # 外层 WHERE 只保留前 10 名；gmv = buy_cnt × 模拟单价，与 DAU 表的 GMV 口径一致
    topn_df = spark.sql(f"""
        SELECT item_id, rank_no, category_id, buy_cnt, action_cnt,
               pv_user_cnt, buy_user_cnt,
               sim_price, ROUND(buy_cnt * sim_price, 2) AS gmv, dt
        FROM (
            SELECT item_id, category_id, buy_cnt, action_cnt,
                   pv_user_cnt, buy_user_cnt,
                   {SIM_PRICE_EXPR} AS sim_price,
                   ROW_NUMBER() OVER (
                       ORDER BY buy_cnt DESC, item_id ASC
                   ) AS rank_no,
                   dt
            FROM dws_item_action_day WHERE dt = '{dt}'
        ) WHERE rank_no <= 10
    """)
    write_hudi(topn_df, "ads_item_topn", "item_id")
    topn_df.show(truncate=False)

    # ---------- 4. 类目销售占比 ----------
    print("步骤4：ads_category_share（购买量/行为量双占比）")
    # SUM(...) OVER () 是无分区的窗口全表求和（即当日所有类目的总购买量/总行为量），
    # 各类目自身值除以总计得占比(%)；购买量、总行为量两个维度各算一份占比
    share_df = spark.sql(f"""
        SELECT category_id, buy_cnt, action_cnt, item_cnt,
               buy_cnt / SUM(buy_cnt) OVER () * 100 AS buy_share_ratio,
               action_cnt / SUM(action_cnt) OVER () * 100 AS action_share_ratio,
               '{dt}' AS dt
        FROM dws_category_action_day WHERE dt = '{dt}'
    """)
    write_hudi(share_df, "ads_category_share", "category_id")
    share_df.show(truncate=False)

    # ---------- 5. 活跃时段热力（DWD直通例外） ----------
    print("步骤5：ads_hour_heatmap（DWD 按 hour_of_day 直通聚合）")
    # DWS 层无小时粒度，故本表为 DWD 直通例外：直接对明细按 hour_of_day 分组；
    # COUNT(CASE WHEN behavior_type=... ) 为条件计数（命中该行为类型才计 1），
    # 把每小时总行为量拆成 pv/fav/cart/buy 四列
    hour_df = spark.sql(f"""
        SELECT hour_of_day,
               COUNT(*) AS action_cnt,
               COUNT(CASE WHEN behavior_type = 'pv' THEN 1 END) AS pv_cnt,
               COUNT(CASE WHEN behavior_type = 'fav' THEN 1 END) AS fav_cnt,
               COUNT(CASE WHEN behavior_type = 'cart' THEN 1 END) AS cart_cnt,
               COUNT(CASE WHEN behavior_type = 'buy' THEN 1 END) AS buy_cnt,
               '{dt}' AS dt
        FROM dwd_user_behavior_fact
        WHERE dt = '{dt}'
        GROUP BY hour_of_day ORDER BY hour_of_day
    """)
    write_hudi(hour_df, "ads_hour_heatmap", "hour_of_day")
    hour_df.show(truncate=False)

    print(f"dt={dt} 五张 ADS 指标表计算完成")


# ============================================================
# 主流程
# ============================================================
# 逐分区计算：指定 --dt 时只有一个分区；全量模式按日期顺序回刷全部历史分区
for dt in dt_list:
    process_one(dt)

# ============================================================
# 导出 CSV（大屏联调 / W8 服务层数据源）
# ============================================================
print("\n" + "=" * 60)
print("导出 ADS 结果 CSV → /home/BaiMenghui/hudi-bigdata/export/ads/")
print("=" * 60)
# 依次导出 5 张 ADS 表到 export/ads/<table>/；insert_overwrite 未涉及的分区仍保留在表中，
# 故导出内容为该表全部分区结果
for t in ["ads_dau_trend", "ads_funnel", "ads_item_topn",
          "ads_category_share", "ads_hour_heatmap"]:
    export_csv(t)
    print(f"  {t}.csv 导出完成")

spark.stop()
print("\nADS层指标计算完成！")
