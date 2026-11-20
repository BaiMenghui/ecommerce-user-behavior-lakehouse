# -*- coding: utf-8 -*-
"""
阶段3（上）：DWD明细层 ETL脚本
从ODS层读取原始数据，执行：
  1. 数据清洗（去重、异常过滤）
  2. 维度关联（退化维度、日期代理键）
  3. 会话划分（Session切分）
  4. 字段丰富（行为编码、小时提取）
  5. 写入 Hudi MOR 表
"""
# ============================================================
# 【脚本总述】
#   脚本名称 : stage3_dwd_etl.py
#   所属阶段 : 阶段3 湖仓分层构建 —— 第二步：DWD 明细事实层
#   脚本职责 : 把 ODS 原始日志加工成"一行一次行为"的干净明细事实：
#              (1) 清洗：按 behavior_id 去重 + 剔除空键/非法行为
#                  类型（1048576 -> 1048560，共剔除 16 条脏数据）；
#              (2) 字段丰富：行为类型编码化 behavior_code、日期
#                  代理键 date_key、小时字段 hour_of_day；
#              (3) 会话切分：以"同一用户相邻行为间隔>30 分钟"为界，
#                  用 lag 取时间差 + 窗口累加生成 session_id（本阶段
#                  最核心的逻辑，见步骤4 逐行详解）；
#              (4) 写入 Hudi MOR 事实表 dwd_user_behavior_fact，
#                  按 dt 日分区，为下游 DWS/ADS 提供统一明细口径。
#   输入表   : ods_user_behavior_log（Hudi MOR，ODS 层原始明细）
#   输出表   : dwd_user_behavior_fact（Hudi MOR，按 dt 分区）
#   运行方式 : WSL 内执行（HoodieCatalog 依赖 Hive metastore 找表，
#              必须带 --conf spark.sql.catalogImplementation=hive）：
#   spark-submit --jars hudi-spark3.3-bundle_2.12-0.14.1.jar \
#     --conf spark.sql.catalogImplementation=hive \
#     stage3_dwd_etl.py
# ============================================================
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ============================================================
# Spark Session 初始化
# ------------------------------------------------------------
# 配置要点：
#   * spark_catalog -> HoodieCatalog + 提交时 --conf
#     spark.sql.catalogImplementation=hive：经 Hive metastore 发现
#     ODS/DWD 湖表，spark.sql 才能直接查询 Hudi 表；
#   * KryoSerializer + HoodieSparkSessionExtension：Hudi 写入的
#     标准配套（记录序列化器 + SQL 扩展注入）；
#   * driver/executor 各 4g：104.86 万条明细的会话切分需要对
#     user_id 做 shuffle + 窗口内排序，按单机资源预留内存。
# ============================================================
spark = SparkSession.builder \
    .appName("stage3_dwd_etl") \
    .config("spark.sql.warehouse.dir", "/home/BaiMenghui/hudi-bigdata/warehouse") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 1. 加载 ODS 层数据
# ============================================================
print("=" * 60)
print("步骤1：加载 ODS 层数据")
print("=" * 60)

# 从 ODS 湖表全量投影 8 个原始字段。ODS 遵循"贴源"原则：不改数值、
# 不做过滤，一切清洗动作放到 DWD 层完成，保证原始数据可回溯、
# 加工逻辑可重算。字段说明：
#   behavior_id : 阶段2 入湖时生成的行为唯一主键，是本次去重与
#                 事实表 recordkey 的依据；
#   ts          : 秒级事件时间戳，会话切分的排序与间隔计算全靠它；
#   event_time  : 可读格式的事件时间（yyyy-MM-dd HH:mm:ss）；
#   dt          : 入湖日分区字段，写 DWD 时沿用为分区键。
ods_df = spark.sql("""
    SELECT
        behavior_id,
        user_id,
        item_id,
        category_id,
        behavior_type,
        ts,
        event_time,
        dt
    FROM ods_user_behavior_log
""")

print(f"ODS原始数据量: {ods_df.count()} 条")

# ============================================================
# 2. 数据清洗
# ============================================================
print("\n" + "=" * 60)
print("步骤2：数据清洗（去重 + 异常过滤）")
print("=" * 60)

# 2.1 去重：基于 behavior_id 去重，保留最新记录
# behavior_id 是入湖时生成的主键，理论上应当唯一；这里用
# dropDuplicates 按该键收拢，防御上游重试/重复提交造成的少量
# 重复行（同键记录内容一致，保留任意一条即可）。
deduped_df = ods_df.dropDuplicates(["behavior_id"])
print(f"去重后数据量: {deduped_df.count()} 条")

# 2.2 异常过滤
# 过滤空用户ID、空商品ID、空行为类型、无效时间戳
# 2.2 两层过滤叠加：
#   四个 isNotNull 挡掉关键字段缺失的记录——缺 user_id/item_id
#   无法关联维度，缺 ts 无法切会话，缺 behavior_type 无法归因；
#   isin(...) 把行为类型收敛到 pv/fav/cart/buy 四个合法枚举值，
#   其余一律视为脏数据剔除。
# 实测 1048576 -> 1048560，共剔除 16 条，脏数据占比极低，
# 符合天池数据集"仅需轻度清洗"的预期。
cleaned_df = deduped_df.filter(
    (F.col("user_id").isNotNull()) &
    (F.col("item_id").isNotNull()) &
    (F.col("behavior_type").isNotNull()) &
    (F.col("ts").isNotNull()) &
    (F.col("behavior_type").isin("pv", "fav", "cart", "buy"))
)
print(f"异常过滤后数据量: {cleaned_df.count()} 条")

# ============================================================
# 3. 维度关联与字段丰富
# ============================================================
print("\n" + "=" * 60)
print("步骤3：维度关联与字段丰富")
print("=" * 60)

# 3.1 行为编码映射：把字符串行为类型翻译成整数编码——
#   pv=1 浏览, fav=2 收藏, cart=3 加购, buy=4 购买，
#   otherwise(0) 兜底（上游已过滤，理论到不了，防御性置 0）。
# 整数编码比字符串省存储，且编码顺序恰好是"购买意向强度"的
# 递增序，下游做漏斗分析（浏览→收藏/加购→购买）时可直接按
# code 数值比较。
enriched_df = cleaned_df.withColumn(
    "behavior_code",
    F.when(F.col("behavior_type") == "pv", 1)
    .when(F.col("behavior_type") == "fav", 2)
    .when(F.col("behavior_type") == "cart", 3)
    .when(F.col("behavior_type") == "buy", 4)
    .otherwise(0)
)

# 3.2 日期代理键：从 event_time 截取 yyyyMMdd 生成 date_key，
# 即星型模型中事实表指向 dim_date 的外键。预计算成独立列，
# 下游不必再对时间字段做格式化函数计算。
enriched_df = enriched_df.withColumn(
    "date_key", F.date_format(F.col("event_time"), "yyyyMMdd")
)

# 3.3 小时提取：取 event_time 的小时数（0~23）落成独立列，
# 支撑"用户活跃时段分布""24 小时购买节奏"这类按小时切片的
# 分析，避免下游反复解析 event_time。
enriched_df = enriched_df.withColumn(
    "hour_of_day", F.hour(F.col("event_time"))
)

# 3.4 关联 dim_date（退化维度：补充分类名等可在此扩展）
# category_id 作为退化维度直接保留在事实表中
# 设计说明：date_key/hour_of_day 等日期属性已直接落在明细行上，
# 无需再 join dim_date——即日期属性"退化"进了事实表；category_id
# 在本数据集里没有名称信息，同样以原始 ID 退化保留，商品/类目
# 的描述性属性由下游分析时再关联 DIM 维表补齐。

print("字段丰富完成：behavior_code, date_key, hour_of_day")

# ============================================================
# 4. 会话划分（Session切分）
# ============================================================
print("\n" + "=" * 60)
print("步骤4：会话划分（Session切分）")
print("=" * 60)

# Session定义：同一用户连续两次行为间隔超过30分钟则切为新会话
# 使用窗口函数计算时间差
#
# ── 算法原理（三步走）────────────────────────────
# 业内通用的"超时会话切分"（sessionization）算法：
#   第1步 排序：把同一用户的所有行为按时间先后排成一条时间线；
#   第2步 找断点：计算每条行为与"上一条行为"的时间差，间隔超过
#         1800 秒（30 分钟）即认为用户已离开上次浏览场景，该行为
#         就是"新会话的第一条"，标记 is_new_session=1；
#   第3步 编号：对每个用户从头累加断点标记，累加值即为会话序号
#         ——标记每出现一次序号 +1，两个断点之间的行为序号相同，
#         天然归入同一会话。
# 为什么取 30 分钟：与 GA/神策等主流分析平台的默认口径一致
# （超过半小时无动作即视为一次访问结束），是行为分析的事实标准。

# ── 第1步：定义窗口——按用户分桶、按事件时间升序 ──
# partitionBy("user_id") 保证不同用户互不干扰（各自独立切会话），
# orderBy("ts") 让 lag() 取到的"上一条"在时间上严格相邻。
w = Window.partitionBy("user_id").orderBy("ts")

# ── 第2步：lag 取上一条行为的 ts，相减得时间差 ──
# lag("ts") 取同窗口内按排序的前一行 ts 值；每个用户的第 1 条
# 行为没有"上一条"，prev_ts 为 null。time_diff = 本条 ts - 上一条
# ts，单位为秒，含义是"距用户上一次动作过去了多久"。
session_df = enriched_df.withColumn(
    "prev_ts", F.lag("ts").over(w)
).withColumn(
    "time_diff", F.col("ts") - F.col("prev_ts")
)

# ── 第3步：标记会话断点 ──
# 判断是否为新会话起始（间隔>1800秒=30分钟 或 首条记录）：
#   time_diff 为 null  -> 该用户的第一条行为，必然开启新会话；
#   time_diff > 1800   -> 与上一条行为间隔超过 30 分钟，视为
#                         离开后重新回来，开启新会话；
#   其余（间隔 <= 1800 秒）-> 仍处于同一会话内，标记 0。
session_df = session_df.withColumn(
    "is_new_session",
    F.when(
        (F.col("time_diff").isNull()) | (F.col("time_diff") > 1800), 1
    ).otherwise(0)
)

# ── 第4步：断点累加编会话号，拼接 session_id ──
# 累加生成会话ID
# w2 与 w 的区别在于显式声明窗口框架 rowsBetween(unboundedPreceding,
# currentRow)：从"该用户第一条行为"一直累加到"当前行为"。对
# is_new_session 求和后，每个用户得到 1,1,...,2,2,...,3,... 这样的
# 序号序列——首条行为自身贡献一个 1，所以会话号从 1 开始；每遇
# 一个断点序号 +1，同一段内的行为序号相同，即为会话归属。
# session_id = "用户ID_s会话号"（如 151_s3 表示用户151的第3个会话），
# 全局唯一且可读：带上 user_id 前缀可跨用户防撞号，且一眼能看出
# 会话属于哪个用户。
w2 = Window.partitionBy("user_id").orderBy("ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
session_df = session_df.withColumn(
    "session_seq", F.sum("is_new_session").over(w2)
).withColumn(
    "session_id", F.concat(F.col("user_id").cast("string"), F.lit("_s"), F.col("session_seq").cast("string"))
)

# 标记会话起始：直接复用断点标记（1 = 本条是该会话的第一条行为）。
# 单独落一列的原因：下游统计"每天发起多少个会话"时，只需对
# is_session_start=1 的行计数即可，不必对 session_id 做昂贵的
# 去重计算。
session_df = session_df.withColumn(
    "is_session_start", F.col("is_new_session")
)

print(f"会话划分完成，会话数: {session_df.select('session_id').distinct().count()}")

# ============================================================
# 5. 选择最终字段并写入 DWD 表
# ============================================================
print("\n" + "=" * 60)
print("步骤5：写入 DWD 明细层")
print("=" * 60)

# 从会话切分结果中挑出最终入表字段，并按"主键→维度→度量→会话→
# 时间"的顺序排定列序；窗口切分产生的中间列 prev_ts / time_diff /
# is_new_session / session_seq 不入事实表（is_new_session 已收敛为
# is_session_start），保持 DWD 表结构干净、语义自解释。
dwd_df = session_df.select(
    "behavior_id",
    "user_id",
    "item_id",
    "category_id",
    "date_key",
    "behavior_type",
    "behavior_code",
    "event_time",
    "ts",
    "dt",
    "session_id",
    "is_session_start",
    "hour_of_day"
)

print(f"DWD层数据量: {dwd_df.count()} 条")
dwd_df.printSchema()
dwd_df.show(5, truncate=False)

# 写入 Hudi MOR 表（各 option 含义）：
#   table.type=MERGE_ON_READ —— 读时合并：写入先落增量 log 文件、
#     由后台异步合并，写入吞吐高，契合本表"大明细 + 日增量追加"
#     的写入模式（维表才选 COW，事实表选 MOR 是湖仓惯例）；
#   recordkey=behavior_id —— 行为唯一主键，upsert 保证重跑幂等；
#   partitionpath.field=dt —— 按业务日期分区，下游按天裁剪读取；
#   precombine.field=ts —— 同主键多版本时按秒级时间戳取最新；
#   cleaner.policy=KEEP_LATEST_COMMITS + retained=10 —— 旧文件
#     版本保留最近 10 个 commit，支持短程时间旅行回溯；
#   compact.inline=false —— 关闭写时同步内联压缩，避免本任务被
#     小文件合并阻塞（合并交给后续异步 compaction）。
dwd_df.write \
    .format("hudi") \
    .option("hoodie.table.name", "dwd_user_behavior_fact") \
    .option("hoodie.datasource.write.table.type", "MERGE_ON_READ") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .option("hoodie.datasource.write.recordkey.field", "behavior_id") \
    .option("hoodie.datasource.write.partitionpath.field", "dt") \
    .option("hoodie.datasource.write.precombine.field", "ts") \
    .option("hoodie.cleaner.policy", "KEEP_LATEST_COMMITS") \
    .option("hoodie.cleaner.commits.retained", "10") \
    .option("hoodie.compact.inline", "false") \
    .mode("append") \
    .save("/home/BaiMenghui/hudi-bigdata/warehouse/dwd_user_behavior_fact")

print("dwd_user_behavior_fact 写入完成")

# ============================================================
# 6. 数据校验
# ============================================================
print("\n" + "=" * 60)
print("步骤6：数据校验")
print("=" * 60)

# 条数核对：DWD 行数必须 <= ODS 行数。"只减不增"是清洗链路的
# 基本约束（去重 + 过滤只会减少行数）；若出现 DWD > ODS，多半
# 是重复跑批叠加写入，需要立即排查。
ods_count = ods_df.count()
dwd_count = spark.sql("SELECT COUNT(*) FROM dwd_user_behavior_fact").collect()[0][0]
print(f"数据量核对: ODS={ods_count}, DWD={dwd_count}")
if dwd_count <= ods_count:
    print("校验通过：DWD <= ODS（清洗去重后合理减少）")
else:
    print("警告：DWD > ODS，请检查")

# 行为类型分布：按 behavior_type 统计落表条数。用"浏览占绝对
# 大头（天池数据集典型占比约 94% 以上）、购买最少"的分布形状，
# 侧面验证清洗过滤没有误删某类行为。
print("\n行为类型分布:")
spark.sql("""
    SELECT behavior_type, COUNT(*) AS cnt
    FROM dwd_user_behavior_fact
    GROUP BY behavior_type
    ORDER BY cnt DESC
""").show()

# 会话统计：先按 session_id 数出每个会话包含的行为条数
# （session_size），再汇总出总会话数、平均会话深度（平均每个会话
# 几条行为）与最大会话深度——验证会话切分结果的规模是否合理
# （平均深度通常在个位数，若异常偏大说明断点判定失效）。
print("会话统计:")
spark.sql("""
    SELECT
        COUNT(DISTINCT session_id) AS total_sessions,
        ROUND(AVG(session_size), 2) AS avg_session_size,
        MAX(session_size) AS max_session_size
    FROM (
        SELECT session_id, COUNT(*) AS session_size
        FROM dwd_user_behavior_fact
        GROUP BY session_id
    )
""").show()

# 日分区完整性：按 dt 分组计数并按日期排序，逐日检查 25 个分区
# 是否都有数据、行数量级是否平稳——防止个别日期漏写或写丢。
print("日分区完整性:")
spark.sql("""
    SELECT dt, COUNT(*) AS cnt
    FROM dwd_user_behavior_fact
    GROUP BY dt
    ORDER BY dt
""").show()

spark.stop()
print("\nDWD层ETL完成！")
