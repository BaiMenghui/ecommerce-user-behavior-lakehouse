#!/usr/bin/env python3
# ============================================================
# 脚本名称: stage1_data_explore.py
# 所属阶段: 阶段1 —— 数据探查(清洗前的数据摸底)
# 功能说明: 读取天池 UserBehavior 电商用户行为原始 CSV, 为数据集建立
#           全景画像: 统计总行数与字段结构、用户/商品/类目去重基数、
#           行为类型(pv/fav/cart/buy)分布、缺失值/重复记录、非法行为
#           类型与时间戳越界情况、用户-商品交互矩阵稀疏度、用户行为量
#           与每日记录分布, 为后续清洗规则与按日分区设计提供量化依据
# 输入    : ~/hudi-bigdata/data/UserBehavior.csv (天池原始数据, 无表头,
#           6 列: 用户id, 商品id, 类目id, 行为类型, 时间戳(秒), 日期字符串)
# 输出    : ~/hudi-bigdata/data/exploration/exploration_report.json (探查报告)
#           ~/hudi-bigdata/data/exploration/daily_distribution.csv (每日记录数)
# 运行方式: wsl bash -c "python3 scripts/stage1_data_explore.py"
# ============================================================
"""
Stage 1: Data Exploration Script
E-commerce User Behavior Lakehouse Analysis Platform
Dataset: Alibaba Tianchi UserBehavior
"""

# 依赖库: pandas 承担数据加载与全部统计计算, numpy 提供数值运算支撑,
# os 处理路径与建目录, json 序列化探查报告, datetime 记录探查时间
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime

# 路径配置: 原始数据文件与探查结果输出目录(WSL 家目录下),
# 输出目录不存在则自动创建, 保证脚本可重复执行
DATA_FILE = os.path.expanduser('~/hudi-bigdata/data/UserBehavior.csv')
OUTPUT_DIR = os.path.expanduser('~/hudi-bigdata/data/exploration')
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("=" * 60)
print("Stage 1: Data Exploration - UserBehavior Dataset")
print("=" * 60)

# ============================================================
# 1. Load Data
# ============================================================
print("\n[1] Loading data...")
# 步骤1: 加载原始 CSV —— 原始文件无表头, 手工指定 6 列字段名;
# 显式声明 dtype 防止 pandas 把大整数 ID 自动推断成 float64 丢精度,
# 行为类型与日期字符串保持 str 类型
df = pd.read_csv(
    DATA_FILE,
    header=None,
    names=['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp', 'datetime_str'],
    dtype={
        'user_id': 'int64',
        'item_id': 'int64',
        'category_id': 'int64',
        'behavior_type': 'str',
        'timestamp': 'int64',
        'datetime_str': 'str'
    }
)
print(f"  Total records: {len(df):,}")
print(f"  Columns: {list(df.columns)}")
print(f"  Data types:\n{df.dtypes.to_string()}")
print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

# ============================================================
# 2. Scale Statistics
# ============================================================
print("\n[2] Scale Statistics")
print("-" * 40)

# 规模统计: 用 nunique 分别对用户/商品/类目做去重计数, 得到三个维度
# 的基数; 再结合总记录数算出人均行为量等均值, 衡量数据集规模能否
# 支撑后续的漏斗转化与留存类分析
total_records = len(df)
unique_users = df['user_id'].nunique()
unique_items = df['item_id'].nunique()
unique_categories = df['category_id'].nunique()

print(f"  Total behavior records: {total_records:,}")
print(f"  Unique users: {unique_users:,}")
print(f"  Unique items: {unique_items:,}")
print(f"  Unique categories: {unique_categories:,}")
print(f"  Avg records per user: {total_records / unique_users:.2f}")
print(f"  Avg records per item: {total_records / unique_items:.2f}")
print(f"  Avg records per category: {total_records / unique_categories:.2f}")

# ============================================================
# 3. Behavior Type Distribution
# ============================================================
print("\n[3] Behavior Type Distribution")
print("-" * 40)

# 行为类型分布: value_counts 统计四种行为的绝对条数,
# value_counts(normalize=True) 再换算成百分比; 该分布是电商转化漏斗
# (浏览 pv -> 收藏/加购 fav/cart -> 购买 buy)分析的基准
behavior_counts = df['behavior_type'].value_counts()
behavior_pcts = df['behavior_type'].value_counts(normalize=True) * 100

# 按固定的 pv/fav/cart/buy 顺序输出, 顺序稳定便于与探查报告 JSON 对照
print(f"  {'Type':<10} {'Count':>15} {'Percentage':>12}")
print(f"  {'-'*10} {'-'*15} {'-'*12}")
for bt in ['pv', 'fav', 'cart', 'buy']:
    count = behavior_counts.get(bt, 0)
    pct = behavior_pcts.get(bt, 0)
    print(f"  {bt:<10} {count:>15,} {pct:>11.2f}%")

# 打印行为类型含义对照表(pv=浏览, fav=收藏, cart=加购, buy=购买),
# 答辩讲解字段含义时可直接引用
behavior_map = {'pv': 'Browse', 'fav': 'Favorite', 'cart': 'Add to Cart', 'buy': 'Purchase'}
print(f"\n  Behavior type mapping:")
for k, v in behavior_map.items():
    print(f"    {k} = {v}")

# ============================================================
# 4. Data Quality Check
# ============================================================
print("\n[4] Data Quality Check")
print("-" * 40)

# 4.1 Missing values
print("\n  [4.1] Missing Values:")
# 缺失值检查: isnull().sum() 逐列统计空值条数并换算占比; 若关键列
# 存在缺失, 清洗阶段需整行剔除, 此处先摸清缺失规模
missing_stats = df.isnull().sum()
for col in df.columns:
    missing_count = missing_stats[col]
    missing_pct = (missing_count / total_records) * 100
    status = "OK" if missing_count == 0 else f"FOUND {missing_count}"
    print(f"    {col:<15} missing: {missing_count:>10} ({missing_pct:.4f}%) [{status}]")

# 4.2 Duplicate values
print("\n  [4.2] Duplicate Records:")
# 重复记录检查 —— 分两级: ① 整行完全重复(duplicated() 不带参数, 六列
# 全部相同); ② 仅按 (user_id, item_id, timestamp) 三个关键字段判定的
# "语义重复", 该组合相同即视为同一条行为, 即使其余字段不同,
# 清洗阶段也会按关键字段去重
dup_count = df.duplicated().sum()
dup_pct = (dup_count / total_records) * 100
print(f"    Fully duplicated rows: {dup_count:,} ({dup_pct:.4f}%)")

# Check duplicates on key fields (user_id, item_id, timestamp)
dup_key = df.duplicated(subset=['user_id', 'item_id', 'timestamp']).sum()
dup_key_pct = (dup_key / total_records) * 100
print(f"    Duplicates on (user_id, item_id, timestamp): {dup_key:,} ({dup_key_pct:.4f}%)")

# 4.3 Anomaly detection
print("\n  [4.3] Anomaly Detection:")

# 异常检测: 逐项校验业务合法性 —— ID 必须为正、行为类型必须落在
# 四类白名单内, 校验结果直接决定清洗脚本的过滤规则

# Check for negative user_ids
# 统计非正数(<=0)的 user_id 记录条数: 非法账号 ID 属于脏数据
neg_users = (df['user_id'] <= 0).sum()
print(f"    Non-positive user_id: {neg_users}")

# Check for negative item_ids
# 统计非正数(<=0)的 item_id 记录条数: 非法商品 ID 属于脏数据
neg_items = (df['item_id'] <= 0).sum()
print(f"    Non-positive item_id: {neg_items}")

# Check for invalid behavior types
# 行为类型白名单校验: 找出白名单外的记录并列出具体取值, 便于定位脏数据
valid_behaviors = {'pv', 'fav', 'cart', 'buy'}
invalid_behaviors = df[~df['behavior_type'].isin(valid_behaviors)]
print(f"    Invalid behavior_type: {len(invalid_behaviors)}")
if len(invalid_behaviors) > 0:
    print(f"    Invalid values: {invalid_behaviors['behavior_type'].unique()}")

# Check timestamp range
# 打印原始秒级时间戳的 min/max, 用于与派生事件时间范围交叉核对
print(f"    Timestamp range: {df['timestamp'].min()} ~ {df['timestamp'].max()}")

# Convert timestamp to datetime for range check
# 把秒级 Unix 时间戳转成 datetime 得到事件时间范围与时间跨度(天数);
# 若数据中混有异常时间戳, 此处的时间范围会明显偏离数据集官方口径
df['event_time'] = pd.to_datetime(df['timestamp'], unit='s')
min_time = df['event_time'].min()
max_time = df['event_time'].max()
print(f"    Event time range: {min_time} ~ {max_time}")
print(f"    Time span: {(max_time - min_time).days} days")

# 4.4 Outlier detection on timestamp (future dates or very old dates)
print("\n  [4.4] Timestamp Outliers:")
# 时间戳越界检测: 以 2017-01-01 ~ 2019-12-31 为合理时间窗口, 统计
# 落在窗口外的记录数与占比; 探查发现的越界/异常时间数据会在清洗
# 阶段被时间戳区间过滤整批剔除, 不进入 cleaned 目录
# Define reasonable range (2017-01-01 to 2019-12-31)
reasonable_start = pd.Timestamp('2017-01-01')
reasonable_end = pd.Timestamp('2019-12-31')
out_of_range = df[(df['event_time'] < reasonable_start) | (df['event_time'] > reasonable_end)]
print(f"    Out-of-range timestamps: {len(out_of_range)} ({len(out_of_range)/total_records*100:.4f}%)")

# ============================================================
# 5. User-Item Interaction Matrix Sparsity
# ============================================================
print("\n[5] User-Item Interaction Matrix Sparsity")
print("-" * 40)

# 用户-商品交互矩阵稀疏度: 理论矩阵规模 = 用户数 x 商品数;
# groupby(['user_id','item_id']).ngroups 统计实际出现过的 (用户,商品)
# 组合数; 稀疏度 = 1 - 实际交互/理论格子数, 量化"用户只与极少数商品
# 发生交互"的程度, 为协同过滤等推荐类分析的可行性提供依据
matrix_size = unique_users * unique_items
actual_interactions = df.groupby(['user_id', 'item_id']).ngroups
sparsity = 1 - (actual_interactions / matrix_size)

print(f"  Matrix dimensions: {unique_users:,} x {unique_items:,}")
print(f"  Matrix total cells: {matrix_size:,}")
print(f"  Actual interactions (unique user-item pairs): {actual_interactions:,}")
print(f"  Sparsity: {sparsity:.10f} ({sparsity*100:.8f}%)")
print(f"  Density: {1-sparsity:.10f} ({(1-sparsity)*100:.8f}%)")

# ============================================================
# 6. Additional Statistics
# ============================================================
print("\n[6] Additional Statistics")
print("-" * 40)

# User behavior count distribution
# 用户维度行为量分布: 每个用户的行为条数(最小/最大/均值/中位数/标准差),
# 刻画活跃度的长尾特征 —— 少数活跃用户贡献了大部分行为记录
user_behavior_counts = df.groupby('user_id').size()
print(f"  User behavior count stats:")
print(f"    Min: {user_behavior_counts.min()}")
print(f"    Max: {user_behavior_counts.max()}")
print(f"    Mean: {user_behavior_counts.mean():.2f}")
print(f"    Median: {user_behavior_counts.median():.2f}")
print(f"    Std: {user_behavior_counts.std():.2f}")

# Daily record distribution
# 每日记录分布: 从事件时间提取日期, 按天分组计数, 得到数据的时间
# 覆盖范围与每日体量 —— 该分布直接决定清洗阶段的按日分区设计
df['dt'] = df['event_time'].dt.date
daily_counts = df.groupby('dt').size()
print(f"\n  Daily record stats:")
print(f"    Days: {len(daily_counts)}")
print(f"    Min records/day: {daily_counts.min():,}")
print(f"    Max records/day: {daily_counts.max():,}")
print(f"    Mean records/day: {daily_counts.mean():,.2f}")

# Top 10 most active users
# 行为量 Top10 的最活跃用户(贡献行为条数最多的用户)
print(f"\n  Top 10 most active users:")
top_users = user_behavior_counts.nlargest(10)
for uid, count in top_users.items():
    print(f"    User {uid}: {count:,} records")

# Top 10 most popular items
# 被(所有类型)行为次数 Top10 的热门商品
item_counts = df.groupby('item_id').size()
print(f"\n  Top 10 most popular items:")
top_items = item_counts.nlargest(10)
for iid, count in top_items.items():
    print(f"    Item {iid}: {count:,} records")

# ============================================================
# 7. Save Exploration Report as JSON
# ============================================================
print("\n[7] Saving exploration report...")

# 步骤7: 汇总探查报告 —— 把前面各步骤的统计值组装成结构化字典;
# numpy 数值统一转成 int/float 原生类型, 否则 json.dump 无法序列化
report = {
    "dataset": "UserBehavior",
    "source": "Alibaba Tianchi (dataId=649)",
    "exploration_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "total_records": total_records,
    "unique_users": int(unique_users),
    "unique_items": int(unique_items),
    "unique_categories": int(unique_categories),
    "behavior_distribution": {bt: int(behavior_counts.get(bt, 0)) for bt in ['pv', 'fav', 'cart', 'buy']},
    "behavior_percentage": {bt: float(behavior_pcts.get(bt, 0)) for bt in ['pv', 'fav', 'cart', 'buy']},
    "missing_values": {col: int(missing_stats[col]) for col in df.columns if col not in ['event_time', 'dt']},
    "duplicate_rows": int(dup_count),
    "duplicate_on_key": int(dup_key),
    "invalid_behavior_types": int(len(invalid_behaviors)),
    "timestamp_range": {
        "min": int(df['timestamp'].min()),
        "max": int(df['timestamp'].max()),
        "min_datetime": str(min_time),
        "max_datetime": str(max_time),
        "out_of_range_count": int(len(out_of_range))
    },
    "interaction_matrix": {
        "users": int(unique_users),
        "items": int(unique_items),
        "total_cells": int(matrix_size),
        "actual_interactions": int(actual_interactions),
        "sparsity": float(sparsity),
        "density": float(1 - sparsity)
    },
    "user_behavior_stats": {
        "min": int(user_behavior_counts.min()),
        "max": int(user_behavior_counts.max()),
        "mean": float(user_behavior_counts.mean()),
        "median": float(user_behavior_counts.median()),
        "std": float(user_behavior_counts.std())
    },
    "daily_stats": {
        "days": int(len(daily_counts)),
        "min_per_day": int(daily_counts.min()),
        "max_per_day": int(daily_counts.max()),
        "mean_per_day": float(daily_counts.mean())
    }
}

# 报告写入 exploration_report.json(indent=2 美化缩进, ensure_ascii=False
# 保留非 ASCII 可读性), 作为阶段1探查结论的留档
report_path = os.path.join(OUTPUT_DIR, 'exploration_report.json')
with open(report_path, 'w', encoding='utf-8') as f:
    json.dump(report, f, indent=2, ensure_ascii=False)
print(f"  Report saved to: {report_path}")

# Save daily distribution CSV
# 每日记录分布另存 CSV(date + record_count 两列): 供后续画每日趋势图
# 或核对清洗阶段各日分区大小
daily_df = daily_counts.reset_index()
daily_df.columns = ['date', 'record_count']
daily_path = os.path.join(OUTPUT_DIR, 'daily_distribution.csv')
daily_df.to_csv(daily_path, index=False)
print(f"  Daily distribution saved to: {daily_path}")

print("\n" + "=" * 60)
print("Data Exploration Complete!")
print("=" * 60)
