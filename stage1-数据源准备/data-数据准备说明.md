# stage1-数据源准备 · 数据说明

因体积原因，原始数据与全量清洗分区不入库。请按以下步骤准备数据：

1. 从[阿里天池 UserBehavior 数据集](https://tianchi.aliyun.com/dataset/649)下载 `UserBehavior.csv`（约 3.5GB 全量，本课题使用其中 100 万条抽样，约 57MB）
2. 放置到本目录下的 `data/UserBehavior.csv`
3. 运行 `scripts/stage1_data_explore.py` 完成全量探查（产出 `exploration_report.json`）
4. 运行 `scripts/stage1_data_clean.py` 完成八步清洗，产出：
   - `data/cleaned/dt=YYYY-MM-DD/user_behavior.csv`（按日分区，共 25 个有效分区）
   - `清洗报告/cleaning_report.json` 与 `清洗报告/partition_summary.csv`

## 已入库的过程资产

- `清洗报告/`：1,048,576 条基准的四项校验结果（记录数 / 缺失值 / 重复值 / 行为类型，全 PASS）
- `exploration_report.json`：数据探查报告（用户/商品规模、行为分布、稀疏度）
- `异常样本-outlier_2018-08-28.csv`：游离异常分区 16 条原始记录（清洗层保留血缘、消费层过滤的治理对象）

## 关键口径

- 行为类型限定：`pv / fav / cart / buy`
- 时间跨度：2017-09-11 ~ 2018-08-28，主分析期为 2017-11-10 ~ 2017-12-03
- 已知异常：`dt=2018-08-28` 仅 16 条、游离主分析期 8 个月，按「清洗层保留、ADS/CSV 导出双保险过滤」处理
