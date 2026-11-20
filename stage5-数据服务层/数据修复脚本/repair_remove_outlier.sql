-- ============================================================
-- 数据质量修复：剔除异常时间戳分区 dt='2018-08-28'
-- 背景：源数据混入 1 个用户 16 条行为、时间戳为 2018-08-28（超出
--       UserBehavior 数据集时间窗），经 ODS→DWD→DWS→ADS 全链路带入。
-- 方式：利用 Hudi 事务性 DELETE 原位回滚（对应 Hive 需重写分区的场景），
--       修复后各表仍保持快照一致性，无需重算历史层。
-- 执行（与 ETL 相同 conf，串行）：
--   spark-sql --master local[2] --jars $HOME/hudi-bigdata/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar \
--     --conf spark.sql.warehouse.dir=$HOME/hudi-bigdata/warehouse \
--     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
--     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--     --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
--     -f repair_remove_outlier.sql
-- ============================================================

DELETE FROM ods_user_behavior_log    WHERE dt = '2018-08-28';
DELETE FROM dwd_user_behavior_fact   WHERE dt = '2018-08-28';
DELETE FROM dws_user_action_day      WHERE dt = '2018-08-28';
DELETE FROM dws_item_action_day      WHERE dt = '2018-08-28';
DELETE FROM dws_category_action_day  WHERE dt = '2018-08-28';
DELETE FROM ads_dau_trend            WHERE dt = '2018-08-28';
DELETE FROM ads_funnel               WHERE dt = '2018-08-28';
DELETE FROM ads_item_topn            WHERE dt = '2018-08-28';
DELETE FROM ads_category_share       WHERE dt = '2018-08-28';
DELETE FROM ads_hour_heatmap         WHERE dt = '2018-08-28';
