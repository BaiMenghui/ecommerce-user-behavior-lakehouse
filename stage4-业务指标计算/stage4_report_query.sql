-- ============================================================
-- 阶段4 汇报回填取数脚本：dt=2017-11-25 关键实测指标
-- 用法（与 ETL 相同 conf，串行执行）：
--   spark-sql --master local[2] --jars $HOME/hudi-bigdata/jars/hudi-spark3.3-bundle_2.12-0.14.1.jar \
--     --conf spark.sql.warehouse.dir=$HOME/hudi-bigdata/warehouse \
--     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
--     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--     --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
--     -f stage4_report_query.sql
-- ============================================================

SELECT 'dwd_行为分布' AS item, behavior_type AS k, CAST(COUNT(*) AS STRING) AS v
FROM dwd_user_behavior_fact WHERE dt = '2017-11-25' GROUP BY behavior_type
UNION ALL
SELECT 'ads_DAU', 'dau', CAST(dau AS STRING) FROM ads_dau_trend WHERE dt = '2017-11-25'
UNION ALL SELECT 'ads_DAU', 'new_user_cnt', CAST(new_user_cnt AS STRING) FROM ads_dau_trend WHERE dt = '2017-11-25'
UNION ALL SELECT 'ads_DAU', 'old_user_cnt', CAST(old_user_cnt AS STRING) FROM ads_dau_trend WHERE dt = '2017-11-25'
UNION ALL SELECT 'ads_DAU', 'order_cnt', CAST(order_cnt AS STRING) FROM ads_dau_trend WHERE dt = '2017-11-25'
UNION ALL SELECT 'ads_DAU', 'paying_user_cnt', CAST(paying_user_cnt AS STRING) FROM ads_dau_trend WHERE dt = '2017-11-25'
UNION ALL SELECT 'ads_DAU', 'gmv', CAST(gmv AS STRING) FROM ads_dau_trend WHERE dt = '2017-11-25'
UNION ALL SELECT 'ads_DAU', 'avg_ticket', CAST(avg_ticket AS STRING) FROM ads_dau_trend WHERE dt = '2017-11-25'
UNION ALL SELECT 'ads_funnel', 'pv_cnt', CAST(pv_cnt AS STRING) FROM ads_funnel WHERE dt = '2017-11-25'
UNION ALL SELECT 'ads_funnel', 'fav_cnt', CAST(fav_cnt AS STRING) FROM ads_funnel WHERE dt = '2017-11-25'
UNION ALL SELECT 'ads_funnel', 'cart_cnt', CAST(cart_cnt AS STRING) FROM ads_funnel WHERE dt = '2017-11-25'
UNION ALL SELECT 'ads_funnel', 'buy_cnt', CAST(buy_cnt AS STRING) FROM ads_funnel WHERE dt = '2017-11-25'
UNION ALL SELECT 'ads_funnel', 'pv2cart_rate', CAST(pv2cart_rate AS STRING) FROM ads_funnel WHERE dt = '2017-11-25'
UNION ALL SELECT 'ads_funnel', 'cart2buy_rate', CAST(cart2buy_rate AS STRING) FROM ads_funnel WHERE dt = '2017-11-25'
UNION ALL SELECT 'ads_funnel', 'pv2buy_rate', CAST(pv2buy_rate AS STRING) FROM ads_funnel WHERE dt = '2017-11-25'
UNION ALL SELECT '当日行数', 'dws_user_action_day', CAST(COUNT(*) AS STRING) FROM dws_user_action_day WHERE dt = '2017-11-25'
UNION ALL SELECT '当日行数', 'dws_item_action_day', CAST(COUNT(*) AS STRING) FROM dws_item_action_day WHERE dt = '2017-11-25'
UNION ALL SELECT '当日行数', 'dws_category_action_day', CAST(COUNT(*) AS STRING) FROM dws_category_action_day WHERE dt = '2017-11-25'
UNION ALL SELECT '当日行数', 'ads_item_topn', CAST(COUNT(*) AS STRING) FROM ads_item_topn WHERE dt = '2017-11-25'
UNION ALL SELECT '当日行数', 'ads_category_share', CAST(COUNT(*) AS STRING) FROM ads_category_share WHERE dt = '2017-11-25'
UNION ALL SELECT '当日行数', 'ads_hour_heatmap', CAST(COUNT(*) AS STRING) FROM ads_hour_heatmap WHERE dt = '2017-11-25'
UNION ALL SELECT '占比核对', 'buy_share_ratio_sum', CAST(ROUND(SUM(buy_share_ratio), 2) AS STRING) FROM ads_category_share WHERE dt = '2017-11-25';
