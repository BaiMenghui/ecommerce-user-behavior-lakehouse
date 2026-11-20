-- 全链路数据快照核查（阶段1~4 重跑后）
SELECT 'ods_total' AS metric, COUNT(*) AS cnt FROM ods_user_behavior_log
UNION ALL
SELECT 'dim_user', COUNT(*) FROM dim_user
UNION ALL
SELECT 'dim_item', COUNT(*) FROM dim_item
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'dwd_total', COUNT(*) FROM dwd_user_behavior_fact
UNION ALL
SELECT 'dws_user_rows', COUNT(*) FROM dws_user_action_day
UNION ALL
SELECT 'dws_item_rows', COUNT(*) FROM dws_item_action_day
UNION ALL
SELECT 'dws_category_rows', COUNT(*) FROM dws_category_action_day
UNION ALL
SELECT 'ads_dau_rows', COUNT(*) FROM ads_dau_trend
UNION ALL
SELECT 'ads_funnel_rows', COUNT(*) FROM ads_funnel
UNION ALL
SELECT 'ads_topn_rows', COUNT(*) FROM ads_item_topn
UNION ALL
SELECT 'ads_share_rows', COUNT(*) FROM ads_category_share
UNION ALL
SELECT 'ads_hour_rows', COUNT(*) FROM ads_hour_heatmap
UNION ALL
SELECT 'outlier_dwd', COUNT(*) FROM dwd_user_behavior_fact WHERE dt = '2018-08-28'
UNION ALL
SELECT 'outlier_ads_dau', COUNT(*) FROM ads_dau_trend WHERE dt = '2018-08-28'
UNION ALL
SELECT 'outlier_ads_hour', COUNT(*) FROM ads_hour_heatmap WHERE dt = '2018-08-28';
