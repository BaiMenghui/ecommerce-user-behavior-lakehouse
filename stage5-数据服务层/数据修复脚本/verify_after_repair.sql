-- ============================================================
-- 修复后验证：各表总量、异常分区残留、有效日期范围
-- 用法同 repair_remove_outlier.sql（-f 执行）
-- ============================================================

SELECT 'ods_total' AS item, CAST(COUNT(*) AS STRING) AS v FROM ods_user_behavior_log
UNION ALL SELECT 'dwd_total', CAST(COUNT(*) AS STRING) FROM dwd_user_behavior_fact
UNION ALL SELECT 'dws_user_total', CAST(COUNT(*) AS STRING) FROM dws_user_action_day
UNION ALL SELECT 'dws_item_total', CAST(COUNT(*) AS STRING) FROM dws_item_action_day
UNION ALL SELECT 'dws_cat_total', CAST(COUNT(*) AS STRING) FROM dws_category_action_day
UNION ALL SELECT 'ads_dau_total', CAST(COUNT(*) AS STRING) FROM ads_dau_trend
UNION ALL SELECT 'ads_funnel_total', CAST(COUNT(*) AS STRING) FROM ads_funnel
UNION ALL SELECT 'ads_topn_total', CAST(COUNT(*) AS STRING) FROM ads_item_topn
UNION ALL SELECT 'ads_share_total', CAST(COUNT(*) AS STRING) FROM ads_category_share
UNION ALL SELECT 'ads_heatmap_total', CAST(COUNT(*) AS STRING) FROM ads_hour_heatmap
UNION ALL SELECT 'outlier_ods_remain', CAST(COUNT(*) AS STRING) FROM ods_user_behavior_log WHERE dt = '2018-08-28'
UNION ALL SELECT 'outlier_dwd_remain', CAST(COUNT(*) AS STRING) FROM dwd_user_behavior_fact WHERE dt = '2018-08-28'
UNION ALL SELECT 'outlier_dws_remain', CAST(COUNT(*) AS STRING) FROM dws_user_action_day WHERE dt = '2018-08-28'
UNION ALL SELECT 'outlier_ads_dau_remain', CAST(COUNT(*) AS STRING) FROM ads_dau_trend WHERE dt = '2018-08-28'
UNION ALL SELECT 'valid_date_cnt', CAST(COUNT(DISTINCT dt) AS STRING) FROM dws_user_action_day
UNION ALL SELECT 'min_dt', CAST(MIN(dt) AS STRING) FROM dws_user_action_day
UNION ALL SELECT 'max_dt', CAST(MAX(dt) AS STRING) FROM dws_user_action_day;
