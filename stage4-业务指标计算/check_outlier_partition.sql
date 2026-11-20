-- 异常分区 2018-08-28 各层数据量核查
SELECT 'dwd' AS layer, COUNT(*) AS cnt FROM dwd_user_behavior_fact WHERE dt = '2018-08-28'
UNION ALL
SELECT 'dws', COUNT(*) FROM dws_user_action_day WHERE dt = '2018-08-28'
UNION ALL
SELECT 'ads_dau', COUNT(*) FROM ads_dau_trend WHERE dt = '2018-08-28'
UNION ALL
SELECT 'ads_hour', COUNT(*) FROM ads_hour_heatmap WHERE dt = '2018-08-28';
