-- 异常分区 2018-08-28 全层清理（口径: 数据源外异常时间戳, 逻辑剔除）
DELETE FROM dwd_user_behavior_fact WHERE dt = '2018-08-28';
DELETE FROM dws_user_action_day WHERE dt = '2018-08-28';
DELETE FROM ads_dau_trend WHERE dt = '2018-08-28';
DELETE FROM ads_funnel WHERE dt = '2018-08-28';
DELETE FROM ads_item_topn WHERE dt = '2018-08-28';
DELETE FROM ads_category_share WHERE dt = '2018-08-28';
DELETE FROM ads_hour_heatmap WHERE dt = '2018-08-28';
