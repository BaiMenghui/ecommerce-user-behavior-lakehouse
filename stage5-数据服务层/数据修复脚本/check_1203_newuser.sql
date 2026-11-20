-- 核查 dt=2017-12-03 new_user_cnt=0 是否成立：用户全集 vs 12-03 前已出现用户
SELECT COUNT(DISTINCT user_id) AS total_users FROM dws_user_action_day;
SELECT COUNT(DISTINCT user_id) AS users_before_1203 FROM dws_user_action_day WHERE dt < '2017-12-03';
SELECT COUNT(DISTINCT user_id) AS users_on_1203 FROM dws_user_action_day WHERE dt = '2017-12-03';
