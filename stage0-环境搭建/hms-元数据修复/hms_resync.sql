-- ============================================================
-- HMS(Derby) ads 表 schema 重同步脚本（备用，未执行）
-- 日期：2026-10-28
-- 背景：本脚本按任务要求生成。执行前直查 Derby 确认 5 张 ads 表
--       的 HMS schema 已与 Hudi 数据 schema 完全一致
--       （指标列名为 buy_cnt，无 order_cnt，gmv/avg_ticket 均为 double），
--       且 spark-sql 直查 COUNT 全部通过（24/24/154/32317/284），
--       故 DROP/CREATE 重同步无需执行，留作日后元数据漂移时备用。
-- 用法：bash ~/hudi-bigdata/sql/run_spark_sql.sh \
--         ~/hudi-bigdata/sql/hms_resync.sql <日志路径>
-- 说明：DROP TABLE 仅删除 HMS 元数据，不删除 warehouse 下的数据文件；
--       CREATE TABLE ... USING hudi LOCATION 会从数据文件自动推断 schema
--       并注册回 HMS（catalogImplementation=hive 会话中执行）。
-- ============================================================

DROP TABLE IF EXISTS ads_dau_trend;
CREATE TABLE ads_dau_trend USING hudi LOCATION '/home/BaiMenghui/hudi-bigdata/warehouse/ads_dau_trend';

DROP TABLE IF EXISTS ads_funnel;
CREATE TABLE ads_funnel USING hudi LOCATION '/home/BaiMenghui/hudi-bigdata/warehouse/ads_funnel';

DROP TABLE IF EXISTS ads_item_topn;
CREATE TABLE ads_item_topn USING hudi LOCATION '/home/BaiMenghui/hudi-bigdata/warehouse/ads_item_topn';

DROP TABLE IF EXISTS ads_category_share;
CREATE TABLE ads_category_share USING hudi LOCATION '/home/BaiMenghui/hudi-bigdata/warehouse/ads_category_share';

DROP TABLE IF EXISTS ads_hour_heatmap;
CREATE TABLE ads_hour_heatmap USING hudi LOCATION '/home/BaiMenghui/hudi-bigdata/warehouse/ads_hour_heatmap';
