-- ============================================================
-- 数据质量修复（续）：剩余 5 张 ADS 表剔除异常分区 dt='2018-08-28'
-- 说明：metastore 中 ads 表 order_cnt 列类型与 Hudi schema 不兼容，
--       导致 DELETE 提交后的 metastore schema 同步失败并中断。
--       数据 commit 在 sync 之前已完成，数据层不受影响。
--       此处用 SET 关闭 hive sync（spark-sql 不接受 hoodie.* 的 --conf），
--       HoodieCatalog 读取走 Hudi 自身元数据，不依赖 HMS schema。
-- ============================================================

SET hoodie.datasource.hive_sync.enable=false;

DELETE FROM ads_dau_trend      WHERE dt = '2018-08-28';
DELETE FROM ads_funnel         WHERE dt = '2018-08-28';
DELETE FROM ads_item_topn      WHERE dt = '2018-08-28';
DELETE FROM ads_category_share WHERE dt = '2018-08-28';
DELETE FROM ads_hour_heatmap   WHERE dt = '2018-08-28';
