-- ============================================================
-- ads_hour_heatmap 表重建注册脚本（EXTERNAL，指向已有数据）
-- 背景: 重刷前 DROP 了 metastore 注册, 数据目录由 ETL 重新写入,
--       managed 建表与现存目录冲突, 故用 EXTERNAL 注册
-- ============================================================

CREATE EXTERNAL TABLE IF NOT EXISTS ads_hour_heatmap (
    hour_of_day INT     COMMENT '小时 0-23（分区内主键）',
    action_cnt  BIGINT  COMMENT '该小时总行为量',
    pv_cnt      BIGINT  COMMENT '该小时浏览量',
    fav_cnt     BIGINT  COMMENT '该小时收藏量',
    cart_cnt    BIGINT  COMMENT '该小时加购量',
    buy_cnt     BIGINT  COMMENT '该小时购买量',
    dt                 STRING  COMMENT '日期分区'
)
USING hudi
PARTITIONED BY (dt)
LOCATION '/home/BaiMenghui/hudi-bigdata/warehouse/ads_hour_heatmap'
OPTIONS (
    type = 'cow',
    primaryKey = 'hour_of_day',
    preCombineField = 'dt',
    hoodie.table.name = 'ads_hour_heatmap',
    hoodie.datasource.write.recordkey.field = 'hour_of_day',
    hoodie.datasource.write.partitionpath.field = 'dt',
    hoodie.datasource.write.precombine.field = 'dt',
    hoodie.datasource.write.table.type = 'COPY_ON_WRITE'
);
