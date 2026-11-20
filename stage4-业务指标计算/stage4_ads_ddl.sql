-- ============================================================
-- 阶段4：ADS应用层 建表SQL（5张指标表 · Hudi COW）
-- 项目：电商用户行为湖仓分析平台
-- 日期：2026-10-22
-- 上游依赖：dws_user_action_day / dws_item_action_day /
--           dws_category_action_day（W5）+ dwd_user_behavior_fact（时段热力直通例外）
-- 优化目标：表结构扁平化、一行一指标结果，适配大屏高并发点查
-- ============================================================

-- ------------------------------------------------------------
-- 1. DAU趋势表 ads_dau_trend
--    粒度：一行 = 一天（大屏：DAU折线 + GMV数字卡 + 新老占比）
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_dau_trend (
    dt               STRING  COMMENT '日期（主键）',
    dau              BIGINT  COMMENT '日活跃用户数',
    new_user_cnt     BIGINT  COMMENT '新增用户数',
    old_user_cnt     BIGINT  COMMENT '老用户数',
    new_user_ratio   DOUBLE  COMMENT '新用户占比（%）',
    pv_cnt           BIGINT  COMMENT '当日浏览总量',
    fav_cnt          BIGINT  COMMENT '当日收藏总量',
    cart_cnt         BIGINT  COMMENT '当日加购总量',
    buy_cnt          BIGINT  COMMENT '当日购买总量（≈订单总量，模拟口径）',
    action_cnt       BIGINT  COMMENT '当日总行为量',
    paying_user_cnt  BIGINT  COMMENT '当日购买用户数（去重）',
    gmv              DOUBLE  COMMENT 'GMV（行为×模拟价格口径）',
    avg_ticket       DOUBLE  COMMENT '客单价 = GMV/购买用户数（模拟口径）'
)
USING hudi
PARTITIONED BY (dt)
OPTIONS (
    type = 'cow',
    primaryKey = 'dt',
    preCombineField = 'dt',
    hoodie.table.name = 'ads_dau_trend',
    hoodie.datasource.write.recordkey.field = 'dt',
    hoodie.datasource.write.partitionpath.field = 'dt',
    hoodie.datasource.write.precombine.field = 'dt',
    hoodie.datasource.write.operation = 'upsert',
    hoodie.datasource.write.table.type = 'COPY_ON_WRITE',
    hoodie.cleaner.policy = 'KEEP_LATEST_COMMITS',
    hoodie.cleaner.commits.retained = 10
);

-- ------------------------------------------------------------
-- 2. 转化漏斗表 ads_funnel
--    粒度：一行 = 一天（大屏：转化漏斗图）
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_funnel (
    dt            STRING  COMMENT '日期（主键）',
    pv_cnt        BIGINT  COMMENT '浏览量（漏斗起点）',
    fav_cnt       BIGINT  COMMENT '收藏量',
    cart_cnt      BIGINT  COMMENT '加购量',
    buy_cnt       BIGINT  COMMENT '购买量（漏斗终点）',
    pv2fav_rate   DOUBLE  COMMENT '浏览→收藏转化率（%）',
    pv2cart_rate  DOUBLE  COMMENT '浏览→加购转化率（%，三步漏斗）',
    cart2buy_rate DOUBLE  COMMENT '加购→购买转化率（%，三步漏斗）',
    pv2buy_rate   DOUBLE  COMMENT '浏览→购买总转化率（%）'
)
USING hudi
PARTITIONED BY (dt)
OPTIONS (
    type = 'cow',
    primaryKey = 'dt',
    preCombineField = 'dt',
    hoodie.table.name = 'ads_funnel',
    hoodie.datasource.write.recordkey.field = 'dt',
    hoodie.datasource.write.partitionpath.field = 'dt',
    hoodie.datasource.write.precombine.field = 'dt',
    hoodie.datasource.write.operation = 'upsert',
    hoodie.datasource.write.table.type = 'COPY_ON_WRITE',
    hoodie.cleaner.policy = 'KEEP_LATEST_COMMITS',
    hoodie.cleaner.commits.retained = 10
);

-- ------------------------------------------------------------
-- 3. 热销商品TopN表 ads_item_topn
--    粒度：一行 = 当日一个上榜商品，每日10行（大屏：排行榜柱状图）
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_item_topn (
    item_id      BIGINT  COMMENT '商品ID（分区内主键）',
    rank_no      INT     COMMENT '当日排名 1-10',
    category_id  BIGINT  COMMENT '所属类目ID',
    buy_cnt      BIGINT  COMMENT '当日购买次数',
    action_cnt   BIGINT  COMMENT '当日总行为次数',
    pv_user_cnt  BIGINT  COMMENT '曝光UV',
    buy_user_cnt BIGINT  COMMENT '购买UV',
    sim_price    BIGINT  COMMENT '模拟单价（pmod(hash(item_id),450)+50 元）',
    gmv          DOUBLE  COMMENT '该商品当日GMV（模拟口径）',
    dt                 STRING  COMMENT '日期分区'
)
USING hudi
PARTITIONED BY (dt)
OPTIONS (
    type = 'cow',
    primaryKey = 'item_id',
    preCombineField = 'dt',
    hoodie.table.name = 'ads_item_topn',
    hoodie.datasource.write.recordkey.field = 'item_id',
    hoodie.datasource.write.partitionpath.field = 'dt',
    hoodie.datasource.write.precombine.field = 'dt',
    hoodie.datasource.write.operation = 'upsert',
    hoodie.datasource.write.table.type = 'COPY_ON_WRITE',
    hoodie.cleaner.policy = 'KEEP_LATEST_COMMITS',
    hoodie.cleaner.commits.retained = 10
);

-- ------------------------------------------------------------
-- 4. 类目销售占比表 ads_category_share
--    粒度：一行 = 当日一个类目（大屏：类目占比饼图）
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_category_share (
    category_id        BIGINT  COMMENT '类目ID（分区内主键）',
    buy_cnt            BIGINT  COMMENT '当日购买次数',
    action_cnt         BIGINT  COMMENT '当日总行为次数',
    item_cnt           BIGINT  COMMENT '动销商品数',
    buy_share_ratio    DOUBLE  COMMENT '购买量占比（%）',
    action_share_ratio DOUBLE  COMMENT '行为量占比（%）',
    dt                 STRING  COMMENT '日期分区'
)
USING hudi
PARTITIONED BY (dt)
OPTIONS (
    type = 'cow',
    primaryKey = 'category_id',
    preCombineField = 'dt',
    hoodie.table.name = 'ads_category_share',
    hoodie.datasource.write.recordkey.field = 'category_id',
    hoodie.datasource.write.partitionpath.field = 'dt',
    hoodie.datasource.write.precombine.field = 'dt',
    hoodie.datasource.write.operation = 'upsert',
    hoodie.datasource.write.table.type = 'COPY_ON_WRITE',
    hoodie.cleaner.policy = 'KEEP_LATEST_COMMITS',
    hoodie.cleaner.commits.retained = 10
);

-- ------------------------------------------------------------
-- 5. 活跃时段热力表 ads_hour_heatmap
--    粒度：一行 = 当日一小时（大屏：小时热力图）
--    ⚠ 分层例外：由 DWD 明细按 hour_of_day 直通聚合（口径文档条款5）
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_hour_heatmap (
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
OPTIONS (
    type = 'cow',
    primaryKey = 'hour_of_day',
    preCombineField = 'dt',
    hoodie.table.name = 'ads_hour_heatmap',
    hoodie.datasource.write.recordkey.field = 'hour_of_day',
    hoodie.datasource.write.partitionpath.field = 'dt',
    hoodie.datasource.write.precombine.field = 'dt',
    hoodie.datasource.write.operation = 'upsert',
    hoodie.datasource.write.table.type = 'COPY_ON_WRITE',
    hoodie.cleaner.policy = 'KEEP_LATEST_COMMITS',
    hoodie.cleaner.commits.retained = 10
);
