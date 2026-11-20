-- ============================================================
-- 阶段3（下）：DWS汇总层 建表SQL（3张主题宽表 · Hudi COW）
-- 项目：电商用户行为湖仓分析平台
-- 日期：2026-10-22
-- 粒度：实体×天（用户/商品/类目）
-- 表类型：COW（读多写少，每日批跑一次，下游ADS高频查询）
-- 上游依赖：dwd_user_behavior_fact（W4产出）
-- ============================================================

-- ------------------------------------------------------------
-- 1. 用户主题宽表 dws_user_action_day
--    粒度：一行 = 一个用户当天的行为汇总
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dws_user_action_day (
    user_id             BIGINT    COMMENT '用户ID（主键）',
    pv_cnt              BIGINT    COMMENT '当日浏览次数',
    fav_cnt             BIGINT    COMMENT '当日收藏次数',
    cart_cnt            BIGINT    COMMENT '当日加购次数',
    buy_cnt             BIGINT    COMMENT '当日购买次数',
    action_cnt          BIGINT    COMMENT '当日总行为数（四类之和）',
    session_cnt         BIGINT    COMMENT '当日会话数（distinct session_id）',
    avg_session_actions DOUBLE    COMMENT '平均每会话行为数（会话深度）',
    active_span_min     DOUBLE    COMMENT '首末行为跨度（分钟，近似活跃时长）',
    first_action_time   TIMESTAMP COMMENT '当日首次行为时间',
    last_action_time    TIMESTAMP COMMENT '当日末次行为时间',
    is_new_user         INT       COMMENT '是否新用户首日（1是/0否，全历史min(dt)=当日）',
    dt                  STRING    COMMENT '日期分区'
)
USING hudi
PARTITIONED BY (dt)
OPTIONS (
    type = 'cow',
    primaryKey = 'user_id',
    preCombineField = 'dt',
    hoodie.table.name = 'dws_user_action_day',
    hoodie.datasource.write.recordkey.field = 'user_id',
    hoodie.datasource.write.partitionpath.field = 'dt',
    hoodie.datasource.write.precombine.field = 'dt',
    hoodie.datasource.write.operation = 'upsert',
    hoodie.datasource.write.table.type = 'COPY_ON_WRITE',
    hoodie.cleaner.policy = 'KEEP_LATEST_COMMITS',
    hoodie.cleaner.commits.retained = 10
);

-- ------------------------------------------------------------
-- 2. 商品主题宽表 dws_item_action_day
--    粒度：一行 = 一个商品当天的行为汇总
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dws_item_action_day (
    item_id      BIGINT COMMENT '商品ID（主键）',
    category_id  BIGINT COMMENT '所属类目ID（维度退化自DWD）',
    pv_cnt       BIGINT COMMENT '当日浏览次数',
    fav_cnt      BIGINT COMMENT '当日收藏次数',
    cart_cnt     BIGINT COMMENT '当日加购次数',
    buy_cnt      BIGINT COMMENT '当日购买次数',
    action_cnt   BIGINT COMMENT '当日总行为数',
    pv_user_cnt  BIGINT COMMENT '当日浏览该商品的独立用户数（曝光UV）',
    buy_user_cnt BIGINT COMMENT '当日购买该商品的独立用户数（W6算客单价复用）',
    dt                  STRING    COMMENT '日期分区'
)
USING hudi
PARTITIONED BY (dt)
OPTIONS (
    type = 'cow',
    primaryKey = 'item_id',
    preCombineField = 'dt',
    hoodie.table.name = 'dws_item_action_day',
    hoodie.datasource.write.recordkey.field = 'item_id',
    hoodie.datasource.write.partitionpath.field = 'dt',
    hoodie.datasource.write.precombine.field = 'dt',
    hoodie.datasource.write.operation = 'upsert',
    hoodie.datasource.write.table.type = 'COPY_ON_WRITE',
    hoodie.cleaner.policy = 'KEEP_LATEST_COMMITS',
    hoodie.cleaner.commits.retained = 10
);

-- ------------------------------------------------------------
-- 3. 类目主题宽表 dws_category_action_day
--    粒度：一行 = 一个类目当天的行为汇总
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dws_category_action_day (
    category_id BIGINT COMMENT '类目ID（主键）',
    pv_cnt      BIGINT COMMENT '当日浏览次数',
    fav_cnt     BIGINT COMMENT '当日收藏次数',
    cart_cnt    BIGINT COMMENT '当日加购次数',
    buy_cnt     BIGINT COMMENT '当日购买次数',
    action_cnt  BIGINT COMMENT '当日总行为数',
    item_cnt    BIGINT COMMENT '当日有行为的商品数（类目动销宽度）',
    pv_user_cnt BIGINT COMMENT '当日浏览该类目的独立用户数',
    dt                  STRING    COMMENT '日期分区'
)
USING hudi
PARTITIONED BY (dt)
OPTIONS (
    type = 'cow',
    primaryKey = 'category_id',
    preCombineField = 'dt',
    hoodie.table.name = 'dws_category_action_day',
    hoodie.datasource.write.recordkey.field = 'category_id',
    hoodie.datasource.write.partitionpath.field = 'dt',
    hoodie.datasource.write.precombine.field = 'dt',
    hoodie.datasource.write.operation = 'upsert',
    hoodie.datasource.write.table.type = 'COPY_ON_WRITE',
    hoodie.cleaner.policy = 'KEEP_LATEST_COMMITS',
    hoodie.cleaner.commits.retained = 10
);
