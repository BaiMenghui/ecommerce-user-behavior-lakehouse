-- ============================================================
-- 阶段3（上）：DIM维度层 + DWD明细层 建表SQL
-- 项目：电商用户行为湖仓分析平台
-- 日期：2026-10-12
-- 理论依据：Kimball维度建模
-- ============================================================

-- ============================================================
-- 第一层：DIM 维度层（Hudi COW 表）
-- ============================================================

-- ------------------------------------------------------------
-- 1. 用户维度表 dim_user
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_user (
    user_id          BIGINT   COMMENT '用户ID（主键）',
    user_key         STRING   COMMENT '用户代理键（UUID）',
    first_active_dt  STRING   COMMENT '首次活跃日期',
    last_active_dt   STRING   COMMENT '末次活跃日期',
    total_pv_cnt     BIGINT   COMMENT '累计浏览次数',
    total_fav_cnt    BIGINT   COMMENT '累计收藏次数',
    total_cart_cnt   BIGINT   COMMENT '累计加购次数',
    total_buy_cnt    BIGINT   COMMENT '累计购买次数',
    user_level       STRING   COMMENT '用户等级（new/active/dormant）',
    update_dt        STRING   COMMENT '更新日期'
)
USING hudi
PARTITIONED BY (update_dt)
OPTIONS (
    type = 'cow',
    primaryKey = 'user_id',
    preCombineField = 'last_active_dt',
    hoodie.table.name = 'dim_user',
    hoodie.datasource.write.recordkey.field = 'user_id',
    hoodie.datasource.write.partitionpath.field = 'update_dt',
    hoodie.datasource.write.precombine.field = 'last_active_dt',
    hoodie.datasource.write.operation = 'upsert',
    hoodie.datasource.write.table.type = 'COPY_ON_WRITE',
    hoodie.cleaner.policy = 'KEEP_LATEST_COMMITS',
    hoodie.cleaner.commits.retained = 10
);

-- ------------------------------------------------------------
-- 2. 商品维度表 dim_item
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_item (
    item_id           BIGINT   COMMENT '商品ID（主键）',
    item_key          STRING   COMMENT '商品代理键（UUID）',
    category_id       BIGINT   COMMENT '类目ID',
    category_name     STRING   COMMENT '类目名称（退化属性）',
    first_exposure_dt STRING   COMMENT '首次曝光日期',
    last_exposure_dt  STRING   COMMENT '末次曝光日期',
    total_pv_cnt      BIGINT   COMMENT '累计被浏览次数',
    total_fav_cnt     BIGINT   COMMENT '累计被收藏次数',
    total_cart_cnt    BIGINT   COMMENT '累计被加购次数',
    total_buy_cnt     BIGINT   COMMENT '累计被购买次数',
    item_status       STRING   COMMENT '商品状态（new/hot/long_tail）',
    update_dt         STRING   COMMENT '更新日期'
)
USING hudi
PARTITIONED BY (update_dt)
OPTIONS (
    type = 'cow',
    primaryKey = 'item_id',
    preCombineField = 'last_exposure_dt',
    hoodie.table.name = 'dim_item',
    hoodie.datasource.write.recordkey.field = 'item_id',
    hoodie.datasource.write.partitionpath.field = 'update_dt',
    hoodie.datasource.write.precombine.field = 'last_exposure_dt',
    hoodie.datasource.write.operation = 'upsert',
    hoodie.datasource.write.table.type = 'COPY_ON_WRITE',
    hoodie.cleaner.policy = 'KEEP_LATEST_COMMITS',
    hoodie.cleaner.commits.retained = 10
);

-- ------------------------------------------------------------
-- 3. 时间维度表 dim_date
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key       STRING   COMMENT '日期代理键（YYYYMMDD）',
    dt             STRING   COMMENT '标准日期（YYYY-MM-DD，主键）',
    year           INT      COMMENT '年份',
    month          INT      COMMENT '月份',
    day            INT      COMMENT '日',
    day_of_week    INT      COMMENT '星期几（1=周一, 7=周日）',
    day_of_year    INT      COMMENT '一年中第几天',
    week_of_year   INT      COMMENT '一年中第几周',
    quarter        INT      COMMENT '季度（1-4）',
    is_weekend     INT      COMMENT '是否周末（0=工作日, 1=周末）',
    month_name     STRING   COMMENT '月份名称',
    weekday_name   STRING   COMMENT '星期名称'
)
USING hudi
OPTIONS (
    type = 'cow',
    primaryKey = 'dt',
    preCombineField = 'dt',
    hoodie.table.name = 'dim_date',
    hoodie.datasource.write.recordkey.field = 'dt',
    hoodie.datasource.write.operation = 'upsert',
    hoodie.datasource.write.table.type = 'COPY_ON_WRITE'
);

-- ============================================================
-- 第二层：DWD 明细层（Hudi MOR 表）
-- ============================================================

-- ------------------------------------------------------------
-- 4. 用户行为事实表 dwd_user_behavior_fact
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwd_user_behavior_fact (
    behavior_id      STRING   COMMENT '行为记录唯一ID（主键）',
    user_id          BIGINT   COMMENT '用户ID（外键→dim_user）',
    item_id          BIGINT   COMMENT '商品ID（外键→dim_item）',
    category_id      BIGINT   COMMENT '类目ID（退化维度）',
    date_key         STRING   COMMENT '日期代理键（外键→dim_date）',
    behavior_type    STRING   COMMENT '行为类型（pv/fav/cart/buy）',
    behavior_code    INT      COMMENT '行为编码（1=pv,2=fav,3=cart,4=buy）',
    event_time       TIMESTAMP COMMENT '行为发生时间',
    ts               BIGINT   COMMENT 'Unix时间戳',
    dt               STRING   COMMENT '日期分区',
    session_id       STRING   COMMENT '会话ID（Session切分）',
    is_session_start INT      COMMENT '是否会话起始（0=否,1=是）',
    hour_of_day      INT      COMMENT '行为发生小时（0-23）'
)
USING hudi
PARTITIONED BY (dt)
OPTIONS (
    type = 'mor',
    primaryKey = 'behavior_id',
    preCombineField = 'ts',
    hoodie.table.name = 'dwd_user_behavior_fact',
    hoodie.datasource.write.recordkey.field = 'behavior_id',
    hoodie.datasource.write.partitionpath.field = 'dt',
    hoodie.datasource.write.precombine.field = 'ts',
    hoodie.datasource.write.operation = 'upsert',
    hoodie.datasource.write.table.type = 'MERGE_ON_READ',
    hoodie.cleaner.policy = 'KEEP_LATEST_COMMITS',
    hoodie.cleaner.commits.retained = 10,
    hoodie.compact.inline = 'false'
);
