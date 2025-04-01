


-- 1. 商品表（tb_product）
CREATE TABLE IF NOT EXISTS ods_tb_product (
                                              product_id INT,
                                              product_name STRING,
                                              price DECIMAL(10, 2),
    category STRING,
    brand STRING
    )
    PARTITIONED BY ( `dt` string)
    row format delimited
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 2. 销售表（tb_sales）
CREATE TABLE IF NOT EXISTS ods_tb_sales (
                                            sales_id INT,
                                            product_id INT,
                                            sales_date DATE,
                                            sales_quantity INT,
                                            sales_amount DECIMAL(10, 2),
    buyer_id INT
    )
    PARTITIONED BY ( `dt` string)
    row format delimited
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 3. 流量表（tb_traffic）
CREATE TABLE IF NOT EXISTS ods_tb_traffic (
                                              traffic_id INT,
                                              product_id INT,
                                              traffic_channel STRING,
                                              access_time TIMESTAMP,
                                              visitor_id INT,
                                              is_converted BOOLEAN
)
    PARTITIONED BY ( `dt` string)
    row format delimited
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 4. 评价表（tb_evaluation）
CREATE TABLE IF NOT EXISTS ods_tb_evaluation (
                                                 evaluation_id INT,
                                                 product_id INT,
                                                 buyer_id INT,
                                                 evaluation_date DATE,
                                                 score INT,
                                                 evaluation_content STRING,
                                                 is_active_evaluation BOOLEAN
)
    PARTITIONED BY ( `dt` string)
    row format delimited
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 5. 搜索词表（tb_search_term）
CREATE TABLE IF NOT EXISTS ods_tb_search_term (
                                                  search_term_id INT,
                                                  product_id INT,
                                                  search_term STRING,
                                                  search_count INT,
                                                  traffic_count INT,
                                                  conversion_rate DECIMAL(5, 2)
    )
    PARTITIONED BY ( `dt` string)
    row format delimited
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 6. 内容表（tb_content）
CREATE TABLE IF NOT EXISTS ods_tb_content (
                                              content_id INT,
                                              product_id INT,
                                              content_type STRING,
                                              content_title STRING,
                                              play_count INT,
                                              interaction_count INT
)
    PARTITIONED BY ( `dt` string)
    row format delimited
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;



load data inpath '/2207A/zhengwei_zhou/tb/tb_product/2025-03-30'
overwrite into table ods_tb_product partition (dt='2025-03-30');


load data inpath '/2207A/zhengwei_zhou/tb/tb_sales/2025-03-30'
overwrite into table ods_tb_sales partition (dt='2025-03-30');

load data inpath '/2207A/zhengwei_zhou/tb/tb_traffic/2025-03-30'
overwrite into table ods_tb_traffic partition (dt='2025-03-30');

load data inpath '/2207A/zhengwei_zhou/tb/tb_evaluation/2025-03-30'
overwrite into table ods_tb_evaluation partition (dt='2025-03-30');

load data inpath '/2207A/zhengwei_zhou/tb/tb_search_term/2025-03-30'
overwrite into table ods_tb_search_term partition (dt='2025-03-30');

load data inpath '/2207A/zhengwei_zhou/tb/tb_content/2025-03-30'
overwrite into table ods_tb_content partition (dt='2025-03-30');



-- 1
create external table dwd_tb_product
(
    product_id INT,
    product_name STRING,
    price DECIMAL(10, 2),
    category STRING,
    brand STRING
)PARTITIONED BY ( `dt` string)
stored as  parquet
location '/2207A/zhengwei_zhou/tb/dwd/dwd_tb_product/'
tblproperties ("parquet.compression"="snappy")
;

insert into dwd_tb_product
select * from ods_tb_product
where dt='2025-03-30'   and product_id is not null;



-- 2. 销售表（dwd_tb_sales）
CREATE external table dwd_tb_sales (
    sales_id INT,
    product_id INT,
    sales_date DATE,
    sales_quantity INT,
    sales_amount DECIMAL(10, 2),
    buyer_id INT
)
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/2207A/zhengwei_zhou/tb/dwd/dwd_tb_sales/'
tblproperties ("parquet.compression"="snappy")
;

insert into dwd_tb_sales
select * from ods_tb_sales
where dt='2025-03-30'   and sales_id is not null;


-- 3. 流量表（dwd_tb_traffic）
CREATE external table dwd_tb_traffic (
    traffic_id INT,
    product_id INT,
    traffic_channel STRING,
    access_time TIMESTAMP,
    visitor_id INT,
    is_converted BOOLEAN
)
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/2207A/zhengwei_zhou/tb/dwd/dwd_tb_traffic/'
tblproperties ("parquet.compression"="snappy")
;

insert into dwd_tb_traffic
select * from ods_tb_traffic
where dt='2025-03-30'   and traffic_id is not null;


-- 4. 评价表（dwd_tb_evaluation）
CREATE external table dwd_tb_evaluation (
    evaluation_id INT,
    product_id INT,
    buyer_id INT,
    evaluation_date DATE,
    score INT,
    evaluation_content STRING,
    is_active_evaluation BOOLEAN
)
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/2207A/zhengwei_zhou/tb/dwd/dwd_tb_evaluation/'
tblproperties ("parquet.compression"="snappy")
;

insert into dwd_tb_evaluation
select * from ods_tb_evaluation
where dt='2025-03-30'   and evaluation_id is not null;





-- 5. 搜索词表（dwd_tb_search_term）
CREATE external table dwd_tb_search_term (
    search_term_id INT,
    product_id INT,
    search_term STRING,
    search_count INT,
    traffic_count INT,
    conversion_rate DECIMAL(5, 2)
)
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/2207A/zhengwei_zhou/tb/dwd/dwd_tb_search_term/'
tblproperties ("parquet.compression"="snappy")
;

insert into dwd_tb_search_term
select * from ods_tb_search_term
where dt='2025-03-30'   and search_term_id is not null;




-- 6. 内容表（dwd_tb_content）
CREATE TABLE IF NOT EXISTS dwd_tb_content (
                                              content_id INT,
                                              product_id INT,
                                              content_type STRING,
                                              content_title STRING,
                                              play_count INT,
                                              interaction_count INT
)
    PARTITIONED BY ( `dt` string)
    stored as  parquet
    location '/2207A/zhengwei_zhou/tb/dwd/dwd_tb_content/'
    tblproperties ("parquet.compression"="snappy")
;

insert into dwd_tb_content
select * from ods_tb_content
where dt='2025-03-30'   and content_id is not null;


--
-- -- 创建 ADS 层销售分析核心概况表
-- CREATE TABLE IF NOT EXISTS ads_sales_core_summary (
--     product_id INT,
--     product_name STRING,
--     dt STRING,
--     sales_quantity BIGINT,
--     sales_amount DECIMAL(10, 2),
--     traffic_count BIGINT,
--     conversion_rate DECIMAL(5, 2),
--     new_customer_discount BOOLEAN,
--     installment_free_interest BOOLEAN,
--     video_published BOOLEAN,
--     main_image_modified BOOLEAN
-- )
-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY '\t'
-- STORED AS TEXTFILE;
--
-- -- 插入数据到 ADS 层表
-- INSERT OVERWRITE TABLE ads_sales_core_summary
-- SELECT
--     p.product_id,
--     p.product_name,
--     s.dt,
--     SUM(s.sales_quantity) AS sales_quantity,
--     SUM(s.sales_amount) AS sales_amount,
--     SUM(st.traffic_count) AS traffic_count,
--     AVG(st.conversion_rate) AS conversion_rate,
--     -- 假设这些运营行动标记数据可以通过某种方式关联或标记
--     -- 这里只是示例，实际需要根据业务逻辑替换条件
--     CASE WHEN EXISTS (
--         -- 这里应根据实际运营系统记录关联判断是否有新客折扣
--         SELECT 1 FROM some_operation_table
--         WHERE product_id = p.product_id AND dt = s.dt AND operation_type = 'new_customer_discount'
--     ) THEN TRUE ELSE FALSE END AS new_customer_discount,
--     CASE WHEN EXISTS (
--         -- 这里应根据实际运营系统记录关联判断是否有分期免息
--         SELECT 1 FROM some_operation_table
--         WHERE product_id = p.product_id AND dt = s.dt AND operation_type = 'installment_free_interest'
--     ) THEN TRUE ELSE FALSE END AS installment_free_interest,
--     CASE WHEN EXISTS (
--         -- 这里应根据实际运营系统记录关联判断是否发布了短视频
--         SELECT 1 FROM some_operation_table
--         WHERE product_id = p.product_id AND dt = s.dt AND operation_type = 'video_published'
--     ) THEN TRUE ELSE FALSE END AS video_published,
--     CASE WHEN EXISTS (
--         -- 这里应根据实际运营系统记录关联判断是否修改了主图
--         SELECT 1 FROM some_operation_table
--         WHERE product_id = p.product_id AND dt = s.dt AND operation_type = 'main_image_modified'
--     ) THEN TRUE ELSE FALSE END AS main_image_modified
-- FROM
--     ods_tb_product p
-- JOIN
--     ods_tb_sales s ON p.product_id = s.product_id
-- JOIN
--     ods_tb_search_term st ON p.product_id = st.product_id AND s.dt = st.dt
-- GROUP BY
--     p.product_id, p.product_name, s.dt;
--
-- -- 查询指定商品的核心数据趋势
-- SELECT
--     dt,
--     sales_quantity,
--     sales_amount,
--     traffic_count,
--     conversion_rate
-- FROM
--     ads_sales_core_summary
-- WHERE
--     product_id = 1  -- 替换为实际的商品 ID
-- ORDER BY
--     dt;
--
-- -- 查询指定商品在报名新客折扣时的核心数据
-- SELECT
--     dt,
--     sales_quantity,
--     sales_amount,
--     traffic_count,
--     conversion_rate
-- FROM
--     ads_sales_core_summary
-- WHERE
--     product_id = 1  -- 替换为实际的商品 ID
--     AND new_customer_discount = TRUE
-- ORDER BY
--     dt;









-- 销售分析
-- 1.核心概况：
CREATE TABLE IF NOT EXISTS ads_sales_analysis (
                                                  product_id INT COMMENT '商品ID',
                                                  dt STRING COMMENT '日期分区（YYYY-MM-DD）',
    -- 核心指标
                                                  total_sales_amount DECIMAL(18,2) COMMENT '累计销售额',
    total_sales_quantity INT COMMENT '累计销售量',
    daily_uv BIGINT COMMENT '当日独立访客数',
    conversion_rate DECIMAL(5,2) COMMENT '转化率（销售量/访问量）',
    avg_evaluation_score DECIMAL(3,1) COMMENT '近7日评价平均分',
    -- 运营行动标记
    has_newbie_discount BOOLEAN COMMENT '是否参与新客折扣',
    has_installment BOOLEAN COMMENT '是否开通分期免息',
    has_short_video BOOLEAN COMMENT '当日是否发布短视频',
    has_taobao_main_image_updated BOOLEAN COMMENT '当日是否修改手淘主图'
    )
    PARTITIONED BY (dt_range STRING COMMENT '时间范围（daily/weekly/monthly）')
    STORED AS PARQUET;


set hive.exec.dynamic.partition.mode=nonstrict;

-- 示例：按天分区（dt_range='daily'）的ADS表数据加工
INSERT OVERWRITE TABLE ads_sales_analysis PARTITION(dt_range='daily')
SELECT
    p.product_id,
    s.dt,
    -- 核心指标
    SUM(s.sales_amount) AS total_sales_amount,
    SUM(s.sales_quantity) AS total_sales_quantity,
    COUNT(DISTINCT t.visitor_id) AS daily_uv,
    ROUND(SUM(s.sales_quantity) / COUNT(DISTINCT t.visitor_id), 2) AS conversion_rate,
    ROUND(AVG(e.score), 1) AS avg_evaluation_score,
    -- 运营行动标记
    MAX(CASE WHEN p.category = '首单礼金' THEN TRUE ELSE FALSE END) AS has_newbie_discount,
    MAX(CASE WHEN p.brand LIKE '%分期免息%' THEN TRUE ELSE FALSE END) AS has_installment,
    MAX(CASE WHEN c.content_type = '短视频' AND c.dt = s.dt THEN TRUE ELSE FALSE END) AS has_short_video,
    MAX(CASE WHEN t.traffic_channel = '手淘推荐' AND t.access_time >= s.sales_date THEN TRUE ELSE FALSE END) AS has_taobao_main_image_updated
FROM
    ods_tb_product p
        LEFT JOIN
    ods_tb_sales s ON p.product_id = s.product_id
        LEFT JOIN
    ods_tb_traffic t ON p.product_id = t.product_id AND t.dt = s.dt
        LEFT JOIN
    ods_tb_evaluation e ON p.product_id = e.product_id AND e.evaluation_date BETWEEN DATE_SUB(s.dt, 7) AND s.dt
        LEFT JOIN
    ods_tb_content c ON p.product_id = c.product_id AND c.dt = s.dt
WHERE
    s.dt BETWEEN '2025-03-30' AND '2025-03-30'  -- 动态替换日期范围
GROUP BY
    p.product_id, s.dt;










--  2.SKU销售详情/属性分析：
-- 创建 ADS 层表用于存储 SKU 销售详情和属性分析结果
CREATE TABLE IF NOT EXISTS ads_sku_sales_detail (
                                                    product_id INT,
                                                    product_name STRING,
                                                    sku_attribute STRING,  -- 假设这里存储 SKU 的属性，如颜色、尺寸等
                                                    sales_quantity BIGINT,
                                                    sales_amount DECIMAL(10, 2),
    dt STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 插入数据到 ADS 层表
INSERT OVERWRITE TABLE ads_sku_sales_detail
SELECT
    p.product_id,
    p.product_name,
    -- 这里需要根据实际情况确定如何从表中获取 SKU 属性
    -- 假设可以从某个表的字段中提取，这里只是示例，实际可能需要调整
    'Some_Attribute' AS sku_attribute,
    SUM(s.sales_quantity) AS sales_quantity,
    SUM(s.sales_amount) AS sales_amount,
    s.dt
FROM
    ods_tb_product p
        JOIN
    ods_tb_sales s ON p.product_id = s.product_id
GROUP BY
    p.product_id, p.product_name, s.dt;

-- 查询不同 SKU 属性的热销程度（按销售数量降序排序）
SELECT
    sku_attribute,
    SUM(sales_quantity) AS total_sales_quantity,
    SUM(sales_amount) AS total_sales_amount
FROM
    ads_sku_sales_detail
GROUP BY
    sku_attribute
ORDER BY
    total_sales_quantity DESC;

-- 查询特定日期内某个 SKU 属性的销售详情
SELECT
    product_id,
    product_name,
    sku_attribute,
    sales_quantity,
    sales_amount
FROM
    ads_sku_sales_detail
WHERE
        dt = '2025-03-30'  -- 可根据实际需求修改日期
  AND sku_attribute = 'Some_Attribute'  -- 可根据实际需求修改 SKU 属性
ORDER BY
    sales_quantity DESC;









-- 3.价格分析：
--sql -- 1. 创建价格分析基础表
CREATE TABLE IF NOT EXISTS ads_price_analysis
( product_id INT,
  product_name STRING,
  category STRING,
  dt STRING,
  price DECIMAL(10, 2),
    sales_quantity BIGINT,
    sales_amount DECIMAL(10, 2),
    price_rank DECIMAL(5, 2),

-- 类目内价格排名百分比
    price_band STRING -- 价格带划分
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS ORC; -- 使用 ORC 格式提升性能
-- 2. 每日数据更新（示例使用 2025-03-30 数据）

INSERT OVERWRITE TABLE ads_price_analysis
SELECT
    p.product_id,
    p.product_name,
    p.category,
    s.dt,
    p.price,
    SUM (s.sales_quantity) AS sales_quantity,
    SUM (s.sales_amount) AS sales_amount,
-- 计算类目内价格排名百分比
    PERCENT_RANK () OVER (
PARTITION BY p.category
ORDER BY p.price
) * 100 AS price_rank,
-- 动态划分价格带（示例分为 4 个区间）
        CONCAT (
                NTILE (4) OVER (
PARTITION BY p.category
ORDER BY p.price
),
                ' 档 '
            ) AS price_band
FROM
    ods_tb_product p
        JOIN
    ods_tb_sales s
    ON p.product_id = s.product_id
        AND p.dt = s.dt -- 确保商品表与销售表日期一致
GROUP BY
    p.product_id, p.product_name, p.category, s.dt, p.price;

-- 3. 价格趋势分析（查看指定商品价格波动）
SELECT
    dt,
    price,
    sales_quantity,
    sales_amount
FROM
    ads_price_analysis
WHERE
        product_id = 123 -- 替换为目标商品 ID
ORDER BY
    dt;

-- 4. 价格带商品表现分析
SELECT
    price_band,
    COUNT (DISTINCT product_id) AS product_count,
    SUM (sales_quantity) AS total_sales,
    SUM (sales_amount) AS total_revenue
FROM
    ads_price_analysis
WHERE
        dt = '2025-03-30'
GROUP BY
    price_band
ORDER BY
    total_revenue DESC;

-- 5. 类目价格对标分析
SELECT
    category,
    product_id,
    price,
    price_rank,
    price_band
FROM
    ads_price_analysis
WHERE
        dt = '2025-03-30'
  AND category = ' 手机 ' -- 替换为目标类目
ORDER BY
    category, price_rank;





-- 流量来源
-- 创建ADS层流量分析表
CREATE TABLE IF NOT EXISTS ads_tb_product_traffic_analysis (
                                                               product_id INT COMMENT '商品ID',
                                                               traffic_channel STRING COMMENT '流量渠道',
                                                               visitor_count INT COMMENT '访客数量',
                                                               visitor_ratio DECIMAL(10, 4) COMMENT '访客占比',
    converted_count INT COMMENT '转化数量',
    conversion_rate DECIMAL(10, 4) COMMENT '转化率'
    )
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS PARQUET;

-- 插入数据到ADS层表
INSERT OVERWRITE TABLE ads_tb_product_traffic_analysis PARTITION (dt='2025-03-30')
SELECT
    t.product_id,
    t.traffic_channel,
    COUNT(DISTINCT t.visitor_id) AS visitor_count,
    -- 计算访客占比
    COUNT(DISTINCT t.visitor_id) / SUM(COUNT(DISTINCT t.visitor_id)) OVER (PARTITION BY t.product_id) AS visitor_ratio,
    -- 计算转化数量
        SUM(CASE WHEN t.is_converted THEN 1 ELSE 0 END) AS converted_count,
    -- 计算转化率
    SUM(CASE WHEN t.is_converted THEN 1 ELSE 0 END) / COUNT(DISTINCT t.visitor_id) AS conversion_rate
FROM
    ods_tb_traffic t
WHERE
        t.dt = '2025-03-30'
GROUP BY
    t.product_id, t.traffic_channel;









-- 价格分析

-- 创建 ADS 层表用于价格力商品分析
CREATE TABLE IF NOT EXISTS ads_tb_price_force_product (
                                                          product_id INT COMMENT '商品 ID',
                                                          product_name STRING COMMENT '商品名称',
                                                          category STRING COMMENT '商品类目',
                                                          price DECIMAL(10, 2) COMMENT '商品价格',
    price_band STRING COMMENT '价格带',
    price_force_star INT COMMENT '价格力星级',
    product_force_core_index DECIMAL(10, 4) COMMENT '商品力核心指标',
    similar_category_excellent_index DECIMAL(10, 4) COMMENT '市场同类目同价格带同星级优秀商品力指标数据',
    category_price_force_rank INT COMMENT '同类目价格力商品榜单排名'
    )
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS PARQUET;




WITH price_band_definition AS (
    SELECT
        product_id,
        product_name,
        category,
        price,
        CASE
            WHEN price <= 100 THEN '0 - 100'
            WHEN price > 100 AND price <= 500 THEN '101 - 500'
            WHEN price > 500 AND price <= 1000 THEN '501 - 1000'
            ELSE '> 1000'
            END AS price_band
    FROM ods_tb_product
    WHERE dt = '2025-03-25'
),
-- 计算商品力核心指标
     product_force_core_calculation AS (
         SELECT
             p.product_id,
             p.product_name,
             p.category,
             p.price,
             pb.price_band,
             -- 这里简单以销量和评价得分综合计算商品力核心指标，可按需调整
             SUM(s.sales_amount) / COUNT(DISTINCT e.evaluation_id) AS product_force_core_index
         FROM ods_tb_product p
                  JOIN price_band_definition pb ON p.product_id = pb.product_id
                  LEFT JOIN ods_tb_sales s ON p.product_id = s.product_id AND s.dt = '2025-03-25'
                  LEFT JOIN ods_tb_evaluation e ON p.product_id = e.product_id AND e.dt = '2025-03-25'
         GROUP BY p.product_id, p.product_name, p.category, p.price, pb.price_band
     ),
-- 计算每个价格带的平均商品力核心指标
     price_band_avg_core_index AS (
         SELECT
             price_band,
             AVG(product_force_core_index) AS avg_core_index
         FROM product_force_core_calculation
         GROUP BY price_band
     ),
-- 计算价格力星级
     price_force_star_calculation AS (
         SELECT
             pfc.product_id,
             pfc.product_name,
             pfc.category,
             pfc.price,
             pfc.price_band,
             pfc.product_force_core_index,
             CASE
                 WHEN pfc.product_force_core_index > pbai.avg_core_index THEN 5
                 WHEN pfc.product_force_core_index > pbai.avg_core_index * 0.8 THEN 4
                 WHEN pfc.product_force_core_index > pbai.avg_core_index * 0.6 THEN 3
                 WHEN pfc.product_force_core_index > pbai.avg_core_index * 0.4 THEN 2
                 ELSE 1
                 END AS price_force_star
         FROM product_force_core_calculation pfc
                  JOIN price_band_avg_core_index pbai ON pfc.price_band = pbai.price_band
     ),
-- 计算每个类目、价格带和星级组合的平均商品力核心指标
     category_band_star_avg_core_index AS (
         SELECT
             category,
             price_band,
             price_force_star,
             AVG(product_force_core_index) AS avg_core_index
         FROM price_force_star_calculation
         GROUP BY category, price_band, price_force_star
     ),
-- 计算市场同类目同价格带同星级优秀商品力指标数据
     similar_category_excellent_calculation AS (
         SELECT
             pfs.product_id,
             pfs.product_name,
             pfs.category,
             pfs.price,
             pfs.price_band,
             pfs.price_force_star,
             pfs.product_force_core_index,
             cb_sai.avg_core_index AS similar_category_excellent_index
         FROM price_force_star_calculation pfs
                  LEFT JOIN category_band_star_avg_core_index cb_sai
                            ON pfs.category = cb_sai.category AND pfs.price_band = cb_sai.price_band AND pfs.price_force_star = cb_sai.price_force_star
     ),
-- 生成同类目价格力商品榜单排名
     category_price_force_ranking AS (
         SELECT
             sce.product_id,
             sce.product_name,
             sce.category,
             sce.price,
             sce.price_band,
             sce.price_force_star,
             sce.product_force_core_index,
             sce.similar_category_excellent_index,
             ROW_NUMBER() OVER (PARTITION BY sce.category ORDER BY sce.product_force_core_index DESC) AS category_price_force_rank
         FROM similar_category_excellent_calculation sce
     )
-- 插入数据到 ADS 层表
INSERT OVERWRITE TABLE ads_tb_price_force_product PARTITION (dt = '2025-03-25')
SELECT
    cpf.product_id,
    cpf.product_name,
    cpf.category,
    cpf.price,
    cpf.price_band,
    cpf.price_force_star,
    cpf.product_force_core_index,
    cpf.similar_category_excellent_index,
    cpf.category_price_force_rank
FROM category_price_force_ranking cpf;














































































































-- # 指标开发
-- # 商品销售核心指标
-- # 近 7 天销售额

SELECT

    product_id,
    SUM(sales_amount) AS sales_amount_7d
FROM
    ods_tb_sales
WHERE
        sales_date >= DATE_SUB('2025-03-05', INTERVAL 7 DAY) -- - INTERVAL 7 DAY
GROUP BY
    product_id;





-- # 近 30 天销售数量

SELECT
    product_id,
    SUM(sales_quantity) AS sales_quantity_30d
FROM
    ods_tb_sales
WHERE
        sales_date >= CURRENT_DATE() - INTERVAL 30 DAY
GROUP BY
    product_id;




-- # 流量指标
-- # 各流量渠道访客占比

WITH total_visitors AS (
    SELECT
        product_id,
        COUNT(DISTINCT visitor_id) AS total_count
    FROM
        ods_tb_traffic
    GROUP BY
        product_id
)
SELECT
    t.product_id,
    t.traffic_channel,
    COUNT(DISTINCT t.visitor_id) / tv.total_count AS visitor_ratio
FROM
    ods_tb_traffic t
        JOIN
    total_visitors tv ON t.product_id = tv.product_id
GROUP BY
    t.product_id, t.traffic_channel;





-- # 各流量渠道转化率

SELECT
    product_id,
    traffic_channel,
    SUM(CASE WHEN is_converted THEN 1 ELSE 0 END) / COUNT(*) AS conversion_rate
FROM
    ods_tb_traffic
GROUP BY
    product_id, traffic_channel;




-- # 评价指标
-- # 商品整体评分

SELECT
    product_id,
    AVG(score) AS average_score
FROM
    ods_tb_evaluation
GROUP BY
    product_id;




-- # 近 30 天负面评价数

SELECT
    product_id,
    COUNT(*) AS negative_evaluation_count_30d
FROM
    ods_tb_evaluation
WHERE
        evaluation_date >= CURRENT_DATE() - INTERVAL 30 DAY AND score <= 3
GROUP BY
    product_id;




-- # 搜索词指标
-- # 搜索词引流转化率前 5

SELECT
    product_id,
    search_term,
    conversion_rate
FROM
    ods_tb_search_term
ORDER BY
    conversion_rate DESC
    LIMIT 5;





-- # 内容指标
-- # 内容互动量 TOP3

SELECT
    product_id,
    content_type,
    content_title,
    interaction_count
FROM
    ods_tb_content
ORDER BY
    interaction_count DESC
    LIMIT 3;
















