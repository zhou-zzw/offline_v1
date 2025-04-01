
-- 商品主题
-- 品类 360 看板
-- 任务工单   04






-- 创建品类销售分析表（Hive兼容版）
CREATE TABLE IF NOT EXISTS ods_category_sales_analysis (
                                                           category_id STRING COMMENT '品类ID',
                                                           category_name STRING COMMENT '品类名称',
                                                           analysis_date DATE COMMENT '分析日期',
                                                           total_sales DECIMAL(10, 2) COMMENT '总销售额',
    total_quantity INT COMMENT '总销量',
    monthly_payment_progress DECIMAL(10, 2) COMMENT '月支付进度',
    monthly_payment_contribution DECIMAL(5, 2) COMMENT '月支付贡献度',
    last_month_payment_rank INT COMMENT '上月支付排名',
    time_dimension STRING COMMENT '时间维度（daily/weekly/monthly）'
    )
    COMMENT '品类销售分析表'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 创建品类属性分析表（Hive兼容版）
CREATE TABLE IF NOT EXISTS ods_category_attribute_analysis (
                                                               category_id STRING COMMENT '品类ID',
                                                               attribute_name STRING COMMENT '属性名称',
                                                               analysis_date DATE COMMENT '分析日期',
                                                               traffic INT COMMENT '流量',
                                                               payment_conversion_rate DECIMAL(5, 2) COMMENT '支付转化率',
    active_product_count INT COMMENT '活跃商品数',
    time_dimension STRING COMMENT '时间维度（daily/weekly/monthly）'
    )
    COMMENT '品类属性分析表'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 创建品类流量分析表（Hive兼容版）
CREATE TABLE IF NOT EXISTS ods_category_traffic_analysis (
                                                             category_id STRING COMMENT '品类ID',
                                                             traffic_source STRING COMMENT '流量来源',
                                                             analysis_date DATE COMMENT '分析日期',
                                                             visitor_count INT COMMENT '访客数',
                                                             conversion_count INT COMMENT '转化数',
                                                             search_keyword STRING COMMENT '搜索关键词',
                                                             keyword_rank INT COMMENT '关键词排名',
                                                             keyword_visitor_count INT COMMENT '关键词访客数'
)
    COMMENT '品类流量分析表'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 创建品类客群洞察表（Hive兼容版）
CREATE TABLE IF NOT EXISTS ods_category_customer_insight (
                                                             category_id STRING COMMENT '品类ID',
                                                             user_type STRING COMMENT '用户类型',
                                                             age_group STRING COMMENT '年龄分组',
                                                             gender STRING COMMENT '性别',
                                                             purchase_frequency STRING COMMENT '购买频次'
)
    COMMENT '品类客群洞察表'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;




------------------------------------

-- 插入品类销售分析表数据
INSERT INTO TABLE ods_category_sales_analysis
SELECT
    concat('cat', lpad(cast(pos as string), 2, '0')) as category_id,
    case pos % 5
    when 0 then '电子产品'
        when 1 then '家居用品'
        when 2 then '服装鞋帽'
        when 3 then '美妆个护'
        else '食品饮料'
end as category_name,
    date_add('2023-01-01', cast(rand() * 365 as int)) as analysis_date,
    round(rand() * 100000 + 5000, 2) as total_sales,
    cast(rand() * 1000 + 50 as int) as total_quantity,
    round(rand() * 100, 2) as monthly_payment_progress,
    round(rand() * 100, 2) as monthly_payment_contribution,
    cast(rand() * 10 + 1 as int) as last_month_payment_rank,
    case
        when rand() < 0.33 then 'daily'
        when rand() < 0.66 then 'weekly'
        else 'monthly'
end as time_dimension
FROM (
    SELECT posexplode(split(space(499), ' ')) as (pos, val)
) t;


-- 插入品类属性分析表数据
INSERT INTO TABLE ods_category_attribute_analysis
SELECT
    concat('cat', lpad(cast(pos as string), 2, '0')) as category_id,
    case pos % 6
    when 0 then '颜色-红色'
        when 1 then '尺寸-中号'
        when 2 then '材质-纯棉'
        when 3 then '功能-防水'
        when 4 then '品牌-知名'
        else '风格-简约'
end as attribute_name,
    date_add('2023-01-01', cast(rand() * 365 as int)) as analysis_date,
    cast(rand() * 1900 + 100 as int) as traffic,
    round(rand() * 0.5 + 0.1, 2) as payment_conversion_rate,
    cast(rand() * 450 + 50 as int) as active_product_count,
    case
        when rand() < 0.33 then 'daily'
        when rand() < 0.66 then 'weekly'
        else 'monthly'
end as time_dimension
FROM (
    SELECT posexplode(split(space(499), ' ')) as (pos, val)
) t;


-- 插入品类流量分析表数据
INSERT INTO TABLE ods_category_traffic_analysis
SELECT
    concat('cat', lpad(cast(pos as string), 2, '0')) as category_id,
    case pos % 5
    when 0 then '直通车'
        when 1 then '淘宝搜索'
        when 2 then '抖音推广'
        when 3 then '自然流量'
        else '微信分享'
end as traffic_source,
    date_add('2023-01-01', cast(rand() * 365 as int)) as analysis_date,
    cast(rand() * 900 + 100 as int) as visitor_count,
    cast(rand() * 150 as int) as conversion_count,
    case pos % 4
        when 0 then '连衣裙 夏季新款'
        when 1 then '运动鞋 男款'
        when 2 then '手机 5G 版'
        else '化妆品 套装'
end as search_keyword,
    cast(rand() * 10 + 1 as int) as keyword_rank,
    cast(rand() * 450 + 50 as int) as keyword_visitor_count
FROM (
    SELECT posexplode(split(space(499), ' ')) as (pos, val)
) t;


-- 插入品类客群洞察表数据
INSERT INTO TABLE ods_category_customer_insight
SELECT
    concat('cat', lpad(cast(pos as string), 2, '0')) as category_id,
    case pos % 3
    when 0 then '新用户'
        when 1 then '老用户'
        else '流失用户'
end as user_type,
    case pos % 4
        when 0 then '18-24岁'
        when 1 then '25-35岁'
        when 2 then '36-45岁'
        else '45岁以上'
end as age_group,
    case pos % 2
        when 0 then '男'
        else '女'
end as gender,
    case pos % 3
        when 0 then '高频'
        when 1 then '中频'
        else '低频'
end as purchase_frequency
FROM (
    SELECT posexplode(split(space(499), ' ')) as (pos, val)
) t;









-- 创建 ADS 层销售分析表
CREATE TABLE IF NOT EXISTS ads_sales_overview (
                                                  category_id STRING COMMENT '品类ID',
                                                  analysis_date DATE COMMENT '分析日期',
                                                  time_dimension STRING COMMENT '时间维度（daily/weekly/monthly）',
                                                  total_sales DECIMAL(10, 2) COMMENT '总销售额',
    total_quantity INT COMMENT '总销量',
    sales_rank INT COMMENT '销售额排名',
    quantity_rank INT COMMENT '销量排名'
    )
    COMMENT '销售分析核心概况表'
    STORED AS PARQUET;

-- 数据填充 SQL
INSERT INTO TABLE ads_sales_overview
    WITH aggregated_data AS (
    -- 按时间维度聚合数据
    SELECT
        category_id,
        analysis_date,
        time_dimension,
        SUM(total_sales) AS total_sales,
        SUM(total_quantity) AS total_quantity
    FROM ods_category_sales_analysis
    GROUP BY category_id, analysis_date, time_dimension
),
ranked_data AS (
    -- 计算排名
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY time_dimension
            ORDER BY total_sales DESC
        ) AS sales_rank,
        ROW_NUMBER() OVER (
            PARTITION BY time_dimension
            ORDER BY total_quantity DESC
        ) AS quantity_rank
    FROM aggregated_data
)
SELECT
    category_id,
    analysis_date,
    time_dimension,
    total_sales,
    total_quantity,
    sales_rank,
    quantity_rank
FROM ranked_data;

















