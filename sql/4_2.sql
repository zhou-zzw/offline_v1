

-- 商品主题
-- 品类 360 看板
-- 任务工单   04

--  销售分析
-- ads_sales_overview  ->  ods_category_sales_analysis(创建品类销售分析表)
-- ads_attribute_analysis -> ods_category_attribute_analysis(创建品类属性分析表)


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






INSERT INTO TABLE ads_sales_overview
SELECT
    t2.category_id,
    t2.analysis_date,
    t2.time_dimension,
    t2.total_sales,
    t2.total_quantity,
    t2.sales_rank,
    t2.quantity_rank
FROM (
         SELECT
             t1.category_id,
             t1.analysis_date,
             t1.time_dimension,
             t1.total_sales,
             t1.total_quantity,
             ROW_NUMBER() OVER (
            PARTITION BY t1.time_dimension
            ORDER BY t1.total_sales DESC
        ) AS sales_rank,
                 ROW_NUMBER() OVER (
            PARTITION BY t1.time_dimension
            ORDER BY t1.total_quantity DESC
        ) AS quantity_rank
         FROM (
                  SELECT
                      category_id,
                      analysis_date,
                      time_dimension,
                      SUM(total_sales) AS total_sales,
                      SUM(total_quantity) AS total_quantity
                  FROM ods_category_sales_analysis
                  GROUP BY category_id, analysis_date, time_dimension
              ) t1
     ) t2;








--- 属性分析
-- 属性分析功能帮助商家深入了解品类下的各种属性对销售的影响，通过属性
-- 的流量、支付转化及动销商品数，商家可查看该属性是否有效帮助商家引流
-- 以及帮助商家拓展机会，从属性角度提升同品类其他商品引流和转化情况。
-- ● 操作路径：生意参谋——商品——品类 360——详情——属性分析
-- ● 时间维度：日/周/月



-- 创建 ADS 层属性分析表
CREATE TABLE IF NOT EXISTS ads_attribute_analysis (
                                                      category_id STRING COMMENT '品类ID',
                                                      attribute_name STRING COMMENT '属性名称',
                                                      analysis_date DATE COMMENT '分析日期',
                                                      time_dimension STRING COMMENT '时间维度（daily/weekly/monthly）',
                                                      total_traffic INT COMMENT '总流量',
                                                      total_conversions INT COMMENT '总转化次数',
                                                      active_products INT COMMENT '动销商品数',
                                                      conversion_rate DECIMAL(5, 2) COMMENT '转化率（%）',
    active_rate DECIMAL(5, 2) COMMENT '动销率（%）'
    )
    COMMENT '品类属性分析表'
    STORED AS PARQUET;


-- 数据填充 SQL
INSERT INTO TABLE ads_attribute_analysis
SELECT
    category_id,
    attribute_name,
    analysis_date,
    time_dimension,
    SUM(traffic) AS total_traffic,
    SUM(active_product_count * payment_conversion_rate) AS total_conversions,
    SUM(active_product_count) AS active_products,
    ROUND(SUM(active_product_count * payment_conversion_rate) / NULLIF(SUM(traffic), 0) * 100, 2) AS conversion_rate,
    ROUND(SUM(active_product_count) / NULLIF(SUM(traffic), 0) * 100, 2) AS active_rate
FROM ods_category_attribute_analysis
GROUP BY category_id, attribute_name, analysis_date, time_dimension;


-- 1.检查时间维度分布：
SELECT time_dimension, COUNT(*)
FROM ads_attribute_analysis
GROUP BY time_dimension;


-- 2.验证转化率计算：
SELECT
    category_id,
    attribute_name,
    total_traffic,
    total_conversions,
    conversion_rate
FROM ads_attribute_analysis
WHERE total_traffic > 0
ORDER BY conversion_rate DESC;



-- 3.核对动销率逻辑：

SELECT
    category_id,
    attribute_name,
    active_products,
    total_traffic,
    active_rate
FROM ads_attribute_analysis
WHERE total_traffic > 0
ORDER BY active_rate DESC;

---------------





---------------------------

-- 流量分析
-- 流量分析帮助商家了解品类的流量来源及转化效果，通过对比店铺不同类目
-- 和其类目下商品的流量渠道分布，从而帮助优化不同类目商品在未来的不同
-- 渠道运营重点。其次热搜词分析展示消费者搜索关键词的排名和访客量，帮
-- 助商家了解哪些关键词能带来更多的流量。
-- ● 操作路径：生意参谋——商品——品类 360——详情——流量分析
-- ● 时间维度：按月查看



-- 创建 ADS 层流量分析表
CREATE TABLE IF NOT EXISTS ads_traffic_analysis (
                                                    category_id STRING COMMENT '品类ID',
                                                    traffic_source STRING COMMENT '流量来源',
                                                    analysis_month STRING COMMENT '分析月份（YYYY-MM）',
                                                    visitor_count INT COMMENT '总访客数',
                                                    conversion_count INT COMMENT '总转化数',
                                                    conversion_rate DECIMAL(5, 2) COMMENT '转化率（%）',
    search_keyword STRING COMMENT '热搜关键词',
    keyword_rank INT COMMENT '关键词排名',
    keyword_visitor_count INT COMMENT '关键词访客数'
    )
    COMMENT '品类流量分析表'
    STORED AS PARQUET;

-- 数据填充 SQL
INSERT INTO TABLE ads_traffic_analysis
SELECT
    m.category_id,
    m.traffic_source,
    m.analysis_month,
    m.visitor_count,
    m.conversion_count,
    ROUND(m.conversion_count / NULLIF(m.visitor_count, 0) * 100, 2) AS conversion_rate,
    k.search_keyword,
    k.keyword_rank,
    k.keyword_visitor_count
FROM (
         -- monthly_data子查询
         SELECT
             category_id,
             traffic_source,
             date_format(analysis_date, 'yyyy-MM') AS analysis_month,
             SUM(visitor_count) AS visitor_count,
             SUM(conversion_count) AS conversion_count,
             SUM(keyword_visitor_count) AS keyword_visitor_count
         FROM ods_category_traffic_analysis
         GROUP BY category_id, traffic_source, date_format(analysis_date, 'yyyy-MM')
     ) m
         LEFT JOIN (
    -- keyword_ranking子查询
    SELECT
        category_id,
        date_format(analysis_date, 'yyyy-MM') AS analysis_month,
        search_keyword,
        keyword_visitor_count,
        ROW_NUMBER() OVER (
            PARTITION BY category_id, date_format(analysis_date, 'yyyy-MM')
            ORDER BY keyword_visitor_count DESC
        ) AS keyword_rank
    FROM ods_category_traffic_analysis
    GROUP BY category_id, date_format(analysis_date, 'yyyy-MM'), search_keyword, keyword_visitor_count
) k
                   ON m.category_id = k.category_id
                       AND m.analysis_month = k.analysis_month
                       AND k.keyword_rank <= 3;


--------------------
-- 客群洞察
-- 客群洞察功能支持查看三种用户行为（搜索人群、访问人群、支付人群）访
-- 问店铺该品类后的人群画像。掌柜可以通过搜索人群可以优化直通车该品类
-- 下的投放的人群设置，基于访问和支付人群可以调整商品定价和投放策略。
-- ● 操作路径：生意参谋——商品——品类 360——详情——属性分析
-- ● 时间维度：按月查看















---------------------------------------------------------------------------------------------






-- 创建 DWS 层品类分析宽表
CREATE TABLE IF NOT EXISTS dws_category_summary (
                                                    category_id STRING COMMENT '品类ID',
                                                    category_name STRING COMMENT '品类名称',
                                                    analysis_date DATE COMMENT '分析日期',
                                                    time_dimension STRING COMMENT '时间维度（daily/weekly/monthly）',

    -- 销售指标
                                                    total_sales DECIMAL(10, 2) COMMENT '总销售额',
    total_quantity INT COMMENT '总销量',
    monthly_payment_progress DECIMAL(10, 2) COMMENT '月支付进度',
    monthly_payment_contribution DECIMAL(5, 2) COMMENT '月支付贡献度',
    last_month_payment_rank INT COMMENT '上月支付排名',

    -- 属性指标
    avg_traffic INT COMMENT '平均流量',
    avg_conversion_rate DECIMAL(5, 2) COMMENT '平均支付转化率',
    avg_active_products INT COMMENT '平均活跃商品数',

    -- 流量指标
    total_visitors INT COMMENT '总访客数',
    total_conversions INT COMMENT '总转化数',
    avg_conversion_rate_traffic DECIMAL(5, 2) COMMENT '流量转化率',
    top_keyword STRING COMMENT '热搜关键词',
    top_keyword_rank INT COMMENT '热搜词排名',
    keyword_visitors INT COMMENT '热搜词带来的访客数',

    -- 客群指标
    search_users INT COMMENT '搜索人群数量',
    visit_users INT COMMENT '访问人群数量',
    pay_users INT COMMENT '支付人群数量',
    user_age_distribution STRUCT<
    age_18_24: INT,
    age_25_35: INT,
    age_36_45: INT,
    age_45_plus: INT
    > COMMENT '年龄分布',
    gender_ratio STRUCT<male: DECIMAL(5, 2), female: DECIMAL(5, 2)> COMMENT '性别比例'
    )
    COMMENT '品类分析宽表（整合销售/属性/流量/客群数据）'
    STORED AS PARQUET;

INSERT INTO TABLE dws_category_summary
SELECT
    s.category_id,
    s.category_name,
    s.analysis_date,
    s.time_dimension,

    -- 销售指标
    s.total_sales,
    s.total_quantity,
    s.monthly_payment_progress,
    s.monthly_payment_contribution,
    s.last_month_payment_rank,

    -- 属性指标
    AVG(a.traffic) AS avg_traffic,
    AVG(a.payment_conversion_rate) AS avg_conversion_rate,
    AVG(a.active_product_count) AS avg_active_products,

    -- 流量指标
    SUM(t.visitor_count) AS total_visitors,
    SUM(t.conversion_count) AS total_conversions,
    ROUND(SUM(t.conversion_count)/NULLIF(SUM(t.visitor_count),0), 2) AS avg_conversion_rate_traffic,
    FIRST_VALUE(t.search_keyword) OVER (
        PARTITION BY t.category_id, t.analysis_date
        ORDER BY t.keyword_visitor_count DESC
    ) AS top_keyword,
        FIRST_VALUE(t.keyword_rank) OVER (
        PARTITION BY t.category_id, t.analysis_date
        ORDER BY t.keyword_visitor_count DESC
    ) AS top_keyword_rank,
        FIRST_VALUE(t.keyword_visitor_count) OVER (
        PARTITION BY t.category_id, t.analysis_date
        ORDER BY t.keyword_visitor_count DESC
    ) AS keyword_visitors,

    -- 客群指标
        SUM(CASE WHEN c.user_type = '搜索人群' THEN 1 ELSE 0 END) AS search_users,
    SUM(CASE WHEN c.user_type = '访问人群' THEN 1 ELSE 0 END) AS visit_users,
    SUM(CASE WHEN c.user_type = '支付人群' THEN 1 ELSE 0 END) AS pay_users,
    named_struct(
            'age_18_24', SUM(CASE WHEN c.age_group = '18-24岁' THEN 1 ELSE 0 END),
            'age_25_35', SUM(CASE WHEN c.age_group = '25-35岁' THEN 1 ELSE 0 END),
            'age_36_45', SUM(CASE WHEN c.age_group = '36-45岁' THEN 1 ELSE 0 END),
            'age_45_plus', SUM(CASE WHEN c.age_group = '45岁以上' THEN 1 ELSE 0 END)
        ) AS user_age_distribution,
    named_struct(
            'male', ROUND(SUM(CASE WHEN c.gender = '男' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0), 2),
            'female', ROUND(SUM(CASE WHEN c.gender = '女' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0), 2)
        ) AS gender_ratio
FROM ods_category_sales_analysis s
         LEFT JOIN ods_category_attribute_analysis a
                   ON s.category_id = a.category_id
                       AND s.analysis_date = a.analysis_date
         LEFT JOIN ods_category_traffic_analysis t
                   ON s.category_id = t.category_id
                       AND s.analysis_date = t.analysis_date
         LEFT JOIN ods_category_customer_insight c
                   ON s.category_id = c.category_id
GROUP BY
    s.category_id,
    s.category_name,
    s.analysis_date,
    s.time_dimension,
    s.total_sales,
    s.total_quantity,
    s.monthly_payment_progress,
    s.monthly_payment_contribution,
    s.last_month_payment_rank;




----------
INSERT INTO TABLE dws_category_summary
SELECT
    s.category_id,
    s.category_name,
    s.analysis_date,
    s.time_dimension,

    -- 销售指标
    s.total_sales,
    s.total_quantity,
    s.monthly_payment_progress,
    s.monthly_payment_contribution,
    s.last_month_payment_rank,

    -- 属性指标
    AVG(a.traffic) AS avg_traffic,
    AVG(a.payment_conversion_rate) AS avg_conversion_rate,
    AVG(a.active_product_count) AS avg_active_products,

    -- 流量指标
    SUM(t.visitor_count) AS total_visitors,
    SUM(t.conversion_count) AS total_conversions,
    ROUND(SUM(t.conversion_count)/NULLIF(SUM(t.visitor_count),0), 2) AS avg_conversion_rate_traffic,
    FIRST_VALUE(t.search_keyword) OVER (
        PARTITION BY s.category_id, s.analysis_date  -- 改用主表字段分区
        ORDER BY t.keyword_visitor_count DESC
    ) AS top_keyword,
        FIRST_VALUE(t.keyword_rank) OVER (
        PARTITION BY s.category_id, s.analysis_date  -- 改用主表字段分区
        ORDER BY t.keyword_visitor_count DESC
    ) AS top_keyword_rank,
        FIRST_VALUE(t.keyword_visitor_count) OVER (
        PARTITION BY s.category_id, s.analysis_date  -- 改用主表字段分区
        ORDER BY t.keyword_visitor_count DESC
    ) AS keyword_visitors,

    -- 客群指标
        SUM(CASE WHEN c.user_type = '搜索人群' THEN 1 ELSE 0 END) AS search_users,
    SUM(CASE WHEN c.user_type = '访问人群' THEN 1 ELSE 0 END) AS visit_users,
    SUM(CASE WHEN c.user_type = '支付人群' THEN 1 ELSE 0 END) AS pay_users,
    named_struct(
            'age_18_24', SUM(CASE WHEN c.age_group = '18-24岁' THEN 1 ELSE 0 END),
            'age_25_35', SUM(CASE WHEN c.age_group = '25-35岁' THEN 1 ELSE 0 END),
            'age_36_45', SUM(CASE WHEN c.age_group = '36-45岁' THEN 1 ELSE 0 END),
            'age_45_plus', SUM(CASE WHEN c.age_group = '45岁以上' THEN 1 ELSE 0 END)
        ) AS user_age_distribution,
    named_struct(
            'male', ROUND(SUM(CASE WHEN c.gender = '男' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0), 2),
            'female', ROUND(SUM(CASE WHEN c.gender = '女' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0), 2)
        ) AS gender_ratio
FROM ods_category_sales_analysis s
         LEFT JOIN ods_category_attribute_analysis a
                   ON s.category_id = a.category_id
                       AND s.analysis_date = a.analysis_date
         LEFT JOIN ods_category_traffic_analysis t
                   ON s.category_id = t.category_id
                       AND s.analysis_date = t.analysis_date
         LEFT JOIN ods_category_customer_insight c
                   ON s.category_id = c.category_id
GROUP BY
    s.category_id,
    s.category_name,
    s.analysis_date,
    s.time_dimension,
    s.total_sales,
    s.total_quantity,
    s.monthly_payment_progress,
    s.monthly_payment_contribution,
    s.last_month_payment_rank;






---------------
INSERT INTO TABLE dws_category_summary
SELECT
    s.category_id,
    s.category_name,
    s.analysis_date,
    s.time_dimension,

    -- 销售指标
    s.total_sales,
    s.total_quantity,
    s.monthly_payment_progress,
    s.monthly_payment_contribution,
    s.last_month_payment_rank,

    -- 属性指标
    AVG(a.traffic) AS avg_traffic,
    AVG(a.payment_conversion_rate) AS avg_conversion_rate,
    AVG(a.active_product_count) AS avg_active_products,

    -- 流量指标
    SUM(t.visitor_count) AS total_visitors,
    SUM(t.conversion_count) AS total_conversions,
    ROUND(SUM(t.conversion_count)/NULLIF(SUM(t.visitor_count),0), 2) AS avg_conversion_rate_traffic,
    FIRST_VALUE(t.search_keyword) OVER (
        PARTITION BY s.category_id, s.analysis_date
        ORDER BY SUM(t.keyword_visitor_count) DESC  -- 使用聚合后的值排序
    ) AS top_keyword,
        FIRST_VALUE(t.keyword_rank) OVER (
        PARTITION BY s.category_id, s.analysis_date
        ORDER BY SUM(t.keyword_visitor_count) DESC  -- 使用聚合后的值排序
    ) AS top_keyword_rank,
        FIRST_VALUE(SUM(t.keyword_visitor_count)) OVER (  -- 改用聚合后的值
        PARTITION BY s.category_id, s.analysis_date
        ORDER BY SUM(t.keyword_visitor_count) DESC
    ) AS keyword_visitors,

    -- 客群指标
        SUM(CASE WHEN c.user_type = '搜索人群' THEN 1 ELSE 0 END) AS search_users,
    SUM(CASE WHEN c.user_type = '访问人群' THEN 1 ELSE 0 END) AS visit_users,
    SUM(CASE WHEN c.user_type = '支付人群' THEN 1 ELSE 0 END) AS pay_users,
    named_struct(
            'age_18_24', SUM(CASE WHEN c.age_group = '18-24岁' THEN 1 ELSE 0 END),
            'age_25_35', SUM(CASE WHEN c.age_group = '25-35岁' THEN 1 ELSE 0 END),
            'age_36_45', SUM(CASE WHEN c.age_group = '36-45岁' THEN 1 ELSE 0 END),
            'age_45_plus', SUM(CASE WHEN c.age_group = '45岁以上' THEN 1 ELSE 0 END)
        ) AS user_age_distribution,
    named_struct(
            'male', ROUND(SUM(CASE WHEN c.gender = '男' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0), 2),
            'female', ROUND(SUM(CASE WHEN c.gender = '女' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0), 2)
        ) AS gender_ratio
FROM ods_category_sales_analysis s
         LEFT JOIN ods_category_attribute_analysis a
                   ON s.category_id = a.category_id
                       AND s.analysis_date = a.analysis_date
         LEFT JOIN ods_category_traffic_analysis t
                   ON s.category_id = t.category_id
                       AND s.analysis_date = t.analysis_date
         LEFT JOIN ods_category_customer_insight c
                   ON s.category_id = c.category_id
GROUP BY
    s.category_id,
    s.category_name,
    s.analysis_date,
    s.time_dimension,
    s.total_sales,
    s.total_quantity,
    s.monthly_payment_progress,
    s.monthly_payment_contribution,
    s.last_month_payment_rank;




-------------
INSERT INTO TABLE dws_category_summary
SELECT
    s.category_id,
    s.category_name,
    s.analysis_date,
    s.time_dimension,

    -- 销售指标
    s.total_sales,
    s.total_quantity,
    s.monthly_payment_progress,
    s.monthly_payment_contribution,
    s.last_month_payment_rank,

    -- 属性指标
    AVG(a.traffic) AS avg_traffic,
    AVG(a.payment_conversion_rate) AS avg_conversion_rate,
    AVG(a.active_product_count) AS avg_active_products,

    -- 流量指标
    t.total_visitors,
    t.total_conversions,
    t.avg_conversion_rate_traffic,
    t.top_keyword,
    t.top_keyword_rank,
    t.keyword_visitors,

    -- 客群指标
    SUM(CASE WHEN c.user_type = '搜索人群' THEN 1 ELSE 0 END) AS search_users,
    SUM(CASE WHEN c.user_type = '访问人群' THEN 1 ELSE 0 END) AS visit_users,
    SUM(CASE WHEN c.user_type = '支付人群' THEN 1 ELSE 0 END) AS pay_users,
    named_struct(
            'age_18_24', SUM(CASE WHEN c.age_group = '18-24岁' THEN 1 ELSE 0 END),
            'age_25_35', SUM(CASE WHEN c.age_group = '25-35岁' THEN 1 ELSE 0 END),
            'age_36_45', SUM(CASE WHEN c.age_group = '36-45岁' THEN 1 ELSE 0 END),
            'age_45_plus', SUM(CASE WHEN c.age_group = '45岁以上' THEN 1 ELSE 0 END)
        ) AS user_age_distribution,
    named_struct(
            'male', ROUND(SUM(CASE WHEN c.gender = '男' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0), 2),
            'female', ROUND(SUM(CASE WHEN c.gender = '女' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0), 2)
        ) AS gender_ratio
FROM ods_category_sales_analysis s
         LEFT JOIN ods_category_attribute_analysis a
                   ON s.category_id = a.category_id
                       AND s.analysis_date = a.analysis_date
         LEFT JOIN (
    SELECT
        category_id,
        analysis_date,
        SUM(visitor_count) AS total_visitors,
        SUM(conversion_count) AS total_conversions,
        ROUND(SUM(conversion_count)/NULLIF(SUM(visitor_count),0), 2) AS avg_conversion_rate_traffic,
        FIRST_VALUE(search_keyword) OVER (
            PARTITION BY category_id, analysis_date
            ORDER BY SUM(keyword_visitor_count) DESC
        ) AS top_keyword,
            FIRST_VALUE(keyword_rank) OVER (
            PARTITION BY category_id, analysis_date
            ORDER BY SUM(keyword_visitor_count) DESC
        ) AS top_keyword_rank,
            FIRST_VALUE(SUM(keyword_visitor_count)) OVER (
            PARTITION BY category_id, analysis_date
            ORDER BY SUM(keyword_visitor_count) DESC
        ) AS keyword_visitors
    FROM ods_category_traffic_analysis
    GROUP BY category_id, analysis_date
) t ON s.category_id = t.category_id AND s.analysis_date = t.analysis_date
         LEFT JOIN ods_category_customer_insight c
                   ON s.category_id = c.category_id
GROUP BY
    s.category_id,
    s.category_name,
    s.analysis_date,
    s.time_dimension,
    s.total_sales,
    s.total_quantity,
    s.monthly_payment_progress,
    s.monthly_payment_contribution,
    s.last_month_payment_rank,
    t.total_visitors,
    t.total_conversions,
    t.avg_conversion_rate_traffic,
    t.top_keyword,
    t.top_keyword_rank,
    t.keyword_visitors;
------------------------------------------

INSERT INTO TABLE dws_category_summary
SELECT
    s.category_id,
    s.category_name,
    s.analysis_date,
    s.time_dimension,

    -- 销售指标
    s.total_sales,
    s.total_quantity,
    s.monthly_payment_progress,
    s.monthly_payment_contribution,
    s.last_month_payment_rank,

    -- 属性指标
    AVG(a.traffic) AS avg_traffic,
    AVG(a.payment_conversion_rate) AS avg_conversion_rate,
    AVG(a.active_product_count) AS avg_active_products,

    -- 流量指标
    t.total_visitors,
    t.total_conversions,
    t.avg_conversion_rate_traffic,
    t.top_keyword,
    t.top_keyword_rank,
    t.keyword_visitors,

    -- 客群指标
    SUM(CASE WHEN c.user_type = '搜索人群' THEN 1 ELSE 0 END) AS search_users,
    SUM(CASE WHEN c.user_type = '访问人群' THEN 1 ELSE 0 END) AS visit_users,
    SUM(CASE WHEN c.user_type = '支付人群' THEN 1 ELSE 0 END) AS pay_users,
    named_struct(
            'age_18_24', SUM(CASE WHEN c.age_group = '18-24岁' THEN 1 ELSE 0 END),
            'age_25_35', SUM(CASE WHEN c.age_group = '25-35岁' THEN 1 ELSE 0 END),
            'age_36_45', SUM(CASE WHEN c.age_group = '36-45岁' THEN 1 ELSE 0 END),
            'age_45_plus', SUM(CASE WHEN c.age_group = '45岁以上' THEN 1 ELSE 0 END)
        ) AS user_age_distribution,
    named_struct(
            'male', ROUND(SUM(CASE WHEN c.gender = '男' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0), 2),
            'female', ROUND(SUM(CASE WHEN c.gender = '女' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0), 2)
        ) AS gender_ratio
FROM ods_category_sales_analysis s
         LEFT JOIN ods_category_attribute_analysis a
                   ON s.category_id = a.category_id
                       AND s.analysis_date = a.analysis_date
         LEFT JOIN (
    -- 调整子查询结构，先计算窗口函数再聚合
    SELECT
        category_id,
        analysis_date,
        total_visitors,
        total_conversions,
        avg_conversion_rate_traffic,
        top_keyword,
        top_keyword_rank,
        keyword_visitors
    FROM (
             SELECT
                 category_id,
                 analysis_date,
                 SUM(visitor_count) AS total_visitors,
                 SUM(conversion_count) AS total_conversions,
                 ROUND(SUM(conversion_count)/NULLIF(SUM(visitor_count),0), 2) AS avg_conversion_rate_traffic,
                 FIRST_VALUE(search_keyword) OVER (
                PARTITION BY category_id, analysis_date
                ORDER BY SUM(keyword_visitor_count) DESC
            ) AS top_keyword,
                     FIRST_VALUE(keyword_rank) OVER (
                PARTITION BY category_id, analysis_date
                ORDER BY SUM(keyword_visitor_count) DESC
            ) AS top_keyword_rank,
                     FIRST_VALUE(SUM(keyword_visitor_count)) OVER (
                PARTITION BY category_id, analysis_date
                ORDER BY SUM(keyword_visitor_count) DESC
            ) AS keyword_visitors
             FROM ods_category_traffic_analysis
             GROUP BY category_id, analysis_date ,search_keyword,keyword_rank
         ) sub
) t ON s.category_id = t.category_id AND s.analysis_date = t.analysis_date
         LEFT JOIN ods_category_customer_insight c
                   ON s.category_id = c.category_id
GROUP BY
    s.category_id,
    s.category_name,
    s.analysis_date,
    s.time_dimension,
    s.total_sales,
    s.total_quantity,
    s.monthly_payment_progress,
    s.monthly_payment_contribution,
    s.last_month_payment_rank,
    t.total_visitors,
    t.total_conversions,
    t.avg_conversion_rate_traffic,
    t.top_keyword,
    t.top_keyword_rank,
    t.keyword_visitors
;

---------------
INSERT INTO TABLE dws_category_summary
SELECT
    s.category_id,
    s.category_name,
    s.analysis_date,
    s.time_dimension,

    -- 销售指标
    s.total_sales,
    s.total_quantity,
    s.monthly_payment_progress,
    s.monthly_payment_contribution,
    s.last_month_payment_rank,

    -- 属性指标
    AVG(a.traffic) AS avg_traffic,
    AVG(a.payment_conversion_rate) AS avg_conversion_rate,
    AVG(a.active_product_count) AS avg_active_products,

    -- 流量指标
    t.total_visitors,
    t.total_conversions,
    t.avg_conversion_rate_traffic,
    t.top_keyword,
    t.top_keyword_rank,
    t.keyword_visitors,

    -- 客群指标
    SUM(CASE WHEN c.user_type = '搜索人群' THEN 1 ELSE 0 END) AS search_users,
    SUM(CASE WHEN c.user_type = '访问人群' THEN 1 ELSE 0 END) AS visit_users,
    SUM(CASE WHEN c.user_type = '支付人群' THEN 1 ELSE 0 END) AS pay_users,
    named_struct(
            'age_18_24', CAST(SUM(CASE WHEN c.age_group = '18-24岁' THEN 1 ELSE 0 END) AS INT),
            'age_25_35', CAST(SUM(CASE WHEN c.age_group = '25-35岁' THEN 1 ELSE 0 END) AS INT),
            'age_36_45', CAST(SUM(CASE WHEN c.age_group = '36-45岁' THEN 1 ELSE 0 END) AS INT),
            'age_45_plus', CAST(SUM(CASE WHEN c.age_group = '45岁以上' THEN 1 ELSE 0 END) AS INT)
        ) AS user_age_distribution,
    named_struct(
            'male', ROUND(SUM(CASE WHEN c.gender = '男' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0), 2),
            'female', ROUND(SUM(CASE WHEN c.gender = '女' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0), 2)
        ) AS gender_ratio
FROM ods_category_sales_analysis s
         LEFT JOIN ods_category_attribute_analysis a
                   ON s.category_id = a.category_id
                       AND s.analysis_date = a.analysis_date
         LEFT JOIN (
    SELECT
        category_id,
        analysis_date,
        SUM(visitor_count) AS total_visitors,
        SUM(conversion_count) AS total_conversions,
        ROUND(SUM(conversion_count)/NULLIF(SUM(visitor_count),0), 2) AS avg_conversion_rate_traffic,
        FIRST_VALUE(search_keyword) OVER (
            PARTITION BY category_id, analysis_date
            ORDER BY SUM(keyword_visitor_count) DESC
        ) AS top_keyword,
            FIRST_VALUE(keyword_rank) OVER (
            PARTITION BY category_id, analysis_date
            ORDER BY SUM(keyword_visitor_count) DESC
        ) AS top_keyword_rank,
            FIRST_VALUE(SUM(keyword_visitor_count)) OVER (
            PARTITION BY category_id, analysis_date
            ORDER BY SUM(keyword_visitor_count) DESC
        ) AS keyword_visitors
    FROM ods_category_traffic_analysis
    GROUP BY category_id, analysis_date
) t ON s.category_id = t.category_id AND s.analysis_date = t.analysis_date
         LEFT JOIN ods_category_customer_insight c
                   ON s.category_id = c.category_id
GROUP BY
    s.category_id,
    s.category_name,
    s.analysis_date,
    s.time_dimension,
    s.total_sales,
    s.total_quantity,
    s.monthly_payment_progress,
    s.monthly_payment_contribution,
    s.last_month_payment_rank,
    t.total_visitors,
    t.total_conversions,
    t.avg_conversion_rate_traffic,
    t.top_keyword,
    t.top_keyword_rank,
    t.keyword_visitors;

------------


CREATE TABLE dws_category_wide_analysis (
    -- 维度字段
                                            category_id      STRING COMMENT '品类ID',
                                            category_name    STRING COMMENT '品类名称',
                                            analysis_date    DATE COMMENT '分析日期',
                                            time_dimension   STRING COMMENT '时间维度(daily/weekly/monthly)',

    -- 销售指标
                                            total_sales               DECIMAL(15,2) COMMENT '总销售额',
                                            total_quantity            BIGINT COMMENT '总销量',
                                            monthly_payment_progress  DECIMAL(5,2) COMMENT '月支付进度',
                                            payment_contribution      DECIMAL(5,2) COMMENT '支付贡献度',
                                            last_month_rank           INT COMMENT '上月排名',

    -- 属性指标
                                            attribute_traffic         BIGINT COMMENT '属性流量',
                                            payment_conversion_rate   DECIMAL(5,2) COMMENT '支付转化率',
                                            active_products           INT COMMENT '活跃商品数',

    -- 流量指标
                                            total_visitors            BIGINT COMMENT '总访客数',
                                            search_keyword_top3       ARRAY<STRING> COMMENT 'TOP3搜索关键词',
                                            avg_keyword_rank         DECIMAL(5,2) COMMENT '平均关键词排名',

    -- 客群指标
                                            new_user_ratio           DECIMAL(5,2) COMMENT '新客占比',
                                            high_frequency_ratio     DECIMAL(5,2) COMMENT '高频购买用户占比',
                                            dominant_age_group       STRING COMMENT '主力年龄区间',
                                            gender_distribution      MAP<STRING,DECIMAL> COMMENT '性别分布',

    -- 时间扩展维度
                                            year                     INT COMMENT '年份',
                                            quarter                  INT COMMENT '季度',
                                            month                    INT COMMENT '月份',
                                            week_of_year             INT COMMENT '年周数'
)
    COMMENT '品类多维分析宽表'
PARTITIONED BY (dt STRING COMMENT '日期分区')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');



INSERT OVERWRITE TABLE dws_category_wide_analysis PARTITION(dt)
SELECT
    s.category_id,
    MAX(s.category_name) AS category_name,
    s.analysis_date,
    s.time_dimension,
    -- 销售指标
    SUM(s.total_sales) AS total_sales,
    SUM(s.total_quantity) AS total_quantity,
    AVG(s.monthly_payment_progress) AS monthly_payment_progress,
    RANK() OVER (ORDER BY SUM(s.total_sales) DESC) AS payment_contribution,
        s.last_month_payment_rank,
    -- 属性指标
    MAX(a.traffic) AS attribute_traffic,
    AVG(a.payment_conversion_rate) AS payment_conversion_rate,
    MAX(a.active_product_count) AS active_products,
    -- 流量指标
    SUM(t.visitor_count) AS total_visitors,
    COLLECT_LIST(t.search_keyword)[0:3] AS search_keyword_top3,
    AVG(t.keyword_rank) AS avg_keyword_rank,
    -- 客群指标
    SUM(CASE WHEN c.user_type='new' THEN 1 ELSE 0 END)/COUNT(*) AS new_user_ratio,
    SUM(CASE WHEN c.purchase_frequency='high' THEN 1 ELSE 0 END)/COUNT(*) AS high_freq_ratio,
    PERCENTILE_APPROX(c.age_group, 0.5) AS dominant_age_group,
    MAP('male', SUM(CASE c.gender WHEN 'M' THEN 1 ELSE 0 END)/COUNT(*),
        'female', SUM(CASE c.gender WHEN 'F' THEN 1 ELSE 0 END)/COUNT(*)) AS gender_distribution,
    -- 时间维度扩展
    YEAR(s.analysis_date),
    QUARTER(s.analysis_date),
    MONTH(s.analysis_date),
    WEEKOFYEAR(s.analysis_date),
    s.dt
FROM ods_category_sales_analysis s
    LEFT JOIN ods_category_attribute_analysis a
ON s.category_id = a.category_id AND s.analysis_date = a.analysis_date
    LEFT JOIN (
    SELECT category_id, analysis_date,
    SUM(visitor_count) AS visitor_count,
    COLLECT_SET(search_keyword) AS search_keyword,
    AVG(keyword_rank) AS keyword_rank
    FROM ods_category_traffic_analysis
    GROUP BY category_id, analysis_date
    ) t ON s.category_id = t.category_id AND s.analysis_date = t.analysis_date
    LEFT JOIN ods_category_customer_insight c
    ON s.category_id = c.category_id
GROUP BY s.category_id, s.analysis_date, s.time_dimension, s.dt;




--------------------------

CREATE TABLE IF NOT EXISTS dws_category_wide (
    -- 基础维度
                                                 category_id STRING COMMENT '品类ID',
                                                 category_name STRING COMMENT '品类名称',
                                                 time_dimension STRING COMMENT '时间维度（daily/weekly/monthly）',
                                                 analysis_date DATE COMMENT '分析日期',

    -- 销售指标
                                                 total_sales DECIMAL(18,2) COMMENT '总销售额',
    total_quantity BIGINT COMMENT '总销量',
    monthly_payment_contribution DECIMAL(5,2) COMMENT '月支付贡献度',

    -- 流量指标
    daily_uv BIGINT COMMENT '日独立访客数',
    avg_conversion_rate DECIMAL(5,2) COMMENT '平均转化率',
    top_search_keyword STRING COMMENT 'TOP1搜索关键词',

    -- 商品属性指标
    active_product_count INT COMMENT '活跃商品数',
    main_attribute STRING COMMENT '核心属性',

    -- 客群画像指标
    dominant_age_group STRING COMMENT '主导年龄层',
    gender_ratio DECIMAL(5,2) COMMENT '性别比例（女性占比）',
    high_freq_user_count BIGINT COMMENT '高频用户数'
    )
    COMMENT '品类维度宽表'
    PARTITIONED BY (dt STRING COMMENT '日期分区（冗余字段）')
    CLUSTERED BY (category_id) INTO 10 BUCKETS
    STORED AS PARQUET;



INSERT OVERWRITE TABLE dws_category_wide PARTITION(dt='${date}')
SELECT
    -- 基础维度
    s.category_id,
    MAX(s.category_name) AS category_name,
    s.time_dimension,
    MAX(s.analysis_date) AS analysis_date,

    -- 销售指标
    SUM(s.total_sales) AS total_sales,
    SUM(s.total_quantity) AS total_quantity,
    AVG(s.monthly_payment_contribution) AS monthly_payment_contribution,

    -- 流量指标（按时间维度聚合）
    MAX(t.visitor_count) AS daily_uv,
    ROUND(SUM(t.conversion_count)/SUM(t.visitor_count), 2) AS avg_conversion_rate,
    MAX(if(rk=1, t.search_keyword, NULL)) AS top_search_keyword,

    -- 商品属性指标
    MAX(a.active_product_count) AS active_product_count,
    MAX(a.attribute_name) AS main_attribute,

    -- 客群画像指标
    MAX(c.age_group) AS dominant_age_group,
    ROUND(SUM(CASE WHEN c.gender='女' THEN 1 ELSE 0 END)/COUNT(*), 2) AS gender_ratio,
    SUM(CASE WHEN c.purchase_frequency='高频' THEN 1 ELSE 0 END) AS high_freq_user_count
FROM ods_category_sales_analysis s
         LEFT JOIN (
    SELECT
        category_id,
        analysis_date,
        visitor_count,
        conversion_count,
        search_keyword,
        ROW_NUMBER() OVER(PARTITION BY category_id ORDER BY keyword_visitor_count DESC) AS rk
    FROM ods_category_traffic_analysis
) t ON s.category_id = t.category_id AND s.analysis_date = t.analysis_date
         LEFT JOIN (
    SELECT
        category_id,
        attribute_name,
        active_product_count,
        ROW_NUMBER() OVER(PARTITION BY category_id ORDER BY traffic DESC) AS attr_rk
    FROM ods_category_attribute_analysis
) a ON s.category_id = a.category_id AND a.attr_rk = 1
         LEFT JOIN ods_category_customer_insight c ON s.category_id = c.category_id
WHERE s.dt = '${date}'
GROUP BY
    s.category_id,
    s.time_dimension;























