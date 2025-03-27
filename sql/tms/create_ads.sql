
/*
todo 1）、运单综合统计：统计最近1日、7日、30日总的运单数量和运单金额； （5分）
*/
-- 1.最近1日

-- 创建 ads 层表，用于存储不同时间窗口的订单统计数据


CREATE TABLE IF NOT EXISTS dev_realtime_tms_zhengwei_zhou.ads_trade_cargo_collect_city_order_summary (
    dt DATE COMMENT '统计日期',
    recent_days INT COMMENT '统计的最近天数窗口，1 代表最近 1 日，7 代表最近 7 日，30 代表最近 30 日',
    order_cnt BIGINT COMMENT '订单数量',
    order_amount DECIMAL(10, 2) COMMENT '订单金额，假设金额最多 10 位数字，2 位小数，可按需调整'
)
COMMENT 'ads层不同时间窗口的城市货物揽收订单统计汇总表'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/2207A/zhengwei_zhou/tms/ads/ads_trade_cargo_collect_city_order_summary';

insert into dev_realtime_tms_zhengwei_zhou.ads_trade_cargo_collect_city_order_summary
SELECT
    '2025-03-25' AS dt
    , 1 AS recent_days
    , sum(order_cnt) AS order_cnt
    , sum(order_amount) AS order_amount
FROM dev_realtime_tms_zhengwei_zhou.dws_trade_cargo_collect_city_order_inc_1d
WHERE dt = '2025-03-25'
UNION
-- 2.最近7日
SELECT
    '2025-03-25' AS dt
     , 7 AS recent_days
     , sum(order_cnt) AS order_cnt
     , sum(order_amount) AS order_amount
FROM dev_realtime_tms_zhengwei_zhou.dws_trade_cargo_collect_city_order_inc_1d
WHERE dt >= date_sub('2025-03-25', 6) AND dt <= '2025-03-25'
UNION
-- 3.最近30日
SELECT
    '2025-03-25' AS dt
     , 30 AS recent_days
     , sum(order_cnt) AS order_cnt
     , sum(order_amount) AS order_amount
FROM dev_realtime_tms_zhengwei_zhou.dws_trade_cargo_collect_city_order_inc_30d
WHERE dt = '2025-03-25'
;



/*
todo 2）、各省份运单综合统计：依据发件省份，统计下单事实表中不同省份运单数量和运单金额，展示下单分布状态。 （5分）
*/

-- 创建 ads 层表，用于存储按省份统计的订单数据
CREATE TABLE IF NOT EXISTS dev_realtime_tms_zhengwei_zhou.ads_trade_order_by_province (
    dt DATE COMMENT '统计日期',
    receiver_province_id INT COMMENT '收件省份ID',
    province_name STRING COMMENT '省份名称',
    order_cnt BIGINT COMMENT '订单数量',
    order_amount DECIMAL(10, 2) COMMENT '订单金额，假设金额最多10位数字，2位小数，可按需调整'
)
COMMENT 'ads层按省份统计的订单信息表'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/2207A/zhengwei_zhou/tms/ads/ads_trade_order_by_province';



insert into ads_trade_order_by_province
SELECT
    '2025-03-25' AS dt
    , receiver_province_id
    , province_name
    , order_cnt
    , order_amount
FROM (
    -- 1按照省份id分组统计
    SELECT
     receiver_province_id
      , count(*) AS order_cnt
      , sum(amount) AS order_amount
    FROM dev_realtime_tms_zhengwei_zhou.dwd_trade_order_inc
    WHERE dt = '2025-03-25'
    GROUP BY receiver_province_id
) t1
LEFT JOIN (
    -- 2获取省份数据
    SELECT
        id, name AS province_name
    FROM dev_realtime_tms_zhengwei_zhou.dim_base_region_info
    WHERE dt = '2025-03-25' AND parent_id = 86
) t2 ON t1.receiver_province_id = t2.id
;



/*
todo 3）、各类型货物运单统计：每日货物类型下单的状态分布表的设计，根据货物类型，统计下单后的不同运单状态的运单数量。 （5分）
 */

-- 创建ads层表，用于存储按货物类型统计的订单数据
CREATE TABLE IF NOT EXISTS dev_realtime_tms_zhengwei_zhou.ads_trade_cargo_type_order_summary (
    cargo_type STRING COMMENT '货物类型标识',
    cargo_type_name STRING COMMENT '货物类型名称',
    order_cnt BIGINT COMMENT '订单数量'
)
COMMENT 'ads层按货物类型统计的城市货物揽收订单汇总表'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/2207A/zhengwei_zhou/tms/ads/ads_trade_cargo_type_order_summary';




insert into ads_trade_cargo_type_order_summary
SELECT
    cargo_type
    , cargo_type_name
    , sum(order_cnt) AS order_cnt
FROM dev_realtime_tms_zhengwei_zhou.dws_trade_cargo_collect_city_order_inc_1d
WHERE dt = '2025-03-25'
GROUP BY cargo_type, cargo_type_name
;


