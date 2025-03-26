



use dev_realtime_tms_zhengwei_zhou;





DROP TABLE IF EXISTS dev_realtime_tms_zhengwei_zhou.dws_trade_cargo_collect_city_order_inc_1d;
CREATE EXTERNAL TABLE dev_realtime_tms_zhengwei_zhou.dws_trade_cargo_collect_city_order_inc_1d
(
    `cargo_type`       STRING COMMENT '货物类型id',
    `cargo_type_name`       STRING COMMENT '货物类型名称',
    `collect_type`     STRING COMMENT '取件类型，1为网点自寄，2为上门取件',
    `collect_type_name`     STRING COMMENT '取件类型名称，1为网点自寄，2为上门取件',
    `receiver_city_id` STRING COMMENT '收件人城市id',
    `receiver_city_name` STRING COMMENT '收件人城市名称',
    `order_cnt`        BIGINT COMMENT '下单数',
    `order_amount`     DECIMAL(32, 2) COMMENT '下单金额'
) COMMENT '最近1日数据汇总表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/2207A/zhengwei_zhou/tms/dws/dws_trade_cargo_collect_city_order_inc_1d';


-- todo：最近30日（1月）数据汇总表
DROP TABLE IF EXISTS dev_realtime_tms_zhengwei_zhou.dws_trade_cargo_collect_city_order_inc_30d;
CREATE EXTERNAL TABLE dev_realtime_tms_zhengwei_zhou.dws_trade_cargo_collect_city_order_inc_30d
(
    `cargo_type`       STRING COMMENT '货物类型id',
    `cargo_type_name`       STRING COMMENT '货物类型名称',
    `collect_type`     STRING COMMENT '取件类型，1为网点自寄，2为上门取件',
    `collect_type_name`     STRING COMMENT '取件类型名称，1为网点自寄，2为上门取件',
    `receiver_city_id` STRING COMMENT '收件人城市id',
    `receiver_city_name` STRING COMMENT '收件人城市名称',
    `order_cnt`        BIGINT COMMENT '下单数',
    `order_amount`     DECIMAL(32, 2) COMMENT '下单金额'
) COMMENT '最近1日数据汇总表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/2207A/zhengwei_zhou/tms/dws/dws_trade_cargo_collect_city_order_inc_30d';



/*
2）、编写HiveSQL，完成最近1日数据汇总表数据装载，并查看汇总表结果；（5分）
*/
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
INSERT INTO dev_realtime_tms_zhengwei_zhou.dws_trade_cargo_collect_city_order_inc_1d PARTITION (dt)
SELECT
     t1.cargo_type
     , t2.cargo_type_name
     , t1.collect_type
     , t3.collect_type_name
     , t1.receiver_city_id
     , t4.receiver_city_name
     , t1.order_cnt
     , t1.order_amount
     , dt
FROM (
    -- 1按照日期、货物类型、区间类型和收件人城市分组，聚合统计订单数和订单额
         SELECT
             dt
              , cargo_type
              , collect_type
              , receiver_city_id
              , count(distinct order_id) AS order_cnt
              , sum(amount) AS order_amount
         FROM dev_realtime_tms_zhengwei_zhou.dwd_trade_order_inc

         GROUP BY dt, cargo_type, collect_type, receiver_city_id
) t1
LEFT JOIN (
    -- 2字典表：货物类型数据
    SELECT
        id, name AS cargo_type_name
    FROM dev_realtime_tms_zhengwei_zhou.ods_base_dic

) t2 ON t1.cargo_type = t2.id
LEFT JOIN (
    -- 3字典表：区间类型类型数据
    SELECT
        id, name AS collect_type_name
    FROM dev_realtime_tms_zhengwei_zhou.ods_base_dic

) t3 ON t1.collect_type = t3.id
LEFT JOIN (
    -- 4维度表：地区表
    SELECT
        id, name AS receiver_city_name
    FROM dev_realtime_tms_zhengwei_zhou.dim_base_region_info

) t4 ON t1.receiver_city_id = t4.id
;



-- 查看数据
SELECT * FROM dev_realtime_tms_zhengwei_zhou.dws_trade_cargo_collect_city_order_inc_1d LIMIT 10 ;

/*
3）、依据最近1日汇总结果数据，编写SQL语句统计最近30日（1月）数据数据装载；（4分）
*/
INSERT INTO dev_realtime_tms_zhengwei_zhou.dws_trade_cargo_collect_city_order_inc_30d PARTITION (dt = '2025-03-25')
SELECT
    cargo_type, cargo_type_name
    , collect_type, collect_type_name
    , receiver_city_id, receiver_city_name
    , sum(order_cnt) AS order_cnt
    , sum(order_amount) AS order_amount
FROM dev_realtime_tms_zhengwei_zhou.dws_trade_cargo_collect_city_order_inc_1d
WHERE dt >= date_sub('2022-06-05', 29) AND dt <= '2025-03-25'
GROUP BY cargo_type, cargo_type_name,
         collect_type, collect_type_name,
         receiver_city_id, receiver_city_name;


-- 查询数据
SELECT * FROM dev_realtime_tms_zhengwei_zhou.dws_trade_cargo_collect_city_order_inc_30d LIMIT 10 ;








