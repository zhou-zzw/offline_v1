



use dev_realtime_tms_zhengwei_zhou;


DROP TABLE IF EXISTS dev_realtime_tms_zhengwei_zhou.dwd_trade_order_inc;
CREATE EXTERNAL TABLE dev_realtime_tms_zhengwei_zhou.dwd_trade_order_inc
(
    -- 运单明细表字段
    `id`            BIGINT,
    `order_id`      STRING COMMENT '订单id',
    `cargo_type`    STRING COMMENT '货物类型id',
    `volume_length` BIGINT COMMENT '长cm',
    `volume_width`  BIGINT COMMENT '宽cm',
    `volume_height` BIGINT COMMENT '高cm',
    `weight`        DECIMAL(16, 2) COMMENT '重量 kg',
    `remark`        STRING COMMENT '备注',
    `create_time`   STRING COMMENT '创建时间',
    `update_time`   STRING COMMENT '更新时间',
    -- 运单表字段
    `order_no`             STRING COMMENT '订单号',
    `status`               STRING COMMENT '订单状态',
    `collect_type`         STRING COMMENT '取件类型，1为网点自寄，2为上门取件',
    `user_id`              BIGINT COMMENT '客户id',
    `receiver_complex_id`  BIGINT COMMENT '收件人小区id',
    `receiver_province_id` STRING COMMENT '收件人省份id',
    `receiver_city_id`     STRING COMMENT '收件人城市id',
    `receiver_district_id` STRING COMMENT '收件人区县id',
    `receiver_address`     STRING COMMENT '收件人详细地址',
    `receiver_name`        STRING COMMENT '收件人姓名',
    `receiver_phone`       STRING COMMENT '收件人电话',
    `receive_location`     STRING COMMENT '起始点经纬度',
    `sender_complex_id`    bigint COMMENT '发件人小区id',
    `sender_province_id`   STRING COMMENT '发件人省份id',
    `sender_city_id`       STRING COMMENT '发件人城市id',
    `sender_district_id`   STRING COMMENT '发件人区县id',
    `sender_address`       STRING COMMENT '发件人详细地址',
    `sender_name`          STRING COMMENT '发件人姓名',
    `sender_phone`         STRING COMMENT '发件人电话',
    `send_location`        STRING COMMENT '发件人坐标',
    `payment_type`         STRING COMMENT '支付方式',
    `cargo_num`            BIGINT COMMENT '货物个数',
    `amount`               DECIMAL(32, 2) COMMENT '金额',
    `estimate_arrive_time` STRING COMMENT '预计到达时间',
    `distance`             DECIMAL(10, 2) COMMENT '距离，单位：公里'
) COMMENT '交易域-下单-事务事实表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/2207A/zhengwei_zhou/tms/dwd/dwd_trade_order_inc';

/*
2）、合理选择维度建模中建模类型（星型模型、雪花模型和星座模型），编写HiveSQL语句，从ODS层读取相关业务表数据进行关联（选择合适JOIN方式），构建事实宽表数据并插入表中。（5分）
*/
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
INSERT INTO dev_realtime_tms_zhengwei_zhou.dwd_trade_order_inc PARTITION (dt)
SELECT
    t1.*,
    order_no, status, collect_type, user_id, receiver_complex_id, receiver_province_id, receiver_city_id, receiver_district_id, receiver_address, receiver_name, receiver_phone, receive_location, sender_complex_id, sender_province_id, sender_city_id, sender_district_id, sender_address, sender_name, sender_phone, send_location, payment_type, cargo_num, amount, estimate_arrive_time, distance,
    date_format(t1.create_time, 'yyyy-MM-dd') AS dt
FROM (
    SELECT
        id, order_id, cargo_type, volume_length, volume_width, volume_height, weight, remark, create_time, update_time
    FROM dev_realtime_tms_zhengwei_zhou.ods_order_cargo
    WHERE dt = '2025-03-25'
) t1
LEFT JOIN (
    SELECT
        id, order_no, status, collect_type, user_id, receiver_complex_id, receiver_province_id, receiver_city_id, receiver_district_id, receiver_address, receiver_name, receiver_phone, receive_location, sender_complex_id, sender_province_id, sender_city_id, sender_district_id, sender_address, sender_name, sender_phone, send_location, payment_type, cargo_num, amount, estimate_arrive_time, distance
    FROM dev_realtime_tms_zhengwei_zhou.ods_order_info
    WHERE dt = '2025-03-25'
) t2 ON t1.order_id = t2.id ;

-- 查看分区数目
SHOW PARTITIONS dev_realtime_tms_zhengwei_zhou.dwd_trade_order_inc ;



/*
3）、考虑事实表数据构建HiveSQL语句执行时性能问题，比如数据倾斜如何处理，列举出至少2种解决方案，并且调试对比效果。（5分）
*/
-- 性能优化1：如果时大表对小表，启用map join
// todo 设置自动选择MapJoin，默认是true
SET hive.auto.convert.join = true;
// map-side join
SET hive.auto.convert.join.noconditionaltask = true;


-- 性能优化2；大表对大表关联，如果数据倾斜，可以开启skew join优化
// todo 有数据倾斜时开启负载均衡，默认false
set hive.optimize.skewjoin=true;


