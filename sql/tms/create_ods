


-- CREATE DATABASE IF NOT EXISTS tms ;



-- 1运单表order_info、
DROP TABLE IF EXISTS dev_realtime_tms_zhengwei_zhou.ods_order_info;
CREATE EXTERNAL TABLE dev_realtime_tms_zhengwei_zhou.ods_order_info (
    `id`                   BIGINT COMMENT 'id',
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
    `distance`             DECIMAL(10, 2) COMMENT '距离，单位：公里',
    `create_time`          STRING COMMENT '创建时间',
    `update_time`          STRING COMMENT '更新时间',
    `is_deleted`           STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '运单表order_info'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/2207A/zhengwei_zhou/tms/ods/ods_order_info';

-- 2运单明细表order_cargo、
DROP TABLE IF EXISTS dev_realtime_tms_zhengwei_zhou.ods_order_cargo;
CREATE EXTERNAL TABLE dev_realtime_tms_zhengwei_zhou.ods_order_cargo (
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
    `is_deleted`    BIGINT COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '运单明细表order_cargo'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/2207A/zhengwei_zhou/tms/ods/ods_order_cargo';

-- 3字典表base_dic、
DROP TABLE IF EXISTS dev_realtime_tms_zhengwei_zhou.ods_base_dic;
CREATE EXTERNAL TABLE dev_realtime_tms_zhengwei_zhou.ods_base_dic (
    `id`          BIGINT COMMENT 'id',
    `parent_id`   BIGINT COMMENT '上级id',
    `name`        STRING COMMENT '名称',
    `dict_code`   STRING COMMENT '编码',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted`  BIGINT COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '字典表base_dic'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/2207A/zhengwei_zhou/tms/ods/ods_base_dic';

-- 4小区表base_complex、
DROP TABLE IF EXISTS dev_realtime_tms_zhengwei_zhou.ods_base_complex;
CREATE EXTERNAL TABLE dev_realtime_tms_zhengwei_zhou.ods_base_complex
(
    `id`            BIGINT,
    `complex_name`  STRING,
    `province_id`   BIGINT,
    `city_id`       BIGINT,
    `district_id`   BIGINT,
    `district_name` STRING,
    `create_time`   STRING,
    `update_time`   STRING,
    `is_deleted`    BIGINT
) COMMENT '小区表base_complex'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/2207A/zhengwei_zhou/tms/ods/ods_base_complex';

-- 5地区表base_region_info、
DROP TABLE IF EXISTS dev_realtime_tms_zhengwei_zhou.ods_base_region_info;
CREATE EXTERNAL TABLE dev_realtime_tms_zhengwei_zhou.ods_base_region_info
(
    `id`          BIGINT COMMENT 'id',
    `parent_id`   BIGINT COMMENT '上级id',
    `name`        STRING COMMENT '名称',
    `dict_code`   STRING COMMENT '编码',
    `short_name`  STRING COMMENT '简称',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted`  BIGINT COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '地区表base_region_info'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/2207A/zhengwei_zhou/tms/ods/ods_base_region_info';

-- 6机构表base_organ
DROP TABLE IF EXISTS dev_realtime_tms_zhengwei_zhou.ods_base_organ;
CREATE EXTERNAL TABLE dev_realtime_tms_zhengwei_zhou.ods_base_organ
(
    `id`            BIGINT,
    `org_name`      STRING COMMENT '机构名称',
    `org_level`     BIGINT COMMENT '行政级别',
    `region_id`     BIGINT COMMENT '区域id，1级机构为city ,2级机构为district',
    `org_parent_id` BIGINT COMMENT '上级机构id',
    `points`        STRING COMMENT '多边形经纬度坐标集合',
    `create_time`   STRING COMMENT '创建时间',
    `update_time`   STRING COMMENT '删除标记（0:不可用 1:可用）',
    `is_deleted`    BIGINT COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '机构表base_organ'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/2207A/zhengwei_zhou/tms/ods/ods_base_organ';



/*
2）、开发工具中，书写Hive DML语句，将物流同步业务数据加载到ODS层各个表中，并且查看各个表的数据，相关操作完成截图，否则0分；（6分，每张表加载成功1分）
*/
-- 1运单表order_info、
LOAD DATA INPATH '/2207A/zhengwei_zhou/tms/order_info/2025-03-25' INTO TABLE dev_realtime_tms_zhengwei_zhou.ods_order_info PARTITION (dt = '2025-03-25') ;

-- 2运单明细表order_cargo、
LOAD DATA INPATH '/2207A/zhengwei_zhou/tms/order_cargo/2025-03-25' INTO TABLE dev_realtime_tms_zhengwei_zhou.ods_order_cargo PARTITION (dt = '2025-03-25') ;

-- 3字典表base_dic、
LOAD DATA INPATH '/2207A/zhengwei_zhou/tms/base_dic/2025-03-25' INTO TABLE dev_realtime_tms_zhengwei_zhou.ods_base_dic PARTITION (dt = '2025-03-25') ;

-- 4小区表base_complex、
LOAD DATA INPATH '/2207A/zhengwei_zhou/tms/base_complex/2025-03-25' INTO TABLE dev_realtime_tms_zhengwei_zhou.ods_base_complex PARTITION (dt = '2025-03-25') ;

-- 5地区表base_region_info、
LOAD DATA INPATH '/2207A/zhengwei_zhou/tms/base_region_info/2025-03-25' INTO TABLE dev_realtime_tms_zhengwei_zhou.ods_base_region_info PARTITION (dt = '2025-03-25') ;

-- 6机构表base_organ
LOAD DATA INPATH '/2207A/zhengwei_zhou/tms/base_organ/2025-03-25' INTO TABLE dev_realtime_tms_zhengwei_zhou.ods_base_organ PARTITION (dt = '2025-03-25') ;






























