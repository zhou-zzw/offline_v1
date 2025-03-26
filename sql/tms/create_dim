

use dev_realtime_tms_zhengwei_zhou;




DROP TABLE IF EXISTS dev_realtime_tms_zhengwei_zhou.dim_base_complex;
CREATE EXTERNAL TABLE dev_realtime_tms_zhengwei_zhou.dim_base_complex
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
    STORED AS ORC
    LOCATION '/2207A/zhengwei_zhou/tms/dim/dim_base_complex';

-- 地区表base_region_info、
DROP TABLE IF EXISTS dev_realtime_tms_zhengwei_zhou.dim_base_region_info;
CREATE EXTERNAL TABLE dev_realtime_tms_zhengwei_zhou.dim_base_region_info
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
    STORED AS ORC
    LOCATION '/2207A/zhengwei_zhou/tms/dim/dim_base_region_info';


-- 机构表base_organ
DROP TABLE IF EXISTS dev_realtime_tms_zhengwei_zhou.dim_base_organ;
CREATE EXTERNAL TABLE dev_realtime_tms_zhengwei_zhou.dim_base_organ
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
    LOCATION '/2207A/zhengwei_zhou/tms/dim/dim_base_organ';


/*
2）、编写Hive DML语句，从ODS层原始数据表查询数据，经过ETL转换处理，将数据加载到DIM层维度表，并查看表数据。（6分，每张表加载成功2分）
*/

-- 小区表base_complex、
INSERT INTO dev_realtime_tms_zhengwei_zhou.dim_base_complex PARTITION (dt = '2025-03-25')
SELECT id,
       complex_name,
       province_id,
       city_id,
       district_id,
       district_name,
       create_time,
       update_time,
       is_deleted
FROM dev_realtime_tms_zhengwei_zhou.ods_base_complex
WHERE dt = '2025-03-25'
;

-- 地区表base_region_info、
INSERT INTO dev_realtime_tms_zhengwei_zhou.dim_base_region_info PARTITION (dt = '2025-03-25')
SELECT id,
       parent_id,
       name,
       dict_code,
       short_name,
       create_time,
       update_time,
       is_deleted
FROM dev_realtime_tms_zhengwei_zhou.ods_base_region_info
WHERE dt = '2025-03-25'
;


-- 机构表base_organ
INSERT INTO dev_realtime_tms_zhengwei_zhou.dim_base_organ PARTITION (dt = '2025-03-25')
SELECT id,
       org_name,
       org_level,
       region_id,
       org_parent_id,
       points,
       create_time,
       update_time,
       is_deleted
FROM dev_realtime_tms_zhengwei_zhou.ods_base_organ
WHERE dt = '2025-03-25'
;
