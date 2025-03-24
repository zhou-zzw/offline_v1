

use dev_realtime_v1_zhengwei_zhou;


-- 3.3.1 创建订单表
drop table if exists ods_order_info;
create table ods_order_info (
    `id` string COMMENT '订单编号',
    `total_amount` decimal(10,2) COMMENT '订单金额',
    `order_status` string COMMENT '订单状态',
    `user_id` string COMMENT '用户id' ,
    `payment_way` string COMMENT '支付方式',
    `out_trade_no` string COMMENT '支付流水号',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间'
) COMMENT '订单表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ods/ods_order_info/'
tblproperties ("parquet.compression"="snappy")
;


load data inpath '/2207A/zhengwei_zhou/order_info/2025-03-23'
overwrite into table ods_order_info partition (dt='2025-03-23');


load data inpath '/2207A/zhengwei_zhou/order_detail/2025-03-23'
overwrite into table ods_order_detail partition (dt='2025-03-23');


load data inpath '/2207A/zhengwei_zhou/sku_info/2025-03-23'
overwrite into table ods_sku_info partition (dt='2025-03-23');

load data inpath '/2207A/zhengwei_zhou/user_info/2025-03-23'
overwrite into table ods_user_info partition (dt='2025-03-23');


load data inpath '/2207A/zhengwei_zhou/base_category1/2025-03-23'
overwrite into table ods_base_category1 partition (dt='2025-03-23');

load data inpath '/2207A/zhengwei_zhou/base_category2/2025-03-23'
overwrite into table ods_base_category2 partition (dt='2025-03-23');


load data inpath '/2207A/zhengwei_zhou/base_category3/2025-03-23'
overwrite into table ods_base_category3 partition (dt='2025-03-23');


load data inpath '/2207A/zhengwei_zhou/payment_info/2025-03-23'
overwrite into table ods_payment_info partition (dt='2025-03-23');



-- 3.3.2 创建订单详情表
drop table if exists ods_order_detail;
create table ods_order_detail(
    `id` string COMMENT '订单编号',
    `order_id` string  COMMENT '订单号',
    `user_id` string COMMENT '用户id' ,
    `sku_id` string COMMENT '商品id',
    `sku_name` string COMMENT '商品名称',
    `order_price` string COMMENT '下单价格',
    `sku_num` string COMMENT '商品数量',
    `create_time` string COMMENT '创建时间'
) COMMENT '订单明细表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ods/ods_order_detail/'
tblproperties ("parquet.compression"="snappy")
;




-- 3.3.3 创建商品表
drop table if exists ods_sku_info;
create table ods_sku_info(
    `id` string COMMENT 'skuId',
    `spu_id` string   COMMENT 'spuid',
    `price` decimal(10,2) COMMENT '价格' ,
    `sku_name` string COMMENT '商品名称',
    `sku_desc` string COMMENT '商品描述',
    `weight` string COMMENT '重量',
    `tm_id` string COMMENT '品牌id',
    `category3_id` string COMMENT '品类id',
    `create_time` string COMMENT '创建时间'
) COMMENT '商品表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ods/ods_sku_info/'
tblproperties ("parquet.compression"="snappy")
;



-- 3.3.4 创建用户表

drop table if exists ods_user_info;
create table ods_user_info(
    `id` string COMMENT '用户id',
    `name`  string COMMENT '姓名',
    `birthday` string COMMENT '生日' ,
    `gender` string COMMENT '性别',
    `email` string COMMENT '邮箱',
    `user_level` string COMMENT '用户等级',
    `create_time` string COMMENT '创建时间'
) COMMENT '用户信息'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ods/ods_user_info/'
tblproperties ("parquet.compression"="snappy")
;




-- 3.3.5 创建商品一级分类表

drop table if exists ods_base_category1;
create table ods_base_category1(
    `id` string COMMENT 'id',
    `name`  string COMMENT '名称'
) COMMENT '商品一级分类'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ods/ods_base_category1/'
tblproperties ("parquet.compression"="snappy")
;




-- 3.3.6 创建商品二级分类表

drop table if exists ods_base_category2;
create external table ods_base_category2(
    `id` string COMMENT ' id',
    `name`  string COMMENT '名称',
    category1_id string COMMENT '一级品类id'
) COMMENT '商品二级分类'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ods/ods_base_category2/'
tblproperties ("parquet.compression"="snappy")
;



-- 3.3.7 创建商品三级分类表

drop table if exists ods_base_category3;
create table ods_base_category3(
    `id` string COMMENT ' id',
    `name`  string COMMENT '名称',
    category2_id string COMMENT '二级品类id'
) COMMENT '商品三级分类'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ods/ods_base_category3/'
tblproperties ("parquet.compression"="snappy")
;





-- 3.3.8 创建支付流水表

drop table if exists `ods_payment_info`;
create table  `ods_payment_info`(
    `id`   bigint COMMENT '编号',
    `out_trade_no`    string COMMENT '对外业务编号',
    `order_id`        string COMMENT '订单编号',
    `user_id`         string COMMENT '用户编号',
    `alipay_trade_no` string COMMENT '支付宝交易流水编号',
    `total_amount`    decimal(16,2) COMMENT '支付金额',
    `subject`         string COMMENT '交易内容',
    `payment_type` string COMMENT '支付类型',
    `payment_time`   string COMMENT '支付时间'
   )  COMMENT '支付流水表'
PARTITIONED BY ( `dt` string)
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ods/ods_payment_info/'
tblproperties ("parquet.compression"="snappy")
;





drop table if exists dwd_comment_log;
create external table dwd_comment_log
(
    mid_id       string,
    user_id      string,
    version_code string,
    version_name string,
    lang         string,
    source       string,
    os           string,
    area         string,
    model        string,
    brand        string,
    sdk_version  string,
    gmail        string,
    height_width string,
    app_time     string,
    network      string,
    lng          string,
    lat          string,
    comment_id   int,
    userid       int,
    p_comment_id int,
    content      string,
    addtime      string,
    other_id     int,
    praise_count int,
    reply_count  int,
    server_time  string
) COMMENT ''
PARTITIONED BY ( `dt` string)
    row format delimited fields terminated by '\t'
stored as  parquet
location '/user/hive/warehouse/dev_realtime_yinshi.db/dwd/dwd_comment_log/'
tblproperties ("parquet.compression"="snappy")
;


load data local inpath ''







create table ods_comment_info(
    id  string,
    user_id string,
    sku_id string,
    spu_id string,
    order_id string,
    appraise string,
    comment_txt string,
    create_time string,
    operate_time string
)partitioned by (dt string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ods/ods_comment_info'
tblproperties ("parquet.compression"="snappy");

load data inpath '/2207A/zhengwei_zhou/comment_info/2025-03-23'
overwrite into table ods_comment_info partition (dt='2025-03-23');




create table dwd_comment_info(
    id  string,
    user_id string,
    sku_id string,
    spu_id string,
    order_id string,
    appraise string,
    comment_txt string,
    create_time string,
    operate_time string
)partitioned by (dt string)
row format delimited fields terminated by '\t'
stored as parquet
location '/warehouse/gmall/dwd/dwd_comment_info'
tblproperties ("parquet.compression"="snappy");

insert into  dwd_comment_info
select * from ods_comment_info
where dt='2025-03-23' and id is not null ;



-- 3.4.1 创建订单表

drop table if exists dwd_order_info;
create external table dwd_order_info (
    `id` string COMMENT '',
    `total_amount` decimal(10,2) COMMENT '',
    `order_status` string COMMENT ' 1 2  3  4  5',
    `user_id` string COMMENT 'id' ,
    `payment_way` string COMMENT '',
    `out_trade_no` string COMMENT '',
    `create_time` string COMMENT '',
    `operate_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/gmall/dwd/dwd_order_info/'
tblproperties ("parquet.compression"="snappy")
;



-- 3.4.2 创建订单详情表

drop table if exists dwd_order_detail;
create external table dwd_order_detail(
    `id` string COMMENT '',
    `order_id` decimal(10,2) COMMENT '',
    `user_id` string COMMENT 'id' ,
    `sku_id` string COMMENT 'id',
    `sku_name` string COMMENT '',
    `order_price` string COMMENT '',
    `sku_num` string COMMENT '',
    `create_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/gmall/dwd/dwd_order_detail/'
tblproperties ("parquet.compression"="snappy")
;


-- 3.4.3 创建用户表

drop table if exists dwd_user_info;
create external table dwd_user_info(
    `id` string COMMENT 'id',
    `name`  string COMMENT '',
    `birthday` string COMMENT '' ,
    `gender` string COMMENT '',
    `email` string COMMENT '',
    `user_level` string COMMENT '',
    `create_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/gmall/dwd/dwd_user_info/'
tblproperties ("parquet.compression"="snappy")
;




-- 3.4.4 创建支付流水表

drop table if exists `dwd_payment_info`;
create external  table  `dwd_payment_info`(
    `id`   bigint COMMENT '',
    `out_trade_no`   string COMMENT '',
    `order_id`        string COMMENT '',
    `user_id`         string COMMENT '',
    `alipay_trade_no` string COMMENT '',
    `total_amount`    decimal(16,2) COMMENT '',
    `subject`         string COMMENT '',
    `payment_type` string COMMENT '',
    `payment_time`   string COMMENT ''
   )  COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/gmall/dwd/dwd_payment_info/'
tblproperties ("parquet.compression"="snappy")
;





-- 3.4.5 创建商品表（增加分类）

drop table if exists dwd_sku_info;
create external table dwd_sku_info(
    `id` string COMMENT 'skuId',
    `spu_id` string COMMENT 'spuid',
    `price` decimal(10,2) COMMENT '' ,
    `sku_name` string COMMENT '',
    `sku_desc` string COMMENT '',
    `weight` string COMMENT '',
    `tm_id` string COMMENT 'id',
    `category3_id` string COMMENT '1id',
    `category2_id` string COMMENT '2id',
    `category1_id` string COMMENT '3id',
    `category3_name` string COMMENT '3',
    `category2_name` string COMMENT '2',
    `category1_name` string COMMENT '1',
    `create_time` string COMMENT ''
) COMMENT ''
PARTITIONED BY ( `dt` string)
stored as  parquet
location '/warehouse/gmall/dwd/dwd_sku_info/'
tblproperties ("parquet.compression"="snappy")
;


insert  overwrite table   dwd_order_info partition(dt)
select  * from ods_order_info
where dt='2025-03-23'  and id is not null;


insert  overwrite table   dwd_order_detail partition(dt)
select  * from ods_order_detail
where dt='2025-03-23'   and id is not null;


insert  overwrite table   dwd_user_info partition(dt)
select  * from ods_user_info
where dt='2025-03-23'   and id is not null;


insert  overwrite table   dwd_payment_info partition(dt)
select  * from ods_payment_info
where dt='2025-03-23'  and id is not null;


insert  overwrite table   dwd_sku_info partition(dt)
select
    sku.id,
    sku.spu_id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.tm_id,
    sku.category3_id,
    c2.id category2_id ,
    c1.id category1_id,
    c3.name category3_name,
    c2.name category2_name,
    c1.name category1_name,
    sku.create_time,
    sku.dt
from
    ods_sku_info sku
join ods_base_category3 c3 on sku.category3_id=c3.id
    join ods_base_category2 c2 on c3.category2_id=c2.id
    join ods_base_category1 c1 on c2.category1_id=c1.id
where sku.dt='2025-03-23'  and c2.dt='2025-03-23'
and  c3.dt='2025-03-23' and  c1.dt='2025-03-23'
and sku.id is not null;







