#!/bin/bash
DATAX_HOME=/opt/soft/datax
# 如果传入日期则do_date等于传入的日期，否则等于前一天日期
if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d "-1 day" +%F`
fi

#处理目标路径，此处的处理逻辑是，如果目标路径不存在，则创建；若存在，则清空，目的是保证同步任务可重复执行
handle_targetdir(){
  hadoop fs -test -e $1
  if [[ $? -eq 1 ]]; then
    echo "路径$1不存在，正在创建......"
    hadoop fs -mkdir -p $1
  else
    echo "路径$1已经存在"
    fs_count=$(hadoop fs -count $1)
    content_size=$(echo $fs_count | awk '{print $3}')
    if [[ $content_size -eq 0 ]]; then
      echo "路径$1为空"
    else
      echo "路径$1不为空，正在清空......"
      hadoop fs -rm -r -f $1/*
    fi
  fi
}

#数据同步
import_data() {
  datax_config=$1
  target_dir=$2
  handle_targetdir $target_dir
  python $DATAX_HOME/bin/datax.py -p" -Dtargetdir=$target_dir" $datax_config
}

case $1 in
"order_info")
  import_data /opt/module/datax/job/import/gmall.order_info.json /user/hive/warehouse/gmall1.db/order_info/$do_date
  ;;
"base_category1")
  import_data /opt/module/datax/job/import/gmall.base_category1.json /user/hive/warehouse/gmall1.db/base_category1/$do_date
  ;;
"base_category2")
  import_data /opt/module/datax/job/import/gmall.base_category2.json /user/hive/warehouse/gmall1.db/base_category2/$do_date
  ;;
"base_category3")
  import_data /opt/module/datax/job/import/gmall.base_category3.json /user/hive/warehouse/gmall1.db/base_category3/$do_date
  ;;
"order_detail")
  import_data /opt/module/datax/job/import/gmall.order_detail.json /user/hive/warehouse/gmall1.db/order_detail/$do_date
  ;;
"sku_info")
  import_data /opt/module/datax/job/import/gmall.sku_info.json /user/hive/warehouse/gmall1.db/sku_info/$do_date
  ;;
"user_info")
  import_data /opt/module/datax/job/import/gmall.user_info.json /user/hive/warehouse/gmall1.db/user_info/$do_date
  ;;
"payment_info")
  import_data /opt/module/datax/job/import/gmall.payment_info.json /user/hive/warehouse/gmall1.db/payment_info/$do_date
  ;;
"base_province")
  import_data /opt/module/datax/job/import/gmall.base_province.json /user/hive/warehouse/gmall1.db/base_province/$do_date
  ;;
"base_region")
  import_data /opt/module/datax/job/import/gmall.base_region.json /user/hive/warehouse/gmall1.db/base_region/$do_date
  ;;
"base_trademark")
  import_data /opt/module/datax/job/import/gmall.base_trademark.json /user/hive/warehouse/gmall1.db/base_trademark/$do_date
  ;;
"activity_info")
  import_data /opt/module/datax/job/import/gmall.activity_info.json /user/hive/warehouse/gmall1.db/activity_info/$do_date
  ;;
"activity_order")
  import_data /opt/module/datax/job/import/gmall.activity_order.json /user/hive/warehouse/gmall1.db/activity_order/$do_date
  ;;
"cart_info")
  import_data /opt/module/datax/job/import/gmall.cart_info.json /user/hive/warehouse/gmall1.db/cart_info/$do_date
  ;;
"comment_info")
  import_data /opt/soft/datax/job/zhengwei_zhou/import/dev_realtime_v1_zhengwei_zhou.comment_info.json /2207A/zhengwei_zhou/comment_info/$do_date
  ;;
"coupon_info")
  import_data /opt/module/datax/job/import/gmall.coupon_info.json /user/hive/warehouse/gmall1.db/coupon_info/$do_date
  ;;
"coupon_use")
  import_data /opt/module/datax/job/import/gmall.coupon_use.json /user/hive/warehouse/gmall1.db/coupon_use/$do_date
  ;;
"favor_info")
  import_data /opt/module/datax/job/import/gmall.favor_info.json /user/hive/warehouse/gmall1.db/favor_info/$do_date
  ;;
"order_refund_info")
  import_data /opt/module/datax/job/import/gmall.order_refund_info.json /user/hive/warehouse/gmall1.db/order_refund_info/$do_date
  ;;
"order_status_log")
  import_data /opt/module/datax/job/import/gmall.order_status_log.json /user/hive/warehouse/gmall1.db/order_status_log/$do_date
  ;;
"spu_info")
  import_data /opt/module/datax/job/import/gmall.spu_info.json /user/hive/warehouse/gmall1.db/spu_info/$do_date
  ;;
"activity_rule")
  import_data /opt/module/datax/job/import/gmall.activity_rule.json /user/hive/warehouse/gmall1.dbactivity_rule/$do_date
  ;;
"base_dic")
  import_data /opt/module/datax/job/import/gmall.base_dic.json /user/hive/warehouse/gmall1.db/base_dic/$do_date
  ;;
"all")
  import_data /opt/soft/datax/job/zhengwei_zhou/import/dev_realtime_v1_zhengwei_zhou.order_info.json /2207A/zhengwei_zhou/order_info/$do_date
  import_data /opt/soft/datax/job/zhengwei_zhou/import/dev_realtime_v1_zhengwei_zhou.base_category1.json /2207A/zhengwei_zhou/base_category1/$do_date
  import_data /opt/soft/datax/job/zhengwei_zhou/import/dev_realtime_v1_zhengwei_zhou.base_category2.json /2207A/zhengwei_zhou/base_category2/$do_date
  import_data /opt/soft/datax/job/zhengwei_zhou/import/dev_realtime_v1_zhengwei_zhou.base_category3.json /2207A/zhengwei_zhou/base_category3/$do_date
  import_data /opt/soft/datax/job/zhengwei_zhou/import/dev_realtime_v1_zhengwei_zhou.order_detail.json /2207A/zhengwei_zhou/order_detail/$do_date
  import_data /opt/soft/datax/job/zhengwei_zhou/import/dev_realtime_v1_zhengwei_zhou.sku_info.json /2207A/zhengwei_zhou/sku_info/$do_date
  import_data /opt/soft/datax/job/zhengwei_zhou/import/dev_realtime_v1_zhengwei_zhou.user_info.json /2207A/zhengwei_zhou/user_info/$do_date
  import_data /opt/soft/datax/job/zhengwei_zhou/import/dev_realtime_v1_zhengwei_zhou.payment_info.json /2207A/zhengwei_zhou/payment_info/$do_date
  import_data /opt/soft/datax/job/zhengwei_zhou/import/dev_realtime_v1_zhengwei_zhou.comment_info.json /2207A/zhengwei_zhou/comment_info/$do_date
  ;;
esac


