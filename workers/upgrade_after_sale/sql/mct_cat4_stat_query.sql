SELECT
    t1.dt AS `日期`
    ,t1.merchant_id AS `商家id`
    ,t1.category_level1_id AS `一级类目id`
    ,t1.category_level1_name AS `一级类目名称`
    ,t1.category_level4_id AS `四级类目id`
    ,t1.category_level4_name AS `四级类目名称`
    ,t1.ordered_store_num AS `下单店铺数`
    ,t1.delivered_goods_amt AS `送达金额`
    ,t1.final_refund_amt_order_time AS `售后赔付金额`
FROM datawarehouse_max.dws_pub_mall_merchant_category_level4_base_daily_asc t1
WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
        AND DATEADD(${date_param}, ${end_offset}, "dd")
    AND t1.mall_id = 871
    AND t1.category_level1_name="水果"
    AND t1.ordered_goods_num > 0
;