SELECT
    t1.dt AS `日期`
    ,t1.customer_store_id AS `店铺id`
    ,t1.category_level1_id AS `一级类目id`
    ,t1.category_level1_name AS `一级类目名称`
    ,t1.ordered_goods_num AS `下单数量`
    ,t1.ordered_goods_amt AS `下单金额`
    ,t1.delivered_goods_num AS `送达数量`
    ,t1.delivered_goods_amt AS `送达金额`
    ,t2.payment_amt AS `实际金额`
    ,t1.final_refund_amt_order_time AS `售后赔付金额`
    ,t1.final_refund_amt_order_time_quality AS `品质问题售后赔付金额`
    ,t1.commission_amt AS `平台抽佣金额`
    ,t1.final_refund_amt AS `自然日售后赔付金额`

FROM datawarehouse_max.dws_store_mall_store_category_level1_base_daily_asc t1
LEFT JOIN (
    SELECT
        t1.dt
        ,t1.customer_store_id
        ,t2.category_level1_id
        ,SUM(t1.payment_amt) AS payment_amt
    FROM datawarehouse_max.dwt_order_order_item_daily_asc t1
    LEFT JOIN datawarehouse_max.dim_goods_daily_full t2
        ON t2.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t2.dt = t1.dt
        AND t2.sku_id = t1.sku_id
    WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.mall_id = 871
        AND t1.status != "CANCEL"
    GROUP BY t1.dt
        ,t1.customer_store_id
        ,t2.category_level1_id

) t2
    ON t2.dt = t1.dt
    AND t2.customer_store_id = t1.customer_store_id
    AND t2.category_level1_id = t1.category_level1_id
WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
        AND DATEADD(${date_param}, ${end_offset}, "dd")
    AND t1.mall_id = 871
    AND NVL(t1.ordered_goods_num,0) > 0
;