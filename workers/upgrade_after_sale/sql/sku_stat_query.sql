WITH dt_range AS(
    SELECT
        dummy
        ,DATEADD(DATE(${date_param}), gap, "dd") AS dt
    FROM VALUES (1) AS t(dummy)
    LATERAL VIEW EXPLODE(SEQUENCE(${end_offset}, ${start_offset}, -1)) t1 AS gap
)




,temp AS(

    SELECT
        t1.dt -- `日期`
        ,t1.merchant_id -- `商家id`
        ,t1.category_level1_id -- `一级类目id`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_id -- `四级类目id`
        ,t1.category_level4_name -- `四级类目名称`
        ,t1.sku_id -- `商品id`
        ,t1.operation_type -- `运营标签`
        ,t1.filter_label -- `前端标签`
        ,t1.ordered_store_num -- `下单店铺数`
        ,t1.delivered_goods_amt -- `送达金额`
        ,t2.payment_amt -- `实付金额`
        ,t2.order_item_ticket_num -- `明细订单数量`
        ,t1.final_refund_amt_order_time -- `售后赔付金额`
        ,t1.final_refund_amt_order_time_quality -- `品质问题售后赔付金额`
        ,t1.after_sale_num_order_time -- `售后数量`
        ,t3.after_sale_ticket_num -- `售后单数量`
        ,t3.as_order_item_ticket_num -- `售后明细单数量`
        ,1 AS dummy
    FROM datawarehouse_max.dws_pub_mall_sku_base_daily_asc  t1
    LEFT JOIN (
        SELECT
            t1.dt
            ,t1.sku_id
            ,SUM(t1.payment_amt) AS payment_amt
            ,COUNT(DISTINCT t1.order_item_id) AS order_item_ticket_num
        FROM datawarehouse_max.dwt_order_order_item_daily_asc t1

        WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset} -8, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")
            AND t1.mall_id = 871
            AND t1.status != "CANCEL"
        GROUP BY t1.dt
            ,t1.sku_id

    ) t2
        ON t2.dt = t1.dt
        AND t2.sku_id = t1.sku_id
    LEFT JOIN (
        SELECT
            DATE(t1.order_create_time) AS dt
            ,t1.sku_id
            ,COUNT(DISTINCT t1.after_sale_order_id) AS after_sale_ticket_num
            ,COUNT(DISTINCT t1.order_item_id) AS as_order_item_ticket_num
        FROM datawarehouse_max.dwt_order_after_sale_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset} -8, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")

            AND DATE(t1.order_create_time) BETWEEN DATEADD(${date_param}, ${start_offset} -15, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")
            AND t1.mall_id = 871
            AND t1.status != "CANCEL"

        GROUP BY DATE(t1.order_create_time)
            ,t1.sku_id
    ) t3
        ON t3.dt = t1.dt
        AND t3.sku_id = t1.sku_id

    WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset} -8, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.mall_id = 871
        AND t1.category_level1_name="水果"
        AND t1.ordered_goods_num > 0
)



SELECT
	t1.dt AS `日期`
	,t1.sku_id AS `商品id`
    ,t2.merchant_id AS `商家id`
    ,t2.merchant_name AS `商家名称`
    ,t2.category_level1_id AS `一级类目id`
    ,t2.category_level1_name AS `一级类目名称`
    ,t2.category_level4_id AS `四级类目id`
    ,t2.category_level4_name AS `四级类目名称`

	,IF(LENGTH(t1.operation_type)>1, t1.operation_type, NULL) AS `运营标签`

	,t1.ordered_store_num AS `下单店铺数`
	,t1.delivered_goods_amt AS `送达金额`
	,t1.payment_amt AS `实付金额`
	,t1.order_item_ticket_num AS `明细订单数量`
	,t1.final_refund_amt_order_time AS `售后赔付金额`
	,t1.final_refund_amt_order_time_quality AS `品质问题售后赔付金额`
	,t1.after_sale_num_order_time AS `售后数量`
	,t1.after_sale_ticket_num AS `售后单数量`

	,t1.ordered_store_num_m6dtcd AS `近7天下单店铺数`
	,t1.delivered_goods_amt_m6dtcd AS `近7天送达金额`
	,t1.payment_amt_m6dtcd AS `近7天实付金额`
	,t1.final_refund_amt_order_time_m6dtcd AS `近7天售后赔付金额`
	,t1.final_refund_amt_order_time_quality_m6dtcd AS `近7天品质问题售后赔付金额`
	,t1.after_sale_ticket_num_m6dtcd AS `近7天售后单数量`
	,t1.order_item_ticket_num_m6dtcd AS `近7天明细订单数量`
	,t1.after_sale_ticket_num_m7dtm1d AS `前7天售后单数量`
	,t1.order_item_ticket_num_m7dtm1d AS `前7天明细订单数量`
    ,t1.as_order_item_ticket_num_m7dtm1d AS `前7天售后明细单数量`
    ,t1.order_days_m7dtm1d AS `前7天交易天数`

	,t1.after_sale_num_order_time_m8dtm5d AS `m8到m5售后数量`
	,t1.after_sale_num_order_time_m4dtm1d AS `m4到m1售后数量`
	,t1.after_sale_ticket_num_m8dtm5d AS `m8到m5售后单数量`
	,t1.after_sale_ticket_num_m4dtm1d AS `m4到m1售后单数量`

	,t1.after_sale_days_m8dtm5d AS `m8到m5售后天数`
	,t1.after_sale_days_m4dtm1d AS `m4到m1售后天数`
	,t1.ordered_days_m8dtm5d AS `m8到m5下单天数`
	,t1.ordered_days_m4dtm1d AS `m4到m1下单天数`

	,t1.ordered_days_m6dtcd AS `近7日下单天数`
	,t1.after_sale_days_m6dtcd AS `近7日有售后天数`
FROM(
    SELECT
        t1.dt -- `日期`
        ,t2.sku_id -- `商品id`

        ,MAX(IF(t1.dt=t2.dt, t2.operation_type, 0)) AS operation_type -- `运营标签`

        ,MAX(IF(t1.dt=t2.dt, t2.ordered_store_num, 0)) AS ordered_store_num -- `下单店铺数`
        ,MAX(IF(t1.dt=t2.dt, t2.delivered_goods_amt, 0)) AS delivered_goods_amt -- `送达金额`
        ,MAX(IF(t1.dt=t2.dt, t2.payment_amt, 0)) AS payment_amt -- `实付金额`
        ,MAX(IF(t1.dt=t2.dt, t2.order_item_ticket_num, 0)) AS order_item_ticket_num -- `明细订单数量`
        ,MAX(IF(t1.dt=t2.dt, t2.final_refund_amt_order_time, 0)) AS final_refund_amt_order_time -- `售后赔付金额`
        ,MAX(IF(t1.dt=t2.dt, t2.final_refund_amt_order_time_quality, 0)) AS final_refund_amt_order_time_quality -- `品质问题售后赔付金额`
        ,MAX(IF(t1.dt=t2.dt, t2.after_sale_num_order_time, 0)) AS after_sale_num_order_time -- `售后数量`
        ,MAX(IF(t1.dt=t2.dt, t2.after_sale_ticket_num, 0)) AS after_sale_ticket_num -- `售后单数量`

        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt, t2.ordered_store_num, 0)) AS ordered_store_num_m6dtcd -- `近7天下单店铺数`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt, t2.delivered_goods_amt, 0)) AS delivered_goods_amt_m6dtcd -- `近7天送达金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt, t2.payment_amt, 0)) AS payment_amt_m6dtcd -- `近7天实付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt, t2.final_refund_amt_order_time, 0)) AS final_refund_amt_order_time_m6dtcd -- `近7天售后赔付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt, t2.final_refund_amt_order_time_quality, 0)) AS final_refund_amt_order_time_quality_m6dtcd -- `近7天品质问题售后赔付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt, t2.after_sale_ticket_num, 0)) AS after_sale_ticket_num_m6dtcd -- `近7天售后单数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt, t2.order_item_ticket_num, 0)) AS order_item_ticket_num_m6dtcd -- `近7天明细订单数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -7, "dd") AND DATEADD(DATE(t1.dt),  -1, "dd"), t2.after_sale_ticket_num, 0)) AS after_sale_ticket_num_m7dtm1d -- `前7天售后单数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -7, "dd") AND DATEADD(DATE(t1.dt),  -1, "dd"), t2.order_item_ticket_num, 0)) AS order_item_ticket_num_m7dtm1d -- `前7天明细订单数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -7, "dd") AND DATEADD(DATE(t1.dt),  -1, "dd"), t2.as_order_item_ticket_num, 0)) AS as_order_item_ticket_num_m7dtm1d -- `前7天售后明细单数量`

        ,COUNT(DISTINCT IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -7, "dd") AND DATEADD(DATE(t1.dt),  -1, "dd") AND t2.ordered_store_num>0, t2.dt, NULL)) AS order_days_m7dtm1d -- `前7天交易天数`

        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -8, "dd") AND DATEADD(DATE(t1.dt),  -5, "dd"), t2.after_sale_num_order_time, 0)) AS after_sale_num_order_time_m8dtm5d -- `m8到m5售后数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -4, "dd") AND DATEADD(DATE(t1.dt),  -1, "dd"), t2.after_sale_num_order_time, 0)) AS after_sale_num_order_time_m4dtm1d -- `m4到m1售后数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -8, "dd") AND DATEADD(DATE(t1.dt),  -5, "dd"), t2.after_sale_ticket_num, 0)) AS after_sale_ticket_num_m8dtm5d -- `m8到m5售后单数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -4, "dd") AND DATEADD(DATE(t1.dt),  -1, "dd"), t2.after_sale_ticket_num, 0)) AS after_sale_ticket_num_m4dtm1d -- `m4到m1售后单数量`

        ,COUNT(DISTINCT IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -8, "dd") AND DATEADD(DATE(t1.dt),  -5, "dd") AND t2.after_sale_ticket_num>0, t2.dt, NULL)) AS after_sale_days_m8dtm5d -- `m8到m5售后天数`
        ,COUNT(DISTINCT IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -4, "dd") AND DATEADD(DATE(t1.dt),  -1, "dd") AND t2.after_sale_ticket_num>0, t2.dt, NULL)) AS after_sale_days_m4dtm1d -- `m4到m1售后天数`
        ,COUNT(DISTINCT IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -8, "dd") AND DATEADD(DATE(t1.dt),  -5, "dd") AND t2.ordered_store_num>0, t2.dt, NULL)) AS ordered_days_m8dtm5d -- `m8到m5下单天数`
        ,COUNT(DISTINCT IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -4, "dd") AND DATEADD(DATE(t1.dt),  -1, "dd") AND t2.ordered_store_num>0, t2.dt, NULL)) AS ordered_days_m4dtm1d -- `m4到m1下单天数`

        ,COUNT(DISTINCT IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt AND t2.ordered_store_num>0, t2.dt, NULL)) AS ordered_days_m6dtcd -- `近7日下单天数`
        ,COUNT(DISTINCT IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt AND t2.after_sale_ticket_num>0, t2.dt, NULL)) AS after_sale_days_m6dtcd -- `近7日有售后天数`
    FROM dt_range t1
    LEFT JOIN temp t2
    ON t2.dummy = t1.dummy
        AND t2.dt BETWEEN DATEADD(DATE(t1.dt), -8, "dd") AND t1.dt

    GROUP BY t1.dt
        ,t2.sku_id
) t1
JOIN datawarehouse_max.dim_goods_daily_full t2
    ON t2.dt = MAX_PT("datawarehouse_max.dim_goods_daily_full")
    AND t2.sku_id = t1.sku_id
    AND t2.mall_id = 871

WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
        AND DATEADD(${date_param}, ${end_offset}, "dd")
;