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

        ,t1.ordered_store_num -- `下单店铺数`
        ,t1.delivered_goods_amt -- `送达金额`
        ,t1.final_refund_amt_order_time -- `售后赔付金额`
        ,t2.final_refund_amt_order_time_quality -- `品质问题售后赔付金额`
        ,t1.after_sale_num_order_time -- `售后数量`
        ,t3.after_sale_ticket_num -- `售后单数量`
        ,1 AS dummy
    FROM datawarehouse_max.dws_pub_mall_merchant_category_level4_base_daily_asc t1
    LEFT JOIN (
        SELECT
            t1.dt
            ,t1.merchant_id
            ,t1.category_level4_id

            ,SUM(t1.final_refund_amt_order_time_quality) AS final_refund_amt_order_time_quality
        FROM datawarehouse_max.dws_pub_mall_sku_base_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset} -30, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.mall_id = 871
        AND t1.category_level1_name="水果"
        GROUP BY t1.dt
            ,t1.merchant_id
            ,t1.category_level4_id
    ) t2
        ON t2.dt = t1.dt
        AND t2.merchant_id = t1.merchant_id
        AND t2.category_level4_id = t1.category_level4_id

    LEFT JOIN (
        SELECT
            DATE(t1.order_create_time) AS dt
            ,t1.merchant_id
            ,t1.back_category_id AS category_level4_id
            ,COUNT(DISTINCT t1.after_sale_order_id) AS after_sale_ticket_num
        FROM datawarehouse_max.dwt_order_after_sale_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset} -40, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")

            AND DATE(t1.order_create_time) BETWEEN DATEADD(${date_param}, ${start_offset} -30, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")
            AND t1.mall_id = 871
            AND t1.status != "CANCEL"
            AND INSTR(t1.category_level4_name, "水果") > 0
        GROUP BY DATE(t1.order_create_time)
            ,t1.merchant_id
            ,t1.back_category_id
    ) t3
        ON t3.dt = t1.dt
        AND t3.merchant_id = t1.merchant_id
        AND t3.category_level4_id = t1.category_level4_id
    WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset} -30, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.mall_id = 871
        AND t1.category_level1_name="水果"
        AND t1.ordered_goods_num > 0
)




SELECT
	t1.dt AS `日期`
	,t1.merchant_id AS `商家id`
    ,t2.merchant_name AS `商家名称`
    ,t2.category_level1_id AS `一级类目id`
    ,t2.category_level1_name AS `一级类目名称`
	,t1.category_level4_id AS `四级类目id` 
    ,t2.category_level4_name AS `四级类目名称`

	,t1.ordered_store_num AS `下单店铺数`
	,t1.delivered_goods_amt AS `送达金额`
	,t1.final_refund_amt_order_time AS `售后赔付金额`
	,t1.final_refund_amt_order_time_quality AS `品质问题售后赔付金额`
	,t1.after_sale_num_order_time AS `售后数量`
	,t1.after_sale_ticket_num AS `售后单数量`

	,t1.ordered_store_num_m29dtcd AS `近30天下单店铺数`
	,t1.delivered_goods_amt_m29dtcd AS `近30天送达金额`
	,t1.final_refund_amt_order_time_m29dtcd AS `近30天售后赔付金额`
	,t1.final_refund_amt_order_time_quality_m29dtcd AS `近30天品质问题售后赔付金额`
	,t1.after_sale_num_m29dtcd AS `近30天售后数量`
	,t1.after_sale_ticket_num_m29dtcd AS `近30天售后单数量`

	,t1.ordered_store_num_m6dtcd AS `近7天下单店铺数`
	,t1.delivered_goods_amt_m6dtcd AS `近7天送达金额`
	,t1.final_refund_amt_order_time_m6dtcd AS `近7天售后赔付金额`
	,t1.final_refund_amt_order_time_quality_m6dtcd AS `近7天品质问题售后赔付金额`
	,t1.after_sale_num_m6dtcd AS `近7天售后数量`
	,t1.after_sale_ticket_num_m6dtcd AS `近7天售后单数量`
    ,t1.ordered_days_m29dtcd AS `近30天下单天数`
    ,t1.ordered_days_m6dtcd AS `近7天下单天数`
FROM(

    SELECT
        t1.dt -- `日期`
        ,t2.merchant_id -- `商家id`
        ,t2.category_level1_id -- `一级类目id`
        ,t2.category_level4_id -- `四级类目id` 

        ,MAX(IF(t1.dt=t2.dt, t2.ordered_store_num, 0)) AS ordered_store_num -- `下单店铺数`
        ,MAX(IF(t1.dt=t2.dt, t2.delivered_goods_amt, 0)) AS delivered_goods_amt -- `送达金额`
        ,MAX(IF(t1.dt=t2.dt, t2.final_refund_amt_order_time, 0)) AS final_refund_amt_order_time -- `售后赔付金额`
        ,MAX(IF(t1.dt=t2.dt, t2.final_refund_amt_order_time_quality, 0)) AS final_refund_amt_order_time_quality -- `品质问题售后赔付金额`
        ,MAX(IF(t1.dt=t2.dt, t2.after_sale_num_order_time, 0)) AS after_sale_num_order_time -- `售后数量`
        ,MAX(IF(t1.dt=t2.dt, t2.after_sale_ticket_num, 0)) AS after_sale_ticket_num -- `售后单数量`

        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -29, "dd") AND t1.dt, t2.ordered_store_num, 0)) AS ordered_store_num_m29dtcd -- `近30天下单店铺数`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -29, "dd") AND t1.dt, t2.delivered_goods_amt, 0)) AS delivered_goods_amt_m29dtcd -- `近30天送达金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -29, "dd") AND t1.dt, t2.final_refund_amt_order_time, 0)) AS final_refund_amt_order_time_m29dtcd -- `近30天售后赔付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -29, "dd") AND t1.dt, t2.final_refund_amt_order_time_quality, 0)) AS final_refund_amt_order_time_quality_m29dtcd -- `近30天品质问题售后赔付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -29, "dd") AND t1.dt, t2.after_sale_num_order_time, 0)) AS after_sale_num_m29dtcd -- `近30天售后数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -29, "dd") AND t1.dt, t2.after_sale_ticket_num, 0)) AS after_sale_ticket_num_m29dtcd -- `近30天售后单数量`

        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt, t2.ordered_store_num, 0)) AS ordered_store_num_m6dtcd -- `近7天下单店铺数`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt, t2.delivered_goods_amt, 0)) AS delivered_goods_amt_m6dtcd -- `近7天送达金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt, t2.final_refund_amt_order_time, 0)) AS final_refund_amt_order_time_m6dtcd -- `近7天售后赔付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt, t2.final_refund_amt_order_time_quality, 0)) AS final_refund_amt_order_time_quality_m6dtcd -- `近7天品质问题售后赔付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt, t2.after_sale_num_order_time, 0)) AS after_sale_num_m6dtcd -- `近7天售后数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt, t2.after_sale_ticket_num, 0)) AS after_sale_ticket_num_m6dtcd -- `近7天售后单数量`

        ,COUNT(DISTINCT IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -29, "dd") AND t1.dt AND t2.ordered_store_num>0, t2.dt, NULL)) AS ordered_days_m29dtcd -- `近30天下单天数`
        ,COUNT(DISTINCT IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt AND t2.ordered_store_num>0, t2.dt, NULL)) AS ordered_days_m6dtcd -- `近7天下单天数`
    FROM dt_range t1
    LEFT JOIN temp t2
        ON t2.dummy = t1.dummy
        AND t2.dt BETWEEN DATEADD(DATE(t1.dt), -30, "dd") AND t1.dt

    GROUP BY t1.dt
        ,t2.merchant_id
        ,t2.category_level1_id -- `一级类目id`
        ,t2.category_level4_id
) t1

JOIN (
    SELECT
        t1.merchant_id
        ,t1.merchant_name
        ,t1.category_level1_id
        ,t1.category_level1_name
        ,t1.category_level4_id
        ,t1.category_level4_name
    FROM datawarehouse_max.dim_goods_daily_full  t1
    WHERE t1.dt = MAX_PT("datawarehouse_max.dim_goods_daily_full")
        AND t1.mall_id = 871
        AND INSTR(t1.category_level1_name, "水果") > 0
    GROUP BY t1.merchant_id
        ,t1.merchant_name
        ,t1.category_level1_id
        ,t1.category_level1_name
        ,t1.category_level4_id
        ,t1.category_level4_name
) t2
    ON t2.merchant_id = t1.merchant_id
    AND t2.category_level4_id = t1.category_level4_id
    AND t2.category_level1_id = t1.category_level1_id

WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
;