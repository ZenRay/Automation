WITH dt_range AS(
    SELECT
        dummy
        ,DATEADD(${date_param}, gap, "dd") AS dt
    FROM VALUES (1) AS t(dummy)
    LATERAL VIEW EXPLODE(SEQUENCE(0, ${start_offset}, -1)) t1 AS gap
)




,base AS(
    SELECT
        t1.dt -- `日期`
        ,t2.category_level1_id -- `一级类目id`
        ,t2.category_level1_name -- `一级类目名称`
        ,t2.category_level4_id -- `四级类目id`
        ,t2.category_level4_name -- `四级类目名称`

        ,MAX(IF(t1.dt=t2.dt, t2.merchant_num_onsale, 0)) AS merchant_num_onsale -- `上架商家数量`
        ,MAX(IF(t1.dt=t2.dt, t2.sku_num_sold, 0)) AS sku_num_sold -- `动销商品数量`
        ,MAX(IF(t1.dt=t2.dt, t2.ordered_goods_amt, 0)) AS ordered_goods_amt -- `下单金额`
        ,MAX(IF(t1.dt=t2.dt, t2.ordered_goods_num, 0)) AS ordered_goods_num -- `下单数量`
        ,MAX(IF(t1.dt=t2.dt, t2.ordered_store_num, 0)) AS ordered_store_num -- `下单店铺数`
        ,MAX(IF(t1.dt=t2.dt, t2.delivered_goods_amt, 0)) AS delivered_goods_amt -- `送达金额`
        ,MAX(IF(t1.dt=t2.dt, t2.delivered_goods_num, 0)) AS delivered_goods_num -- `送达数量`
        ,MAX(IF(t1.dt=t2.dt, t2.after_sale_store_num_order_time, 0)) AS after_sale_store_num_order_time -- `售后店铺数`
        ,MAX(IF(t1.dt=t2.dt, t2.final_refund_amt_order_time, 0)) AS final_refund_amt_order_time -- `售后赔付金额`
        ,MAX(IF(t1.dt=t2.dt, t2.final_refund_amt_quality_order_time, 0)) AS final_refund_amt_quality_order_time -- `质量问题售后赔付金额`

        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 13, "dd") AND DATEADD(DATE(t1.dt), ${end_offset} - 7, "dd")
            , t2.merchant_num_onsale, 0
        )) AS merchant_num_onsale_m13tm7d -- `m13d到m7d上架商家数量`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 13, "dd") AND DATEADD(DATE(t1.dt), ${end_offset} - 7, "dd")
            , t2.sku_num_sold, 0
        )) AS sku_num_sold_m13tm7d -- `m13d到m7d动销商品数量`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 13, "dd") AND DATEADD(DATE(t1.dt), ${end_offset} - 7, "dd")
            , t2.ordered_goods_amt, 0
        )) AS ordered_goods_amt_m13tm7d -- `m13d到m7d下单金额`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 13, "dd") AND DATEADD(DATE(t1.dt), ${end_offset} - 7, "dd")
            , t2.ordered_goods_num, 0
        )) AS ordered_goods_num_m13tm7d -- `m13d到m7d下单数量`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 13, "dd") AND DATEADD(DATE(t1.dt), ${end_offset} - 7, "dd")
            , t2.ordered_store_num, 0
        )) AS ordered_store_num_m13tm7d -- `m13d到m7d下单店铺数`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 13, "dd") AND DATEADD(DATE(t1.dt), ${end_offset} - 7, "dd")
            , t2.delivered_goods_amt, 0
        )) AS delivered_goods_amt_m13tm7d -- `m13d到m7d送达金额`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 13, "dd") AND DATEADD(DATE(t1.dt), ${end_offset} - 7, "dd")
            , t2.delivered_goods_num, 0
        )) AS delivered_goods_num_m13tm7d -- `m13d到m7d送达数量`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 13, "dd") AND DATEADD(DATE(t1.dt), ${end_offset} - 7, "dd")
            , t2.after_sale_store_num_order_time, 0
        )) AS after_sale_store_num_order_time_m13tm7d -- `m13d到m7d售后店铺数`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 13, "dd") AND DATEADD(DATE(t1.dt), ${end_offset} - 7, "dd")
            , t2.final_refund_amt_order_time, 0
        )) AS final_refund_amt_order_time_m13tm7d -- `m13d到m7d售后赔付金额`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 13, "dd") AND DATEADD(DATE(t1.dt), ${end_offset} - 7, "dd")
            , t2.final_refund_amt_quality_order_time, 0
        )) AS final_refund_amt_quality_order_time_m13tm7d -- `m13d到m7d质量问题售后赔付金额`

        ,COUNT(DISTINCT IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 13, "dd") AND DATEADD(DATE(t1.dt), ${end_offset} - 7, "dd") AND t2.ordered_goods_num>0
            , t2.dt, NULL
        )) AS ordered_days_m13tm7d -- `m13d到m7d下单天数`

        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 6, "dd") AND t1.dt
            , t2.merchant_num_onsale, 0
        )) AS merchant_num_onsale_m6tcd -- `近7天上架商家数量`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 6, "dd") AND t1.dt
            , t2.sku_num_sold, 0
        )) AS sku_num_sold_m6tcd -- `近7天动销商品数量`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 6, "dd") AND t1.dt
            , t2.ordered_goods_amt, 0
        )) AS ordered_goods_amt_m6tcd -- `近7天下单金额`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 6, "dd") AND t1.dt
            , t2.ordered_goods_num, 0
        )) AS ordered_goods_num_m6tcd -- `近7天下单数量`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 6, "dd") AND t1.dt
            , t2.ordered_store_num, 0
        )) AS ordered_store_num_m6tcd -- `近7天下单店铺数`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 6, "dd") AND t1.dt
            , t2.delivered_goods_amt, 0
        )) AS delivered_goods_amt_m6tcd -- `近7天送达金额`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 6, "dd") AND t1.dt
            , t2.delivered_goods_num, 0
        )) AS delivered_goods_num_m6tcd -- `近7天送达数量`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 6, "dd") AND t1.dt
            , t2.after_sale_store_num_order_time, 0
        )) AS after_sale_store_num_order_time_m6tcd -- `近7天售后店铺数`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 6, "dd") AND t1.dt
            , t2.final_refund_amt_order_time, 0
        )) AS final_refund_amt_order_time_m6tcd -- `近7天售后赔付金额`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 6, "dd") AND t1.dt
            , t2.final_refund_amt_quality_order_time, 0
        )) AS final_refund_amt_quality_order_time_m6tcd -- `近7天质量问题售后赔付金额`

        ,COUNT(DISTINCT IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 6, "dd") AND t1.dt AND t2.ordered_goods_num>0
            , t2.dt, NULL
        )) AS ordered_days_m6tcd -- `近7天下单天数`


    FROM dt_range t1
    JOIN (
        SELECT
            t1.dt -- `日期`
            ,t1.category_level1_id -- `一级类目id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`

            ,t1.merchant_num_onsale -- `上架商家数量`
            ,t1.sku_num_sold -- `动销商品数量`
            ,t1.ordered_goods_amt -- `下单金额`
            ,t1.ordered_goods_num -- `下单数量`
            ,t1.ordered_store_num -- `下单店铺数`
            ,t1.delivered_goods_amt -- `送达金额`
            ,t1.delivered_goods_num -- `送达数量`
            ,t1.after_sale_store_num_order_time -- `售后店铺数`
            ,t1.final_refund_amt_order_time -- `售后赔付金额`
            ,t1.final_refund_amt_quality_order_time -- `质量问题售后赔付金额`
            ,1 AS dummy
        FROM datawarehouse_max.dws_pub_mall_category_level4_base_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset} - 15, "dd")
                    AND DATEADD(${date_param}, ${end_offset}, "dd")
            AND t1.mall_id = 871
            AND t1.category_level1_name = "水果"
    ) t2
        ON t2.dummy = t1.dummy
        AND t2.dt BETWEEN DATEADD(DATE(t1.dt), ${end_offset} - 14, "dd") AND t1.dt

    GROUP BY t1.dt -- `日期`
        ,t2.category_level1_id -- `一级类目id`
        ,t2.category_level1_name -- `一级类目名称`
        ,t2.category_level4_id -- `四级类目id`
        ,t2.category_level4_name -- `四级类目名称`
)

SELECT
	t1.dt AS `日期`
	,t1.category_level1_id AS `一级类目id`
	,t1.category_level1_name AS `一级类目名称`
	,t1.category_level4_id AS `四级类目id`
	,t1.category_level4_name AS `四级类目名称`

	,t1.merchant_num_onsale AS `上架商家数量`
	,t1.sku_num_sold AS `动销商品数量`
	,t1.ordered_goods_amt AS `下单金额`
	,t1.ordered_goods_num AS `下单数量`
	,t1.ordered_store_num AS `下单店铺数`
	,t1.delivered_goods_amt AS `送达金额`
	,t1.delivered_goods_num AS `送达数量`
	,t1.after_sale_store_num_order_time AS `售后店铺数`
	,t1.final_refund_amt_order_time AS `售后赔付金额`
	,t1.final_refund_amt_quality_order_time AS `质量问题售后赔付金额`

	,t1.merchant_num_onsale_m13tm7d AS `m13d到m7d上架商家数量`
	,t1.sku_num_sold_m13tm7d AS `m13d到m7d动销商品数量`
	,t1.ordered_goods_amt_m13tm7d AS `m13d到m7d下单金额`
	,t1.ordered_goods_num_m13tm7d AS `m13d到m7d下单数量`
	,t1.ordered_store_num_m13tm7d AS `m13d到m7d下单店铺数`
	,t1.delivered_goods_amt_m13tm7d AS `m13d到m7d送达金额`
	,t1.delivered_goods_num_m13tm7d AS `m13d到m7d送达数量`
	,t1.after_sale_store_num_order_time_m13tm7d AS `m13d到m7d售后店铺数`
	,t1.final_refund_amt_order_time_m13tm7d AS `m13d到m7d售后赔付金额`
	,t1.final_refund_amt_quality_order_time_m13tm7d AS `m13d到m7d质量问题售后赔付金额`
    ,t1.ordered_days_m13tm7d AS `m13d到m7d下单天数`
	,t1.merchant_num_onsale_m6tcd AS `近7天上架商家数量`
	,t1.sku_num_sold_m6tcd AS `近7天动销商品数量`
	,t1.ordered_goods_amt_m6tcd AS `近7天下单金额`
	,t1.ordered_goods_num_m6tcd AS `近7天下单数量`
	,t1.ordered_store_num_m6tcd AS `近7天下单店铺数`
	,t1.delivered_goods_amt_m6tcd AS `近7天送达金额`
	,t1.delivered_goods_num_m6tcd AS `近7天送达数量`
	,t1.after_sale_store_num_order_time_m6tcd AS `近7天售后店铺数`
	,t1.final_refund_amt_order_time_m6tcd AS `近7天售后赔付金额`
	,t1.final_refund_amt_quality_order_time_m6tcd AS `近7天质量问题售后赔付金额`

	,t1.ordered_days_m6tcd AS `近7天下单天数`

FROM base t1

WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
    AND DATEADD(${date_param}, ${end_offset}, "dd")
;