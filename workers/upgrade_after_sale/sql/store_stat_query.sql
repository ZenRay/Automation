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
        ,t2.customer_store_id -- `店铺id`
        ,MAX(IF(t1.dt=t2.dt, t2.exposed_cnt, 0)) AS exposed_cnt -- `曝光次数`
        ,MAX(IF(t1.dt=t2.dt, t2.ordered_goods_amt, 0)) AS ordered_goods_amt -- `下单金额`
        ,MAX(IF(t1.dt=t2.dt, t2.delivered_goods_amt, 0)) AS delivered_goods_amt -- `送达金额`
        ,MAX(IF(t1.dt=t2.dt, t2.delivered_goods_num, 0)) AS delivered_goods_num -- `送达数量`
        ,MAX(IF(t1.dt=t2.dt, t2.after_sale_num_quality_order_time, 0)) AS after_sale_num_quality_order_time -- `质量问题售后数量`
        ,MAX(IF(t1.dt=t2.dt, t2.after_sale_num_order_time, 0)) AS after_sale_num_order_time -- `售后数量`
        ,MAX(IF(t1.dt=t2.dt, t2.final_refund_amt_order_time_quality, 0)) AS final_refund_amt_order_time_quality -- `质量问题售后赔付金额`
        ,MAX(IF(t1.dt=t2.dt, t2.final_refund_amt_order_time, 0)) AS final_refund_amt_order_time -- `售后赔付金额`
        ,MAX(IF(t1.dt=t2.dt, t2.final_refund_amt, 0)) AS final_refund_amt -- `自然日售后赔付金额`
        ,MAX(IF(t1.dt=t2.dt, t2.commission_amt, 0)) AS commission_amt -- `平台抽佣金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 29, "dd") AND t2.dt, t2.exposed_cnt, 0)) AS exposed_cnt_m29tcd -- `近30天曝光次数`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 29, "dd") AND t2.dt, t2.ordered_goods_amt, 0)) AS ordered_goods_amt_m29tcd -- `近30天下单金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 29, "dd") AND t2.dt, t2.delivered_goods_amt, 0)) AS delivered_goods_amt_m29tcd -- `近30天送达金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 29, "dd") AND t2.dt, t2.delivered_goods_num, 0)) AS delivered_goods_num_m29tcd -- `近30天送达数量`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 29, "dd") AND t2.dt, t2.after_sale_num_quality_order_time, 0)) AS after_sale_num_quality_order_time_m29tcd -- `近30天质量问题售后数量`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 29, "dd") AND t2.dt, t2.after_sale_num_order_time, 0)) AS after_sale_num_order_time_m29tcd -- `近30天售后数量`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 29, "dd") AND t2.dt, t2.final_refund_amt_order_time_quality, 0)) AS final_refund_amt_order_time_quality_m29tcd -- `近30天质量问题售后赔付金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 29, "dd") AND t2.dt, t2.final_refund_amt_order_time, 0)) AS final_refund_amt_order_time_m29tcd -- `近30天售后赔付金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 29, "dd") AND t2.dt, t2.final_refund_amt, 0)) AS final_refund_amt_m29tcd -- `近30天自然日售后赔付金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 29, "dd") AND t2.dt, t2.commission_amt, 0)) AS commission_amt_m29tcd -- `近30天平台抽佣金额`

        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 13, "dd") AND t2.dt, t2.exposed_cnt, 0)) AS exposed_cnt_m13tcd -- `近14天曝光次数`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 13, "dd") AND t2.dt, t2.ordered_goods_amt, 0)) AS ordered_goods_amt_m13tcd -- `近14天下单金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 13, "dd") AND t2.dt, t2.delivered_goods_amt, 0)) AS delivered_goods_amt_m13tcd -- `近14天送达金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 13, "dd") AND t2.dt, t2.delivered_goods_num, 0)) AS delivered_goods_num_m13tcd -- `近14天送达数量`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 13, "dd") AND t2.dt, t2.after_sale_num_quality_order_time, 0)) AS after_sale_num_quality_order_time_m13tcd -- `近14天质量问题售后数量`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 13, "dd") AND t2.dt, t2.after_sale_num_order_time, 0)) AS after_sale_num_order_time_m13tcd -- `近14天售后数量`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 13, "dd") AND t2.dt, t2.final_refund_amt_order_time_quality, 0)) AS final_refund_amt_order_time_quality_m13tcd -- `近14天质量问题售后赔付金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 13, "dd") AND t2.dt, t2.final_refund_amt_order_time, 0)) AS final_refund_amt_order_time_m13tcd -- `近14天售后赔付金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 13, "dd") AND t2.dt, t2.final_refund_amt, 0)) AS final_refund_amt_m13tcd -- `近14天自然日售后赔付金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 13, "dd") AND t2.dt, t2.commission_amt, 0)) AS commission_amt_m13tcd -- `近14天平台抽佣金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 6, "dd") AND t2.dt, t2.exposed_cnt, 0)) AS exposed_cnt_m6tcd -- `近7天曝光次数`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 6, "dd") AND t2.dt, t2.ordered_goods_amt, 0)) AS ordered_goods_amt_m6tcd -- `近7天下单金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 6, "dd") AND t2.dt, t2.delivered_goods_amt, 0)) AS delivered_goods_amt_m6tcd -- `近7天送达金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 6, "dd") AND t2.dt, t2.delivered_goods_num, 0)) AS delivered_goods_num_m6tcd -- `近7天送达数量`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 6, "dd") AND t2.dt, t2.after_sale_num_quality_order_time, 0)) AS after_sale_num_quality_order_time_m6tcd -- `近7天质量问题售后数量`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 6, "dd") AND t2.dt, t2.after_sale_num_order_time, 0)) AS after_sale_num_order_time_m6tcd -- `近7天售后数量`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 6, "dd") AND t2.dt, t2.final_refund_amt_order_time_quality, 0)) AS final_refund_amt_order_time_quality_m6tcd -- `近7天质量问题售后赔付金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 6, "dd") AND t2.dt, t2.final_refund_amt_order_time, 0)) AS final_refund_amt_order_time_m6tcd -- `近7天售后赔付金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 6, "dd") AND t2.dt, t2.final_refund_amt, 0)) AS final_refund_amt_m6tcd -- `近7天自然日售后赔付金额`
        ,SUM(IF(t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 6, "dd") AND t2.dt, t2.commission_amt, 0)) AS commission_amt_m6tcd -- `近7天平台抽佣金额`

        ,COUNT(DISTINCT IF(
            t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 29, "dd") AND t2.dt AND t2.exposed_cnt > 0, t2.dt, NULL
        )) AS exposed_days_m29tcd -- `近30天曝光天数`
        ,COUNT(DISTINCT IF(
            t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 29, "dd") AND t2.dt AND t2.ordered_goods_num > 0, t2.dt, NULL
        )) AS ordered_days_m29tcd -- `近30天下单天数`

        ,COUNT(DISTINCT IF(
            t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 13, "dd") AND DATEADD(DATE(t2.dt), ${end_offset} - 7, "dd")
                AND t2.exposed_cnt > 0, t2.dt, NULL
        )) AS exposed_days_m13tm7 -- `m13到m7曝光天数`
        ,COUNT(DISTINCT IF(
            t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 13, "dd") AND DATEADD(DATE(t2.dt), ${end_offset} - 7, "dd")
            AND t2.ordered_goods_num > 0, t2.dt, NULL
        )) AS ordered_days_m13tm7 -- `m13到m7下单天数`

        ,COUNT(DISTINCT IF(
            t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 6, "dd") AND t2.dt
                AND t2.exposed_cnt > 0, t2.dt, NULL
        )) AS exposed_days_m6tcd -- `近7天曝光天数`
        ,COUNT(DISTINCT IF(
            t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 6, "dd") AND t2.dt
            AND t2.ordered_goods_num > 0, t2.dt, NULL
        )) AS ordered_days_m6tcd -- `近7天下单天数`
    FROM dt_range t1
    JOIN (
        SELECT
            t1.dt -- `日期`
            ,t1.customer_store_id -- `店铺id`
            ,t1.exposed_cnt -- `曝光次数`
            ,t1.ordered_goods_amt -- `下单金额`
            ,t1.ordered_goods_num
            ,t1.delivered_goods_amt -- `送达金额`
            ,t1.delivered_goods_num -- `送达数量`
            ,t1.after_sale_num_quality_order_time -- `质量问题售后数量`
            ,t1.after_sale_num_order_time -- `售后数量`
            ,t1.final_refund_amt_order_time_quality -- `质量问题售后赔付金额`
            ,t1.final_refund_amt_order_time -- `售后赔付金额`
            ,t1.final_refund_amt -- `自然日售后赔付金额`
            ,t1.commission_amt -- `平台抽佣金额`
            ,1 AS dummy

        FROM datawarehouse_max.dws_store_mall_store_base_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(${date_param}, ${end_offset} - 30, "dd")
                    AND DATEADD(${date_param}, ${end_offset}, "dd")
            AND t1.mall_id = 871

            AND NVL(t1.exposed_cnt ,0) + NVL(t1.ordered_goods_amt, 0) + NVL(t1.final_refund_amt, 0)> 0
    ) t2
        ON t2.dummy = t1.dummy
        AND t1.dt BETWEEN DATEADD(DATE(t2.dt), ${end_offset} - 29, "dd") AND t2.dt

    GROUP BY t1.dt
        ,t2.customer_store_id
)



SELECT
	t1.dt AS `日期`
	,t1.customer_store_id AS `店铺id`
	,t1.exposed_cnt AS `曝光次数`
	,t1.ordered_goods_amt AS `下单金额`
	,t1.delivered_goods_amt AS `送达金额`
	,t1.delivered_goods_num AS `送达数量`
	,t1.after_sale_num_quality_order_time AS `质量问题售后数量`
	,t1.after_sale_num_order_time AS `售后数量`
	,t1.final_refund_amt_order_time_quality AS `质量问题售后赔付金额`
	,t1.final_refund_amt_order_time AS `售后赔付金额`
	,t1.final_refund_amt AS `自然日售后赔付金额`
	,t1.commission_amt AS `平台抽佣金额`
	,t1.exposed_cnt_m29tcd AS `近30天曝光次数`
	,t1.ordered_goods_amt_m29tcd AS `近30天下单金额`
	,t1.delivered_goods_amt_m29tcd AS `近30天送达金额`
	,t1.delivered_goods_num_m29tcd AS `近30天送达数量`
	,t1.after_sale_num_quality_order_time_m29tcd AS `近30天质量问题售后数量`
	,t1.after_sale_num_order_time_m29tcd AS `近30天售后数量`
	,t1.final_refund_amt_order_time_quality_m29tcd AS `近30天质量问题售后赔付金额`
	,t1.final_refund_amt_order_time_m29tcd AS `近30天售后赔付金额`
	,t1.final_refund_amt_m29tcd AS `近30天自然日售后赔付金额`
	,t1.commission_amt_m29tcd AS `近30天平台抽佣金额`

	,t1.exposed_cnt_m13tcd AS `近14天曝光次数`
	,t1.ordered_goods_amt_m13tcd AS `近14天下单金额`
	,t1.delivered_goods_amt_m13tcd AS `近14天送达金额`
	,t1.delivered_goods_num_m13tcd AS `近14天送达数量`
	,t1.after_sale_num_quality_order_time_m13tcd AS `近14天质量问题售后数量`
	,t1.after_sale_num_order_time_m13tcd AS `近14天售后数量`
	,t1.final_refund_amt_order_time_quality_m13tcd AS `近14天质量问题售后赔付金额`
	,t1.final_refund_amt_order_time_m13tcd AS `近14天售后赔付金额`
	,t1.final_refund_amt_m13tcd AS `近14天自然日售后赔付金额`
	,t1.commission_amt_m13tcd AS `近14天平台抽佣金额`
	,t1.exposed_cnt_m6tcd AS `近7天曝光次数`
	,t1.ordered_goods_amt_m6tcd AS `近7天下单金额`
	,t1.delivered_goods_amt_m6tcd AS `近7天送达金额`
	,t1.delivered_goods_num_m6tcd AS `近7天送达数量`
	,t1.after_sale_num_quality_order_time_m6tcd AS `近7天质量问题售后数量`
	,t1.after_sale_num_order_time_m6tcd AS `近7天售后数量`
	,t1.final_refund_amt_order_time_quality_m6tcd AS `近7天质量问题售后赔付金额`
	,t1.final_refund_amt_order_time_m6tcd AS `近7天售后赔付金额`
	,t1.final_refund_amt_m6tcd AS `近7天自然日售后赔付金额`
	,t1.commission_amt_m6tcd AS `近7天平台抽佣金额`

	,t1.exposed_days_m29tcd AS `近30天曝光天数`
	,t1.ordered_days_m29tcd AS `近30天下单天数`
	,t1.exposed_days_m13tm7 AS `m13到m7曝光天数`
	,t1.ordered_days_m13tm7 AS `m13到m7下单天数`
	,t1.exposed_days_m6tcd AS `近7天曝光天数`
	,t1.ordered_days_m6tcd AS `近7天下单天数`
FROM base t1
WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                        AND DATEADD(${date_param}, ${end_offset}, "dd")
;