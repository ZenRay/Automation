WITH dt_range AS(
    SELECT
        dummy
        ,DATEADD(DATE(${date_param}), gap, "dd") AS dt
    FROM VALUES (1) AS t(dummy)
    LATERAL VIEW EXPLODE(SEQUENCE(${end_offset}, ${start_offset}, -1)) t1 AS gap
)




,gmv90 AS (
    SELECT
        DATEADD(DATE(${date_param}), ${end_offset}, "dd") AS dt
        ,customer_store_id
        
        ,SUM(ordered_goods_amt) AS ordered_goods_amt_m89tcd
        ,COUNT(DISTINCT IF(t1.ordered_goods_num>0, t1.dt, NULL)) AS ordered_days_m89tcd
        
    FROM datawarehouse_max.dws_store_mall_store_base_daily_asc t1
    WHERE dt BETWEEN DATEADD(DATE(${date_param}), ${start_offset} - 89, "dd")
                AND DATEADD(DATE(${date_param}), ${end_offset}, "dd")
      AND mall_id = 871
    GROUP BY customer_store_id
      
)



-- 门店下单日期
,store_date AS(
    SELECT
        t1.dt
        ,t1.mall_id
        ,t1.customer_store_id
        ,t1.mall_first_order_dt
        ,t1.mall_last_order_dt
    FROM datawarehouse_max.dws_store_mall_store_label_daily_full t1
    WHERE t1.dt BETWEEN DATEADD(DATE(${date_param}), ${start_offset}, "dd")
            AND DATEADD(DATE(${date_param}), ${end_offset}, "dd")
        AND t1.mall_id = 871

)

-- 门店品类
,store_cat AS(
    SELECT /*+MAPJOIN(t1) */
        t1.dt
        ,t2.mall_id
        ,t2.customer_store_id
        ,t2.category_level1_id
        ,t3.category_level1_name
        ,t2.category_level4_id
        ,COUNT(DISTINCT IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 13, "dd") AND DATEADD(DATE(t1.dt),  0, "dd"), t2.dt, NULL
        )) AS ordered_days_m13tcd
    FROM dt_range t1
    JOIN datawarehouse_max.dws_store_mall_store_category_level4_base_daily_asc t2
        ON t2.dt BETWEEN DATEADD(DATE(${date_param}), ${start_offset} - 20, "dd")
                    AND DATEADD(DATE(${date_param}), ${end_offset}, "dd")
        AND t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 13, "dd") AND DATEADD(DATE(t1.dt),  0, "dd")
        
    LEFT JOIN datawarehouse_max.dim_category_daily_full t3
        ON t3.dt BETWEEN DATEADD(DATE(${date_param}), ${start_offset} - 20, "dd")
                    AND DATEADD(DATE(${date_param}), ${end_offset}, "dd")
        AND t3.dt = t1.dt
        AND t3.back_category_id = t2.category_level4_id
    WHERE t2.ordered_goods_num > 0
        AND t2.mall_id = 871
    GROUP BY  t1.dt
        ,t2.mall_id
        ,t2.customer_store_id
        ,t2.category_level1_id
        ,t3.category_level1_name
        ,t2.category_level4_id

)



,base AS(
    SELECT
        t1.dt -- `日期`
        ,871 AS mall_id
        ,t2.customer_store_id -- `店铺id`
        ,MAX(IF(t1.dt=t2.dt, t2.ordered_goods_amt, 0)) AS ordered_goods_amt -- `下单金额`
        ,MAX(IF(t1.dt=t2.dt, t2.delivered_goods_amt, 0)) AS delivered_goods_amt -- `送达金额`
        ,MAX(IF(t1.dt=t2.dt, t2.delivered_goods_num, 0)) AS delivered_goods_num -- `送达数量`
        ,MAX(IF(t1.dt=t2.dt, t2.after_sale_num_quality_order_time, 0)) AS after_sale_num_quality_order_time -- `质量问题售后数量`
        ,MAX(IF(t1.dt=t2.dt, t2.after_sale_num_order_time, 0)) AS after_sale_num_order_time -- `售后数量`
        ,MAX(IF(t1.dt=t2.dt, t2.final_refund_amt_order_time_quality, 0)) AS final_refund_amt_order_time_quality -- `质量问题售后赔付金额`
        ,MAX(IF(t1.dt=t2.dt, t2.final_refund_amt_order_time, 0)) AS final_refund_amt_order_time -- `售后赔付金额`
        ,MAX(IF(t1.dt=t2.dt, t2.final_refund_amt, 0)) AS final_refund_amt -- `自然日售后赔付金额`
        ,MAX(IF(t1.dt=t2.dt, t2.commission_amt, 0)) AS commission_amt -- `平台抽佣金额`
 
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 29, "dd") AND t1.dt, t2.order_ticked_num, 0)) AS order_ticked_num_m29tcd -- `近30天订单数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 29, "dd") AND t1.dt, t2.order_item_ticked_num, 0)) AS order_item_ticked_num_m29tcd -- `近30天明细订单数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 29, "dd") AND t1.dt, t2.ordered_goods_amt, 0)) AS ordered_goods_amt_m29tcd -- `近30天下单金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 29, "dd") AND t1.dt, t2.delivered_goods_amt, 0)) AS delivered_goods_amt_m29tcd -- `近30天送达金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 29, "dd") AND t1.dt, t2.delivered_goods_num, 0)) AS delivered_goods_num_m29tcd -- `近30天送达数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 29, "dd") AND t1.dt, t2.after_sale_num_quality_order_time, 0)) AS after_sale_num_quality_order_time_m29tcd -- `近30天质量问题售后数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 29, "dd") AND t1.dt, t2.after_sale_num_order_time, 0)) AS after_sale_num_order_time_m29tcd -- `近30天售后数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 29, "dd") AND t1.dt, t2.final_refund_amt_order_time_quality, 0)) AS final_refund_amt_order_time_quality_m29tcd -- `近30天质量问题售后赔付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 29, "dd") AND t1.dt, t2.final_refund_amt_order_time, 0)) AS final_refund_amt_order_time_m29tcd -- `近30天售后赔付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 29, "dd") AND t1.dt, t2.final_refund_amt, 0)) AS final_refund_amt_m29tcd -- `近30天自然日售后赔付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 29, "dd") AND t1.dt, t2.commission_amt, 0)) AS commission_amt_m29tcd -- `近30天平台抽佣金额`

        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt), -29, "dd") AND t1.dt, t2.withdraw_after_sale_ticket_num, 0)) AS withdraw_after_sale_ticket_num_m29tcd -- `近30天自然日撤销售后单数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt), -29, "dd") AND t1.dt, t2.total_after_sale_ticket_num, 0)) AS total_after_sale_ticket_num_m29tcd -- `近30天自然日售后单数量`

        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 13, "dd") AND t1.dt, t2.ordered_goods_amt, 0)) AS ordered_goods_amt_m13tcd -- `近14天下单金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 13, "dd") AND t1.dt, t2.delivered_goods_amt, 0)) AS delivered_goods_amt_m13tcd -- `近14天送达金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 13, "dd") AND t1.dt, t2.delivered_goods_num, 0)) AS delivered_goods_num_m13tcd -- `近14天送达数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 13, "dd") AND t1.dt, t2.after_sale_num_quality_order_time, 0)) AS after_sale_num_quality_order_time_m13tcd -- `近14天质量问题售后数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 13, "dd") AND t1.dt, t2.after_sale_num_order_time, 0)) AS after_sale_num_order_time_m13tcd -- `近14天售后数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 13, "dd") AND t1.dt, t2.final_refund_amt_order_time_quality, 0)) AS final_refund_amt_order_time_quality_m13tcd -- `近14天质量问题售后赔付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 13, "dd") AND t1.dt, t2.final_refund_amt_order_time, 0)) AS final_refund_amt_order_time_m13tcd -- `近14天售后赔付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 13, "dd") AND t1.dt, t2.final_refund_amt, 0)) AS final_refund_amt_m13tcd -- `近14天自然日售后赔付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 13, "dd") AND t1.dt, t2.commission_amt, 0)) AS commission_amt_m13tcd -- `近14天平台抽佣金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 6, "dd") AND t1.dt, t2.ordered_goods_amt, 0)) AS ordered_goods_amt_m6tcd -- `近7天下单金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 6, "dd") AND t1.dt, t2.delivered_goods_amt, 0)) AS delivered_goods_amt_m6tcd -- `近7天送达金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 6, "dd") AND t1.dt, t2.delivered_goods_num, 0)) AS delivered_goods_num_m6tcd -- `近7天送达数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 6, "dd") AND t1.dt, t2.after_sale_num_quality_order_time, 0)) AS after_sale_num_quality_order_time_m6tcd -- `近7天质量问题售后数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 6, "dd") AND t1.dt, t2.after_sale_num_order_time, 0)) AS after_sale_num_order_time_m6tcd -- `近7天售后数量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 6, "dd") AND t1.dt, t2.final_refund_amt_order_time_quality, 0)) AS final_refund_amt_order_time_quality_m6tcd -- `近7天质量问题售后赔付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 6, "dd") AND t1.dt, t2.final_refund_amt_order_time, 0)) AS final_refund_amt_order_time_m6tcd -- `近7天售后赔付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 6, "dd") AND t1.dt, t2.final_refund_amt, 0)) AS final_refund_amt_m6tcd -- `近7天自然日售后赔付金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 6, "dd") AND t1.dt, t2.commission_amt, 0)) AS commission_amt_m6tcd -- `近7天平台抽佣金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 6, "dd") AND t1.dt, t2.uas_platform_refund_amt, 0)) AS uas_platform_refund_amt_m6tcd -- `近7日升级售后自然日平台承担金额`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 6, "dd") AND t1.dt, t2.uas_ticket_num, 0)) AS uas_ticket_num_m6tcd -- `近7日升级售后自然日单量`
        ,SUM(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 6, "dd") AND t1.dt, NVL(t2.uas_timeout_ticket_num,0) + NVL(t2.total_after_sale_ticket_num, 0), 0)) AS total_after_sale_ticket_num_m6tcd -- `近7日总售后自然日单量`

        ,COUNT(DISTINCT IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 29, "dd") AND t1.dt AND t2.ordered_goods_num > 0, t2.dt, NULL
        )) AS ordered_days_m29tcd -- `近30天下单天数`

        ,COUNT(DISTINCT IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 13, "dd") AND DATEADD(DATE(t1.dt),  - 7, "dd")
            AND t2.ordered_goods_num > 0, t2.dt, NULL
        )) AS ordered_days_m13tm7 -- `m13到m7下单天数`
        ,COUNT(DISTINCT IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 13, "dd") AND DATEADD(DATE(t1.dt),  - 3, "dd")
            AND t2.uas_ticket_num > 0, t2.dt, NULL
        )) AS uas_days_m13tm3 -- `m13到m3升级售后天数`
        ,COUNT(DISTINCT IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 13, "dd") AND DATEADD(DATE(t1.dt),  - 3, "dd")
            AND t2.uas_ticket_num > 0 AND t2.is_ordered_cdta3d>0, t2.dt, NULL
        )) AS uas_rebuy_cdta3d_days_m13tm3 -- `m13到m3升级售后且4日复购天数`

        ,COUNT(DISTINCT IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt),  - 6, "dd") AND t1.dt
            AND t2.ordered_goods_num > 0, t2.dt, NULL
        )) AS ordered_days_m6tcd -- `近7天下单天数`
    FROM dt_range t1
    JOIN (
        SELECT
            t1.dt -- `日期`
            ,t1.customer_store_id -- `店铺id`
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
            ,0 AS withdraw_after_sale_ticket_num -- `自然日撤销售后单数量`
            ,0 AS total_after_sale_ticket_num -- `自然日售后单数量`
            ,0 AS order_ticked_num -- `订单数量`
            ,0 AS order_item_ticked_num -- `明细订单数量`
            ,0 AS uas_platform_refund_amt -- `升级售后自然日平台承担金额`
            ,0 AS uas_ticket_num -- `升级售后自然日单量`
            ,0 AS uas_timeout_ticket_num -- `超时升级售后自然日单量`
            ,0 AS is_ordered_cdta6d -- `是否后7日下单`
            ,0 AS is_ordered_cdta3d -- `是否后3日下单`
            ,1 AS dummy
        FROM datawarehouse_max.dws_store_mall_store_base_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(DATE(${date_param}), ${start_offset} - 30, "dd")
                    AND DATEADD(DATE(${date_param}), ${end_offset} + 7, "dd")
            AND t1.mall_id = 871

            AND NVL(t1.ordered_goods_amt, 0) + NVL(t1.final_refund_amt, 0)> 0
        
        UNION ALL
        SELECT
            t1.dt -- `日期`
            ,t1.customer_store_id -- `店铺id`
            ,0 AS ordered_goods_amt -- `下单金额`
            ,0 AS ordered_goods_num
            ,0 AS delivered_goods_amt -- `送达金额`
            ,0 AS delivered_goods_num -- `送达数量`
            ,0 AS after_sale_num_quality_order_time -- `质量问题售后数量`
            ,0 AS after_sale_num_order_time -- `售后数量`
            ,0 AS final_refund_amt_order_time_quality -- `质量问题售后赔付金额`
            ,0 AS final_refund_amt_order_time -- `售后赔付金额`
            ,0 AS final_refund_amt -- `自然日售后赔付金额`
            ,0 AS commission_amt -- `平台抽佣金额`

            ,COUNT(DISTINCT IF(t1.status="CANCEL", t1.after_sale_order_id, NULL)) AS withdraw_after_sale_ticket_num -- `自然日撤销售后单数量`
            ,COUNT(DISTINCT t1.after_sale_order_id) AS total_after_sale_ticket_num -- `自然日售后单数量`
            ,0 AS order_ticked_num -- `订单数量`
            ,0 AS order_item_ticked_num -- `明细订单数量`

            ,0 AS uas_platform_refund_amt -- `升级售后自然日平台承担金额`
            ,0 AS uas_ticket_num -- `升级售后自然日单量`
            ,0 AS uas_timeout_ticket_num -- `超时升级售后自然日单量`
            ,0 AS is_ordered_cdta6d -- `是否后7日下单`
            ,0 AS is_ordered_cdta3d -- `是否后3日下单`
            ,1 AS dummy
        FROM datawarehouse_max.dwt_order_after_sale_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(DATE(${date_param}), ${start_offset} - 30, "dd")
                    AND DATEADD(DATE(${date_param}), ${end_offset}, "dd")
            AND t1.mall_id = 871

        GROUP BY t1.dt -- `日期`
            ,t1.customer_store_id -- `店铺id`

        UNION ALL
        SELECT
            t1.dt -- `日期`
            ,t1.customer_store_id -- `店铺id`
            ,0 AS ordered_goods_amt -- `下单金额`
            ,0 AS ordered_goods_num
            ,0 AS delivered_goods_amt -- `送达金额`
            ,0 AS delivered_goods_num -- `送达数量`
            ,0 AS after_sale_num_quality_order_time -- `质量问题售后数量`
            ,0 AS after_sale_num_order_time -- `售后数量`
            ,0 AS final_refund_amt_order_time_quality -- `质量问题售后赔付金额`
            ,0 AS final_refund_amt_order_time -- `售后赔付金额`
            ,0 AS final_refund_amt -- `自然日售后赔付金额`
            ,0 AS commission_amt -- `平台抽佣金额`

            ,0 AS withdraw_after_sale_ticket_num -- `自然日撤销售后单数量`
            ,0 AS total_after_sale_ticket_num -- `自然日售后单数量`

            ,COUNT(DISTINCT t1.order_id) AS order_ticked_num -- `订单数量`
            ,COUNT(DISTINCT t1.order_item_id) AS order_item_ticked_num -- `明细订单数量`

            ,0 AS uas_platform_refund_amt -- `升级售后自然日平台承担金额`
            ,0 AS uas_ticket_num -- `升级售后自然日单量`
            ,0 AS uas_timeout_ticket_num -- `超时升级售后自然日单量`
            ,0 AS is_ordered_cdta6d -- `是否后7日下单`
            ,0 AS is_ordered_cdta3d -- `是否后3日下单`
            ,1 AS dummy
        FROM datawarehouse_max.dwt_order_order_item_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(DATE(${date_param}), ${start_offset} - 30, "dd")
                    AND DATEADD(DATE(${date_param}), ${end_offset}, "dd")
            AND t1.mall_id = 871
            AND t1.status != "CANCEL"

        GROUP BY t1.dt -- `日期`
            ,t1.customer_store_id -- `店铺id`

        -- 升级售后
        UNION ALL
        SELECT
            t1.dt --`日期`
            ,t1.customer_store_id --`店铺id`

            ,0 AS ordered_goods_amt -- `下单金额`
            ,0 AS ordered_goods_num
            ,0 AS delivered_goods_amt -- `送达金额`
            ,0 AS delivered_goods_num -- `送达数量`
            ,0 AS after_sale_num_quality_order_time -- `质量问题售后数量`
            ,0 AS after_sale_num_order_time -- `售后数量`
            ,0 AS final_refund_amt_order_time_quality -- `质量问题售后赔付金额`
            ,0 AS final_refund_amt_order_time -- `售后赔付金额`
            ,0 AS final_refund_amt -- `自然日售后赔付金额`
            ,0 AS commission_amt -- `平台抽佣金额`

            ,0 AS withdraw_after_sale_ticket_num -- `自然日撤销售后单数量`
            ,0 AS total_after_sale_ticket_num -- `自然日售后单数量`

            ,0 AS order_ticked_num -- `订单数量`
            ,0 AS order_item_ticked_num -- `明细订单数量`

            ,MAX(t1.uas_platform_refund_amt) AS uas_platform_refund_amt --`升级售后自然日平台承担金额`
            ,MAX(t1.uas_ticket_num) AS uas_ticket_num --`升级售后自然日单量`
            ,MAX(t1.uas_timeout_ticket_num) AS uas_timeout_ticket_num --`超时升级售后自然日单量`
            ,MAX(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt), 0, "dd") AND DATEADD(DATE(t1.dt), 6, "dd") AND t2.ordered_goods_num>0, 1, 0)) AS is_ordered_cdta6d -- `是否后7日下单`
            ,MAX(IF(t2.dt BETWEEN DATEADD(DATE(t1.dt), 0, "dd") AND DATEADD(DATE(t1.dt), 3, "dd") AND t2.ordered_goods_num>0, 1, 0)) AS is_ordered_cdta3d -- `是否后3日下单`
            ,1 AS dummy
        FROM(
            SELECT
                DATE(t1.create_time) AS dt
                ,t1.company_id AS mall_id
                ,t1.store_id AS customer_store_id

                ,0 AS ordered_goods_amt -- `下单金额`
                ,0 AS ordered_goods_num
                ,0 AS delivered_goods_amt -- `送达金额`
                ,0 AS delivered_goods_num -- `送达数量`
                ,0 AS after_sale_num_quality_order_time -- `质量问题售后数量`
                ,0 AS after_sale_num_order_time -- `售后数量`
                ,0 AS final_refund_amt_order_time_quality -- `质量问题售后赔付金额`
                ,0 AS final_refund_amt_order_time -- `售后赔付金额`
                ,0 AS final_refund_amt -- `自然日售后赔付金额`
                ,0 AS commission_amt -- `平台抽佣金额`

                ,0 AS withdraw_after_sale_ticket_num -- `自然日撤销售后单数量`
                ,0 AS total_after_sale_ticket_num -- `自然日售后单数量`

                ,0 AS order_ticked_num -- `订单数量`
                ,0 AS order_item_ticked_num -- `明细订单数量`

                ,SUM(t1.guo_li_payment_amount) AS uas_platform_refund_amt -- `升级售后自然日平台承担金额`
                ,COUNT(DISTINCT t1.upgrade_no) AS uas_ticket_num -- `升级售后自然日单量`
                ,COUNT(DISTINCT IF(t1.upgrade_type="TIMEOUT", t1.upgrade_no, NULL)) AS uas_timeout_ticket_num -- `超时升级售后自然日单量`
                
            FROM datawarehouse_max.ods_css_upgrade_after_sales_order_full t1
            WHERE t1.dt = MAX_PT('datawarehouse_max.ods_css_upgrade_after_sales_order_full')
                AND DATE(t1.create_time) BETWEEN DATEADD(DATE(${date_param}), ${start_offset} - 30, "dd")
                            AND DATEADD(DATE(${date_param}), ${end_offset}, "dd")
                AND t1.company_id = 871

                AND t1.process_status != "CANCEL"
            GROUP BY DATE(t1.create_time)
                ,t1.store_id
                ,t1.company_id
        ) t1
        LEFT JOIN datawarehouse_max.dws_store_mall_store_base_daily_asc t2
            ON t2.dt BETWEEN DATEADD(DATE(${date_param}), ${start_offset} - 30, "dd")
                AND DATEADD(DATE(${date_param}), ${end_offset}, "dd")
            AND t2.dt BETWEEN DATEADD(DATE(t1.dt), 0, "dd") AND DATEADD(DATE(t1.dt), 6, "dd")
            AND t2.mall_id  = t1.mall_id
            AND t2.customer_store_id = t1.customer_store_id
        GROUP BY t1.dt --`日期`
            ,t1.customer_store_id --`店铺id`


    ) t2
        ON t2.dummy = t1.dummy
        AND t2.dt BETWEEN DATEADD(DATE(t1.dt), - 29, "dd") AND t1.dt
        
    GROUP BY t1.dt
        ,t2.customer_store_id
)



SELECT
	t1.dt AS `日期`
    ,t3.grid_id AS `网格id`
    ,t3.grid_name AS `网格名称`
    ,t3.bd_id AS `bd_id`
    ,t3.bd_name AS `bd姓名`
	,t1.customer_store_id AS `店铺id`
    ,t5.mall_first_order_dt AS `最早下单日期`
    ,t5.mall_last_order_dt AS `最近下单日期`
    ,DATEDIFF(DATE(t1.dt), DATE(t5.mall_first_order_dt)) AS `最早下单间隔天数`
    ,DATEDIFF(DATE(t1.dt), DATE(t5.mall_last_order_dt)) AS `最近下单间隔天数`


    ,t2.ordered_goods_amt_m89tcd AS `近90天下单金额`
    ,t2.ordered_days_m89tcd AS `近90天下单天数`

	,t1.ordered_goods_amt AS `下单金额`
	,t1.delivered_goods_amt AS `送达金额`
	,t1.delivered_goods_num AS `送达数量`
	,t1.after_sale_num_quality_order_time AS `质量问题售后数量`
	,t1.after_sale_num_order_time AS `售后数量`
	,t1.final_refund_amt_order_time_quality AS `质量问题售后赔付金额`
	,t1.final_refund_amt_order_time AS `售后赔付金额`
	,t1.final_refund_amt AS `自然日售后赔付金额`
	,t1.commission_amt AS `平台抽佣金额`
 
	,t1.order_ticked_num_m29tcd AS `近30天订单数量`
	,t1.order_item_ticked_num_m29tcd AS `近30天明细订单数量`
	,t1.ordered_goods_amt_m29tcd AS `近30天下单金额`
	,t1.delivered_goods_amt_m29tcd AS `近30天送达金额`
	,t1.delivered_goods_num_m29tcd AS `近30天送达数量`
	,t1.after_sale_num_quality_order_time_m29tcd AS `近30天质量问题售后数量`
	,t1.after_sale_num_order_time_m29tcd AS `近30天售后数量`
	,t1.final_refund_amt_order_time_quality_m29tcd AS `近30天质量问题售后赔付金额`
	,t1.final_refund_amt_order_time_m29tcd AS `近30天售后赔付金额`
	,t1.final_refund_amt_m29tcd AS `近30天自然日售后赔付金额`
	,t1.commission_amt_m29tcd AS `近30天平台抽佣金额`

	,t1.withdraw_after_sale_ticket_num_m29tcd AS `近30天自然日撤销售后单数量`
	,t1.total_after_sale_ticket_num_m29tcd AS `近30天自然日售后单数量`

	,t1.ordered_goods_amt_m13tcd AS `近14天下单金额`
	,t1.delivered_goods_amt_m13tcd AS `近14天送达金额`
	,t1.delivered_goods_num_m13tcd AS `近14天送达数量`
	,t1.after_sale_num_quality_order_time_m13tcd AS `近14天质量问题售后数量`
	,t1.after_sale_num_order_time_m13tcd AS `近14天售后数量`
	,t1.final_refund_amt_order_time_quality_m13tcd AS `近14天质量问题售后赔付金额`
	,t1.final_refund_amt_order_time_m13tcd AS `近14天售后赔付金额`
	,t1.final_refund_amt_m13tcd AS `近14天自然日售后赔付金额`
	,t1.commission_amt_m13tcd AS `近14天平台抽佣金额`
	,t1.ordered_goods_amt_m6tcd AS `近7天下单金额`
	,t1.delivered_goods_amt_m6tcd AS `近7天送达金额`
	,t1.delivered_goods_num_m6tcd AS `近7天送达数量`
	,t1.after_sale_num_quality_order_time_m6tcd AS `近7天质量问题售后数量`
	,t1.after_sale_num_order_time_m6tcd AS `近7天售后数量`
	,t1.final_refund_amt_order_time_quality_m6tcd AS `近7天质量问题售后赔付金额`
	,t1.final_refund_amt_order_time_m6tcd AS `近7天售后赔付金额`
	,t1.final_refund_amt_m6tcd AS `近7天自然日售后赔付金额`
	,t1.commission_amt_m6tcd AS `近7天平台抽佣金额`
	,t1.uas_platform_refund_amt_m6tcd AS `近7日升级售后自然日平台承担金额`
	,t1.uas_ticket_num_m6tcd AS `近7日升级售后自然日单量`
	,t1.total_after_sale_ticket_num_m6tcd AS `近7日总售后自然日单量`

	,t1.ordered_days_m29tcd AS `近30天下单天数`

	,t1.ordered_days_m13tm7 AS `m13到m7下单天数`
	,t1.uas_days_m13tm3 AS `m13到m3升级售后天数`
	,t1.uas_rebuy_cdta3d_days_m13tm3 AS `m13到m3升级售后且4日复购天数`
    
	,t1.ordered_days_m6tcd AS `近7天下单天数`
    ,NVL(t4.ordered_cat4_num,0) AS `近14天下单品类数`
    ,NVL(t4.ordered_fruit_cat4_num,0) AS `近14天下单水果品类数`

FROM base t1
LEFT JOIN gmv90 t2  
    ON t2.dt = t1.dt
    AND t2.customer_store_id = t1.customer_store_id

LEFT JOIN (
    SELECT
        t1.dt
        ,t1.mall_id
        ,t1.customer_store_id
        ,t1.grid_id -- `网格id`
        ,t1.grid_name -- `网格名称`
        ,t1.bd_id -- `bd_id`
        ,t1.bd_name -- `bd姓名`
    
    FROM datawarehouse_max.dim_store_daily_full t1
    WHERE t1.dt = MAX_PT('datawarehouse_max.dim_store_daily_full')
    AND t1.mall_id = 871
) t3
    ON t3.customer_store_id = t1.customer_store_id
    ANd t3.mall_id = t1.mall_id

LEFT JOIN (
    SELECT
        t1.customer_store_id
        ,t1.mall_id
        ,t1.dt
        ,COUNT(DISTINCT IF(t1.ordered_days_m13tcd>0, t1.category_level4_id, NULL)) AS ordered_cat4_num -- `近14天下单品类数`
        ,COUNT(DISTINCT IF(t1.ordered_days_m13tcd>0 AND INSTR(t1.category_level1_name, "水果")>0, t1.category_level4_id, NULL)) AS ordered_fruit_cat4_num -- `近14天下单水果品类数`
    FROM store_cat t1
    
    GROUP BY t1.customer_store_id
        ,t1.mall_id
        ,t1.dt
) t4
    ON t4.dt = t1.dt
    AND t4.mall_id = t1.mall_id
    AND t4.customer_store_id = t1.customer_store_id

LEFT JOIN store_date t5
    ON t5.dt = t1.dt
    AND t5.mall_id = t1.mall_id
    AND t5.customer_store_id = t1.customer_store_id
WHERE t1.dt BETWEEN DATEADD(DATE(${date_param}), ${start_offset}, "dd")
    AND DATEADD(DATE(${date_param}), ${end_offset}, "dd")
;