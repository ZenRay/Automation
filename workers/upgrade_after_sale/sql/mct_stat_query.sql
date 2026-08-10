WITH dt_range AS(
    SELECT
        dummy
        ,DATEADD(DATE(${date_param}), gap, "dd") AS dt
    FROM VALUES (1) AS t(dummy)
    LATERAL VIEW EXPLODE(SEQUENCE(${end_offset}, ${start_offset}, -1)) t1 AS gap
)



,temp AS(
    SELECT
        t1.dt
        ,t1.merchant_id
        ,1 AS dummy

        ,SUM(NVL(t1.after_sale_ticket_num, 0)) AS after_sale_ticket_num -- `自然日售后单数量`

        ,SUM(NVL(t1.appeal_after_sale_ticket_num,0)) AS appeal_after_sale_ticket_num -- `自然日申述售后单量`
        ,SUM(NVL(t1.appeal_passed_after_sale_ticket_num,0)) AS appeal_passed_after_sale_ticket_num -- `自然日申述通过售后单量`
        ,SUM(NVL(t1.min_appeal_mct_refund_amt,0)) AS min_appeal_mct_refund_amt -- `自然日申诉最低商家赔付金额`
    FROM(
        SELECT
            t1.dt
            ,t1.merchant_id
            ,COUNT(DISTINCT IF(t1.status !="CANCEL", t1.after_sale_order_id, NULL)) AS after_sale_ticket_num -- `自然日售后单数量`
            ,COUNT(DISTINCT IF(
                t1.status != "CANCEL" AND t2.seller_appeal_status IN ("APPEALING", "APPEAL_PASSED", "APPEAL_REJECT"), t1.after_sale_order_id, NULL
            )) AS appeal_after_sale_ticket_num -- `自然日申述售后单量`
            ,COUNT(DISTINCT IF(
                t1.status != "CANCEL" AND t2.seller_appeal_status IN ("APPEAL_PASSED"), t1.after_sale_order_id, NULL
            )) AS appeal_passed_after_sale_ticket_num -- `自然日申述通过售后单量`
            ,ROUND(MIN(IF(
                t1.status != "CANCEL" AND t2.seller_appeal_status IN ("APPEALING", "APPEAL_PASSED", "APPEAL_REJECT"), t2.seller_refund_money, NULL
            )), 2) AS min_appeal_mct_refund_amt -- `自然日申诉最低商家赔付金额`
        FROM datawarehouse_max.dwt_order_after_sale_daily_asc t1
        LEFT JOIN datawarehouse_max.ods_css_demeter_after_sale_order_full t2
            ON t2.dt = MAX_PT('datawarehouse_max.ods_css_demeter_after_sale_order_full')
            AND t2.id = t1.after_sale_order_id
            AND t2.company_id = 871
        LEFT JOIN datawarehouse_max.ods_css_demeter_after_sale_order_refund_asc t3
            ON t3.dt BETWEEN DATEADD(${date_param}, ${start_offset} -8, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")
            AND t3.after_sale_order_id = t2.id
        WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset} -8, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")
            AND t1.mall_id = 871

        GROUP BY t1.dt
            ,t1.merchant_id


    ) t1
    GROUP BY t1.dt
        ,t1.merchant_id
)


SELECT
    t1.dt AS `日期`
    ,t1.merchant_id AS `商家id`
    ,t1.after_sale_ticket_num AS `自然日售后单数量`
    ,t1.appeal_after_sale_ticket_num AS `自然日申述售后单量`
    ,t1.appeal_passed_after_sale_ticket_num AS `自然日申述通过售后单量`
    ,t1.min_appeal_mct_refund_amt AS `自然日申诉最低商家赔付金额`
    ,t1.max_min_appeal_mct_refund_amt_m7dtm1d AS `前7日最高自然日申诉最低商家赔付金额`
    ,t1.after_sale_ticket_num_m7dtm1d AS `前7天自然日售后单数量`
    ,t1.appeal_after_sale_ticket_num_m7dtm1d AS `前7天自然日申述售后单量`
    ,t1.appeal_passed_after_sale_ticket_num_m7dtm1d AS `前7天自然日申述通过售后单量`
    ,t1.after_sale_ticket_num_m6dtcd AS `近7天自然日售后单数量`

FROM(
    SELECT
        t1.dt
        ,t2.merchant_id

        ,MAX(IF(t1.dt=t2.dt, t2.after_sale_ticket_num, 0)) AS after_sale_ticket_num -- `自然日售后单数量`
        ,MAX(IF(t1.dt=t2.dt, t2.appeal_after_sale_ticket_num, 0)) AS appeal_after_sale_ticket_num -- `自然日申述售后单量`
        ,MAX(IF(t1.dt=t2.dt, t2.appeal_passed_after_sale_ticket_num, 0)) AS appeal_passed_after_sale_ticket_num -- `自然日申述通过售后单量`
        ,MAX(IF(t1.dt=t2.dt, t2.min_appeal_mct_refund_amt, 0)) AS min_appeal_mct_refund_amt -- `自然日申诉最低商家赔付金额`

        ,MAX(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt),  -7, "dd") AND DATEADD(DATE(t1.dt),  -1, "dd") AND t2.min_appeal_mct_refund_amt>0, t2.min_appeal_mct_refund_amt, 0
        )) AS max_min_appeal_mct_refund_amt_m7dtm1d -- `前7日最高自然日申诉最低商家赔付金额`

        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt),  -7, "dd") AND DATEADD(DATE(t1.dt),  -1, "dd"), t2.after_sale_ticket_num, 0
        )) AS after_sale_ticket_num_m7dtm1d -- `前7天自然日售后单数量`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt),  -7, "dd") AND DATEADD(DATE(t1.dt),  -1, "dd"), t2.appeal_after_sale_ticket_num, 0
        )) AS appeal_after_sale_ticket_num_m7dtm1d -- `前7天自然日申述售后单量`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt),  -7, "dd") AND DATEADD(DATE(t1.dt),  -1, "dd"), t2.appeal_passed_after_sale_ticket_num, 0
        )) AS appeal_passed_after_sale_ticket_num_m7dtm1d -- `前7天自然日申述通过售后单量`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt, t2.after_sale_ticket_num, 0
        )) AS after_sale_ticket_num_m6dtcd -- `近7天自然日售后单数量`
    FROM dt_range t1
    JOIN temp t2
        ON t2.dummy = t1.dummy
        AND t2.dt BETWEEN DATEADD(DATE(t1.dt), - 7, "dd") AND t1.dt
    WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                    AND DATEADD(${date_param}, ${end_offset}, "dd")
    GROUP BY t1.dt
        ,t2.merchant_id
) t1

;