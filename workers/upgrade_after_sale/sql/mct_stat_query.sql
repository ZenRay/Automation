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

        ,SUM(NVL(t1.after_sale_ticket_num, 0)) AS after_sale_ticket_num -- `售后单数量`
    FROM(
        SELECT
            t1.dt
            ,t1.merchant_id
            ,COUNT(DISTINCT IF(t1.status !="CANCEL", t1.after_sale_order_id, NULL)) AS after_sale_ticket_num -- `售后单数量`
        FROM datawarehouse_max.dwt_order_after_sale_daily_asc t1
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
    ,t1.after_sale_ticket_num AS `售后单数量`
    ,t1.after_sale_ticket_num_m7dtm1d AS `前7天售后单数量`
    ,t1.after_sale_ticket_num_m6dtcd AS `近7天售后单数量`
FROM(
    SELECT
        t1.dt
        ,t2.merchant_id

        ,MAX(IF(t1.dt=t2.dt, t2.after_sale_ticket_num, 0)) AS after_sale_ticket_num -- `售后单数量`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt),  -7, "dd") AND DATEADD(DATE(t1.dt),  -1, "dd"), t2.after_sale_ticket_num, 0
        )) AS after_sale_ticket_num_m7dtm1d -- `前7天售后单数量`
        ,SUM(IF(
            t2.dt BETWEEN DATEADD(DATE(t1.dt),  -6, "dd") AND t1.dt, t2.after_sale_ticket_num, 0
        )) AS after_sale_ticket_num_m6dtcd -- `近7天售后单数量`
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