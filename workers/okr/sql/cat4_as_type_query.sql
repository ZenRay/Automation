WITH base AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id
        ,t1.category_level1_id -- `一级类目id`
        ,t1.category_level4_id -- `四级类目id`
        ,t1.category_level4_name -- `四级类目名称`

        ,t1.after_sale_type_desc -- `售后类型`
        ,ROUND(t1.final_refund_amt / t2.delivered_goods_amt, 4) AS refund_amt_ratio -- `售后赔付率`
        ,ROW_NUMBER() OVER(
            PARTITION BY t1.dt, t1.category_level1_id, t1.category_level4_id ORDER BY NVL(t1.final_refund_amt / t2.delivered_goods_amt, 0) DESC
        ) AS refund_amt_ratio_rnk -- `售后赔付率排名`
    FROM datawarehouse_max.dws_qa_cate4_after_sale_type_stat_daily_asc t1
    LEFT JOIN datawarehouse_max.dws_pub_mall_category_level4_base_daily_asc t2
        ON t2.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t2.dt = t1.dt
        AND  t2.mall_id = t1.mall_id
        AND t2.category_level1_id = t1.category_level1_id
        AND t2.category_level4_id = t1.category_level4_id
    WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                        AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.mall_id = 871
)


SELECT 
	t1.dt AS  `日期`
	,t1.mall_id AS  `商城id`
	,t1.category_level1_id AS  `一级类目id`
	,t1.category_level4_id AS  `四级类目id`
	,t1.category_level4_name AS  `四级类目名称`

	,t1.after_sale_type_desc AS  `售后类型`
	,t1.refund_amt_ratio AS  `售后赔付率`
	,t1.refund_amt_ratio_rnk AS  `售后赔付率排名`
FROM base t1
;