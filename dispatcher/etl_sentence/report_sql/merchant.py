#coding:utf8
"""summary_line

Keyword arguments:
argument -- description
Return: return_description
"""
mct_roi_report_sentence = """
WITH base AS(

    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.mall -- `商城`
        ,t1.customer_store_id
        ,t1.ssp_activity_tag
        ,t1.sku_id
        ,t1.sku_name
        ,t1.merchant_id
        ,t1.merchant_name
        ,t1.category_level1_id -- `一级类目id`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_id -- `四级类目id`
        ,t1.category_level4_name -- `四级类目名称`
        
        ,MAX(t1.ordered_goods_num) AS ordered_goods_num -- `下单商品数量`
        ,MAX(t1.delivered_goods_num) AS delivered_goods_num -- `送货商品数量`
        ,MAX(t1.delivered_goods_amt) AS delivered_goods_amt -- `送货金额`
        ,MAX(t1.after_sale_applied_num) AS after_sale_applied_num -- `售后申请数量`
        ,MAX(t1.after_sale_refund_amt) AS after_sale_refund_amt -- `售后赔付金额`

        ,MAX(t1.exposed_cnt) AS exposed_cnt -- `曝光次数`
        ,MAX(t1.clicked_cnt) AS clicked_cnt -- `点击次数`
        ,MAX(t1.added_cnt) AS added_cnt -- `加购次数`
    FROM(
        SELECT
            t1.dt -- `日期`
            ,t1.mall_id -- `商城id`
            ,t1.mall -- `商城`
            ,t1.customer_store_id
            ,t1.ssp_activity_tag
            ,t1.sku_id
            ,t1.sku_name
            ,t1.merchant_id
            ,t1.merchant_name
            ,t1.category_level1_id -- `一级类目id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`


            ,t1.ordered_goods_num -- `下单商品数量`
            ,t1.delivered_goods_num -- `送货商品数量`
            ,t1.delivered_goods_amt -- `送货金额`
            ,t1.after_sale_applied_num -- `售后申请数量`
            ,t1.after_sale_refund_amt -- `售后赔付金额`

            ,NULL AS exposed_cnt -- `曝光次数`
            ,NULL AS clicked_cnt -- `点击次数`
            ,NULL AS added_cnt -- `加购次数`
        FROM changsha_trade_fact_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -15, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
            AND t1.mall_id = 871
            AND t1.dimention_type = "商城+门店+SKU"


        UNION 
        SELECT
            t1.dt -- `日期`
            ,t1.mall_id -- `商城id`
            ,t1.mall -- `商城`
            ,t1.customer_store_id
            ,t1.ssp_activity_tag
            ,t1.sku_id
            ,t1.sku_name
            ,t1.merchant_id
            ,t1.merchant_name
            ,t1.category_level1_id -- `一级类目id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`

            ,NULL AS ordered_goods_num -- `下单商品数量`
            ,NULL AS delivered_goods_num -- `送货商品数量`
            ,NULL AS delivered_goods_amt -- `送货金额`
            ,NULL AS after_sale_applied_num -- `售后申请数量`
            ,NULL AS after_sale_refund_amt -- `售后赔付金额`

            ,t1.exposed_cnt -- `曝光次数`
            ,t1.clicked_cnt -- `点击次数`
            ,t1.added_cnt -- `加购次数`
        FROM changsha_flow_fact_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -15, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
            AND t1.mall_id = 871
            AND t1.dimention_type = "商城+门店+SKU"

    ) t1
    GROUP BY t1.dt -- `日期`
            ,t1.mall_id -- `商城id`
            ,t1.mall -- `商城`
            ,t1.customer_store_id
            ,t1.ssp_activity_tag
            ,t1.sku_id
            ,t1.sku_name
            ,t1.merchant_id
            ,t1.merchant_name
            ,t1.category_level1_id -- `一级类目id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`

)


,stats AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.mall -- `商城`


        ,IF(
            GROUPING(t1.sku_id)==0, t1.sku_id, -99999L
        ) AS sku_id -- '商品ID' BIGINT
        ,IF(
            GROUPING(t1.sku_name)==0, t1.sku_name, "合计"
        ) AS sku_name -- '商品名称' STRING
        ,IF(
            GROUPING(t1.category_level4_id)==0, t1.category_level4_id, -99999L
        ) AS category_level4_id -- '四级类目ID' BIGINT
        ,IF(
            GROUPING(t1.category_level4_name)==0, t1.category_level4_name, "合计"
        ) AS category_level4_name -- '四级类目名称' STRING
        ,IF(
            GROUPING(t1.category_level1_id)==0, t1.category_level1_id, -99999L
        ) AS category_level1_id -- '一级类目ID' BIGINT
        ,IF(
            GROUPING(t1.category_level1_name)==0, t1.category_level1_name, "合计"
        ) AS category_level1_name -- '一级类目名称' STRING

        ,IF(
            GROUPING(t1.merchant_id)==0, t1.merchant_id, -99999L
        ) AS merchant_id -- '商家ID' BIGINT
        ,IF(
            GROUPING(t1.merchant_name)==0, t1.merchant_name, "合计"
        ) AS merchant_name -- '商家名称' STRING

        ,MAX(NVL(t2.is_up_today,0)) AS is_up_today -- `当日是否上架`
        ,MAX(t2.mct_supplied_price_base) AS mct_supplied_price_base -- `商家基准供货价`
        ,NVL(
            MAX(t2.mct_supplied_price_new), MAX(t2.mct_supplied_price_base)
        ) AS mct_supplied_price_new -- `商家新客供货价`
        ,MAX(t3.old_pond_num) AS os_pool_num -- `老客户客户池`
        ,MAX(t3.new_pond_num) AS ns_pool_num -- `新客户客户池`
        ,SUM(t1.ordered_goods_num) AS ordered_goods_num -- `下单商品数量`
        ,SUM(t1.delivered_goods_num) AS delivered_goods_num -- `送货商品数量`
        
        ,SUM(t1.after_sale_applied_num) AS after_sale_applied_num -- `售后申请数量`
        ,SUM(t1.after_sale_refund_amt) AS after_sale_refund_amt -- `售后赔付金额`
        ,CAST(
            SUM(t1.after_sale_applied_num) / SUM(t1.delivered_goods_num)
                AS DECIMAL(10,4)
        ) AS after_sale_applied_ratio -- `售后提交率`
        ,CAST(
            SUM(t1.after_sale_refund_amt) / SUM(t1.delivered_goods_amt)
                AS DECIMAL(10,4)
        ) AS after_sale_refund_ratio -- `售后赔付率`
        ,COUNT(DISTINCT IF(t1.exposed_cnt>0, t1.customer_store_id, NULL)) AS exposed_store_num -- `曝光店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_cnt>0 AND t1.ssp_activity_tag="NEW", t1.customer_store_id, NULL
        )) AS exposed_new_store_num -- `新客户曝光店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_cnt>0 AND t1.ssp_activity_tag="OLD", t1.customer_store_id, NULL
        )) AS exposed_old_store_num -- `老客户曝光店铺数`
        ,COUNT(DISTINCT IF(
            t1.ordered_goods_num>0, t1.customer_store_id, NULL
        )) AS ordered_store_num -- `下单店铺数`
        ,COUNT(DISTINCT IF(
            t1.ordered_goods_num>0 AND t1.ssp_activity_tag="NEW", t1.customer_store_id, NULL
        )) AS ordered_new_store_num -- `新客户下单店铺数`
        ,COUNT(DISTINCT IF(
            t1.ordered_goods_num>0 AND t1.ssp_activity_tag="OLD", t1.customer_store_id, NULL
        )) AS ordered_old_store_num -- `老客户下单店铺数`
        ,SUM(IF(
            t1.ssp_activity_tag="NEW", t1.ordered_goods_num, NULL 
        )) AS ordered_ns_goods_num -- `新客户下单商品数量`
        ,SUM(IF(
            t1.ssp_activity_tag="OLD", t1.ordered_goods_num, NULL 
        )) AS ordered_os_goods_num -- `老客户下单商品数量`
        ,SUM(IF(
            t1.ssp_activity_tag="LOSS", t1.ordered_goods_num, NULL 
        )) AS ordered_ls_goods_num -- `召回客户下单商品数量`

        ,SUM(t1.delivered_goods_amt) AS delivered_goods_amt -- `送货商品金额`
        ,SUM(IF(
            t1.ssp_activity_tag="NEW", t1.delivered_goods_amt, NULL 
        )) AS delivered_ns_goods_amt -- `新客户送货商品金额`
        ,SUM(IF(
            t1.ssp_activity_tag="OLD", t1.delivered_goods_amt, NULL 
        )) AS delivered_os_goods_amt -- `老客户送货商品金额`
        ,SUM(IF(
            t1.ssp_activity_tag="LOSS", t1.delivered_goods_amt, NULL 
        )) AS delivered_ls_goods_amt -- `召回客户送货商品金额`

        -- ,SUM(t1.exposed_cnt) AS exposed_cnt -- `曝光次数`
        -- ,SUM(t1.clicked_cnt) AS clicked_cnt -- `点击次数`
        -- ,SUM(t1.added_cnt) AS added_cnt -- `加购次数`
    FROM base t1
    LEFT JOIN changsha_dim_sku_daily_asc t2
        ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -15, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t2.dt = t1.dt
        AND t2.mall_id = t1.mall_id
        AND t2.sku_id = t1.sku_id
    
    LEFT JOIN datawarehouse_max.ads_changsha_sku_dashboard_daily_asc t3
        ON t3.dt BETWEEN DATEADD(CURRENT_DATE(), -15, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t3.dt = t1.dt
        AND t1.mall_id = 871
        AND t3.goods_id = t1.sku_id

    GROUP BY t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.mall -- `商城`
    ,GROUPING SETS (
        -- 全商城
        ()
        -- 商品
        ,(
            t1.sku_id -- `商品id`
            ,t1.sku_name -- `商品名称`
            ,t1.merchant_id -- `商家id`
            ,t1.merchant_name -- `商家名称`
            ,t1.category_level1_id -- `一级类目id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`
        )
        -- 商家四级类目
        ,(
            t1.merchant_id -- `商家id`
            ,t1.merchant_name -- `商家名称`
            ,t1.category_level1_id -- `一级类目id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`
        )
        -- 四级类目
        ,(
            t1.category_level1_id -- `一级类目id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`
        )
    )
        
)


,result AS(

    SELECT 
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.mall -- `商城`


        ,t1.sku_id -- '商品ID' BIGINT
        ,t1.sku_name -- '商品名称' STRING
        ,t1.category_level4_id -- '四级类目ID' BIGINT
        ,t1.category_level4_name -- '四级类目名称' STRING
        ,t1.category_level1_id -- '一级类目ID' BIGINT
        ,t1.category_level1_name -- '一级类目名称' STRING

        ,t1.merchant_id -- '商家ID' BIGINT
        ,t1.merchant_name -- '商家名称' STRING

        ,IF(t1.sku_name !="合计", t1.is_up_today, NULL) AS is_up_today -- `当日是否上架`
        ,IF(t1.sku_name !="合计", t1.mct_supplied_price_base, NULL) AS mct_supplied_price_base -- `商家基准供货价`
        ,IF(t1.sku_name !="合计", t1.mct_supplied_price_new, NULL) AS mct_supplied_price_new -- `商家新客供货价`
        ,IF(t1.sku_name !="合计", t1.os_pool_num, NULL) AS os_pool_num -- `老客户客户池`
        ,IF(t1.sku_name !="合计", t1.ns_pool_num, NULL) AS ns_pool_num -- `新客户客户池`
        ,t1.ordered_goods_num -- `下单商品数量`
        ,t1.delivered_goods_num -- `送货商品数量`
        
        ,t1.after_sale_applied_num -- `售后申请数量`
        ,t1.after_sale_refund_amt -- `售后赔付金额`
        ,IF(t1.delivered_goods_num >= 0, NVL(t1.after_sale_applied_ratio,0), NULL) AS after_sale_applied_ratio -- `售后提交率`
        ,IF(t1.delivered_goods_num >= 0, NVL(t1.after_sale_refund_ratio,0), NULL) AS after_sale_refund_ratio -- `售后赔付率`
        ,t1.exposed_store_num -- `曝光店铺数`
        ,t1.exposed_new_store_num -- `新客户曝光店铺数`
        ,t1.exposed_old_store_num -- `老客户曝光店铺数`
        ,t1.ordered_store_num -- `下单店铺数`
        ,t1.ordered_new_store_num -- `新客户下单店铺数`
        ,t1.ordered_old_store_num -- `老客户下单店铺数`
        ,CAST(
            t1.ordered_old_store_num / t1.exposed_old_store_num AS DECIMAL(10,4)
        ) AS exposed2ordered_old_store_ratio -- `老客户曝光转化率`

        ,CAST(
            t1.ordered_new_store_num / t1.exposed_new_store_num AS DECIMAL(10,4)
        ) AS exposed2ordered_new_store_ratio -- `新客户曝光转化率`
        ,t1.ordered_ns_goods_num -- `新客户下单商品数量`
        ,t1.ordered_os_goods_num -- `老客户下单商品数量`
        ,t1.ordered_ls_goods_num -- `召回客户下单商品数量`

        ,t1.delivered_goods_amt -- `送货商品金额`
        ,t1.delivered_ns_goods_amt -- `新客户送货商品金额`
        ,t1.delivered_os_goods_amt -- `老客户送货商品金额`
        ,t1.delivered_ls_goods_amt -- `召回客户送货商品金额`

        ,t2.target_goods_num -- `四级类目目标件数`
        ,CAST(
            IF(t2.target_goods_num>0, t1.ordered_goods_num / t2.target_goods_num, NULL) AS DECIMAL(10,4)
        ) AS achieved_target_goods_ratio -- `四级类目目标件数达成率`
        ,CASE
            WHEN
                ALL_MATCH(
                    ARRAY(t1.sku_name, t1.category_level4_name, t1.category_level1_name, t1.merchant_name), item -> item ="合计"
                )
            THEN "商城"
            WHEN
                ALL_MATCH(
                    ARRAY(t1.sku_name, t1.category_level4_name, t1.category_level1_name, t1.merchant_name), item -> item !="合计"
                )
            THEN "商品"
            WHEN
                ALL_MATCH(
                    ARRAY( t1.category_level4_name, t1.category_level1_name, t1.merchant_name), item -> item !="合计"
                ) AND t1.sku_name = "合计"
            THEN "商家四级类目"
            WHEN
                ALL_MATCH(
                    ARRAY( t1.category_level4_name, t1.category_level1_name), item -> item !="合计"
                ) AND t1.sku_name = "合计" AND t1.merchant_name = "合计"
            THEN "四级类目"
        ELSE "其他"
        END AS dimention_type -- `维度类型`
        ,DATEDIFF(
            DATE(MAX(t1.dt) OVER())
            ,DATE(MAX(t1.dt) FILTER(WHERE t1.is_up_today>0 AND t1.sku_name != "合计")OVER(PARTITION BY t1.mall_id, t1.sku_id))
            ,"dd"
        ) + 1 AS up_gaps -- `最近下单间隔天数`
    FROM stats t1
    LEFT JOIN datawarehouse_max.ads_changsha_cat4_dashboard_daily_asc t2
        ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -15, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t2.dt = t1.dt
        AND t1.mall_id = 871
        AND t2.category_level4_id = t1.category_level4_id
        AND t1.sku_name = "合计"
        AND t1.merchant_name = "合计"
)


SELECT 
	t1.dt AS `日期`
	,t1.mall_id AS `商城id`
	,t1.mall AS `商城`


	,t1.sku_id AS `商品ID`
	,t1.sku_name AS `商品名称`
	,t1.category_level4_id AS `四级类目ID`
	,t1.category_level4_name AS `四级类目名称`
	,t1.category_level1_id AS `一级类目ID`
	,t1.category_level1_name AS `一级类目名称`

	,t1.merchant_id AS `商家ID`
	,t1.merchant_name AS `商家名称`

	,t1.is_up_today AS `当日是否上架`
	,t1.mct_supplied_price_base AS `商家基准供货价`
	,t1.mct_supplied_price_new AS `商家新客供货价`
	,t1.os_pool_num AS `老客户客户池`
	,t1.ns_pool_num AS `新客户客户池`
	,t1.ordered_goods_num AS `下单商品数量`
	,t1.delivered_goods_num AS `送货商品数量`
	
	,t1.after_sale_applied_num AS `售后申请数量`
	,t1.after_sale_refund_amt AS `售后赔付金额`
	,t1.after_sale_applied_ratio AS `售后提交率`
	,t1.after_sale_refund_ratio AS `售后赔付率`
	,t1.exposed_store_num AS `曝光店铺数`
	,t1.exposed_new_store_num AS `新客户曝光店铺数`
	,t1.exposed_old_store_num AS `老客户曝光店铺数`
	,t1.ordered_store_num AS `下单店铺数`
	,t1.ordered_new_store_num AS `新客户下单店铺数`
	,t1.ordered_old_store_num AS `老客户下单店铺数`
	,t1.exposed2ordered_old_store_ratio AS `老客户曝光转化率`

	,t1.exposed2ordered_new_store_ratio AS `新客户曝光转化率`
	,t1.ordered_ns_goods_num AS `新客户下单商品数量`
	,t1.ordered_os_goods_num AS `老客户下单商品数量`
	,t1.ordered_ls_goods_num AS `召回客户下单商品数量`

	,t1.delivered_goods_amt AS `送货商品金额`
	,t1.delivered_ns_goods_amt AS `新客户送货商品金额`
	,t1.delivered_os_goods_amt AS `老客户送货商品金额`
	,t1.delivered_ls_goods_amt AS `召回客户送货商品金额`

	,t1.target_goods_num AS `四级类目目标件数`
	,t1.achieved_target_goods_ratio AS `四级类目目标件数达成率`
	,t1.dimention_type AS `维度类型`
    ,t1.up_gaps AS `最近下单间隔天数`
    
FROM result t1
WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -9, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
    AND (
        -- t1.up_gaps <= 10 OR t1.sku_name != "合计"
        (t1.dimention_type="商品" AND NVL(t1.up_gaps, -2) BETWEEN 0 AND 10 AND t1.category_level1_name LIKE "%水果%")
        OR (t1.dimention_type != "商品" AND t1.category_level1_name LIKE "%水果%")
        OR (t1.dimention_type = "商城")
    )
    
;
"""
