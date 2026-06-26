WITH mall AS(
    SELECT
        t1.mall_id
        ,t1.mall
    FROM datawarehouse_max.dim_mall_full t1
    -- WHERE t1.mall_id NOT IN (3, 6, 0)
    WHERE t1.mall IN (
        "标果南昌", 
        "标果嘉兴", 
        "标果成都", 
        "标果东莞", 
        "标果贵阳", 
        "标果郑州", 
        "标果西安",
        "标果长沙"
    )
)



-- FIXED: 计算商品日活覆盖率达表的SKU数，添加了过滤条件SKU数大于0
, sku_stat AS(
    SELECT
        t1.dt
        ,t1.mall_id
        ,t1.category_level1_id
        ,t1.category_level4_id

        ,COUNT(
            DISTINCT IF(t1.ordered_store_num / t2.ordered_store_num >= 0.1, t1.sku_id, NULL )
        ) AS acheive_target_sku_num -- `达标sku数`
    FROM datawarehouse_max.ads_pub_mall_sku_stata_daily_asc t1
    LEFT JOIN datawarehouse_max.dws_pub_mall_cate1_base_daily_asc t2
        ON t2.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t2.dt = t1.dt
        AND t2.mall_id = t1.mall_id
        AND t2.category_level1_id = t1.category_level1_id
    WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.mall_id = 871
    GROUP BY t1.dt
        ,t1.mall_id
        ,t1.category_level1_id
        ,t1.category_level4_id


    HAVING acheive_target_sku_num > 0
)


-- SKU 有点更新
,sku_updates AS(
    SELECT
        t1.dt
        ,t1.mall_id
        ,t1.category_level4_name
        ,COUNT(DISTINCT IF(NVL(t1.merit, "") != NVL(t1.merit_m1d, "") AND t1.is_up_today>0, t1.sku_id, NULL )) AS updated_merit_sku_num
        ,COUNT(DISTINCT IF(t1.is_up_today>0, t1.sku_id, NULL)) AS onshelved_sku_num
    FROM(
        SELECT
            t1.dt
            ,t1.mall_id
            ,t1.sku_id
            ,t1.category_level4_name
            ,t1.merit
            ,LAG(t1.merit, 1) OVER(
                PARTITION BY t1.sku_id, t1.mall_id ORDER BY t1.dt
            ) AS merit_m1d
            ,t1.is_up_today
        FROM datawarehouse_max.dim_goods_daily_full t1
        WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset} - 1, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")
            AND t1.mall_id = 871
            AND NVL(t1.is_up_today, 0) + NVL(t1.is_sku_valid, 0) > 0
    ) t1
        GROUP BY t1.dt
        ,t1.mall_id
        ,t1.category_level4_name
)

,base AS(
    SELECT
        t1.dt -- `日期`
        ,DATE_FORMAT(DATE(t1.dt), "yyyy-MM-01") AS dt_month -- `月份`
        ,IF(DATE(LASTDAY(TO_DATE(t1.dt || " 00:00:00", "yyyy-mm-dd hh:mi:ss")))=t1.dt, 1, 0) AS is_last_day -- `是否月末`
        ,t1.mall_name AS mall -- `商城`
        ,t1.mall_id

        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level2_name -- `二级类目名称`
        ,t1.category_level3_name -- `三级类目名称`
        ,t1.category_level4_id -- `四级类目id`
        ,t1.category_level4_name -- `四级类目名称`
        ,t1.delivered_goods_amt -- `送达金额`

        ,t1.final_refund_amt -- `赔付金额`
        ,t1.sku_num_onsale -- `在售sku数`
        ,t1.sku_num_sold -- `动销sku数`

        ,t1.exposed_store_num_old_user -- `老客户曝光店铺数`
        ,t1.ordered_store_num_old_user -- `老客户下单店铺数`
        ,ROUND(NVL(t1.ordered_store_num / t3.ordered_store_num, 0), 5) AS dau_ratio_cat1 -- `一级类目渗透率`
        ,t1.order_cnt -- `明细订单数`

        ,t4.acheive_target_sku_num -- `达标sku数`
        ,ROW_NUMBER() OVER(
            PARTITION BY t1.category_level1_id, t1.category_level1_name, t1.category_level4_name, t1.category_level4_id, t1.dt
            ORDER BY NVL(t1.delivered_goods_amt, 0) DESC
        ) AS delivered_goods_amt_rnk -- `送达金额排名`
        ,ROW_NUMBER() OVER(
            PARTITION BY t1.category_level1_id, t1.category_level1_name, t1.category_level4_name, t1.category_level4_id, t1.dt
            ORDER BY NVL(t1.ordered_store_num / t3.ordered_store_num, 0) DESC
        ) AS ordered_store_ratio_rnk -- `一级类目渗透率排名`

    FROM datawarehouse_max.dws_pub_mall_category_level4_base_daily_asc t1
    JOIN mall t2
        ON t2.mall_id = t1.mall_id
    LEFT JOIN datawarehouse_max.dws_pub_mall_cate1_base_daily_asc t3
        ON t3.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t3.dt = t1.dt
        AND t3.mall_id = t1.mall_id
        AND t3.category_level1_id = t1.category_level1_id

    LEFT JOIN sku_stat t4
        ON t4.dt = t1.dt
        AND t4.mall_id = t1.mall_id
        AND t4.category_level1_id = t1.category_level1_id
        AND t4.category_level4_id = t1.category_level4_id
    WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND ISNOTNULL(t1.category_level4_id)
    QUALIFY t1.mall_id = 871
)


SELECT
	t1.dt AS `日期`
	,t1.dt_month AS `月份`
	,t1.is_last_day AS `是否月末`
	,t1.mall AS `商城`

	,t1.category_level1_name AS `一级类目名称`
	,t1.category_level2_name AS `二级类目名称`
	,t1.category_level3_name AS `三级类目名称`
	,t1.category_level4_id AS `四级类目id`
	,t1.category_level4_name AS `四级类目名称`
	,t1.delivered_goods_amt AS `送达金额`

	,t1.final_refund_amt AS `赔付金额`
	,t1.sku_num_onsale AS `在售sku数`
	,t1.sku_num_sold AS `动销sku数`

	,t1.exposed_store_num_old_user AS `老客户曝光店铺数`
	,t1.ordered_store_num_old_user AS `老客户下单店铺数`
	,t1.dau_ratio_cat1 AS `一级类目渗透率`
	,t1.order_cnt AS `明细订单数`

	,t1.acheive_target_sku_num AS `达标sku数`
	,t1.delivered_goods_amt_rnk AS `送达金额排名`
	,t1.ordered_store_ratio_rnk AS `一级类目渗透率排名`
    ,t2.onshelved_sku_num AS `上架商品数量`
    ,t2.updated_merit_sku_num AS `更新商品卡片数量`

FROM base t1
LEFT JOIN sku_updates t2
    ON t2.dt = t1.dt
    AND t2.category_level4_name = t1.category_level4_name
    AND t2.mall_id = t1.mall_id
WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")

;