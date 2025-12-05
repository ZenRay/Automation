#coding:utf-8
"""
Maxcompute Dimention Tag of Store and Goods pair Table ETL Sentence
1. 商品和门店标签表 dim_sku_store_tags_sentence: changsha_dim_sku_store_tag_daily_asc
"""

dim_sku_store_tags_sentence = """

-- 1. 基础信息： 商城维度信息 和门店商品标签、前台类目信息
WITH mall AS(
    SELECT
        t1.mall_id
        ,t1.mall
    FROM datawarehouse_max.dim_mall_full t1
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


,base AS(
    -- FIXED: 保持和推送线上一致的数据使用 ADS 的数据，其他的取其他商城的数据用 DWS 的数据
    SELECT
        t1.dt
        ,t1.mall_id
        ,t1.sku_id
        ,t1.customer_store_id
        ,TOUPPER(t1.customer_store_tag) AS ssp_activity_tag -- '门店商品活跃度标签' STRING
        ,ROW_NUMBER() OVER(PARTITION BY t1.dt, t1.mall_id, t1.customer_store_id, t1.sku_id ORDER BY t1.orders) AS rnk
    FROM(
        -- FIXED: 保持和推送线上一致的数据使用 ADS 的数据，其他的取其他商城的数据用 DWS 的数据
        SELECT
            DATEADD(TO_DATE(t1.dt), 1, "dd") AS dt
            ,t1.mall_id
            ,t1.sku_id
            ,t1.customer_store_id
            ,DECODE(
                t1.customer_store_tag
                ,"NEW_CUSTOMER", "NEW"
                ,"OLD_CUSTOMER", "OLD"
                ,"RECALL_CUSTOMER","LOSS"
                ,t1.customer_store_tag
            ) AS customer_store_tag
            ,1 AS orders
        FROM datawarehouse_max.ads_mct_cs_mall_sku_customer_tag_daily_asc t1

        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(),-1 + -3, "dd")
                    AND DATEADD(CURRENT_DATE(), 0, "dd")
            

        UNION
        SELECT
            DATEADD(TO_DATE(t1.dt), 1, "dd") AS dt
            ,t1.mall_id
            ,t1.sku_id
            ,t1.customer_store_id
            ,t1.customer_store_tag
            ,2 AS orders
        -- FIXED: 取其他省区的数据
        FROM datawarehouse_max.dws_pub_mall_sku_customer_tag_daily_asc t1
        JOIN mall t2
            ON t2.mall_id = t1.mall_id
        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(),-1 + -3, "dd")
                    AND DATEADD(CURRENT_DATE(), 0, "dd")
    ) t1
    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(),-1 + -3, "dd")
                    AND DATEADD(CURRENT_DATE(), 0, "dd")
    QUALIFY rnk = 1
)



INSERT OVERWRITE TABLE changsha_dim_sku_store_tag_daily_asc PARTITION(dt)
SELECT
    t1.mall_id -- '商城id' BIGINT
    ,t1.sku_id -- '商品id' BIGINT
    ,t1.customer_store_id -- '门店id' BIGINT
    ,t1.ssp_activity_tag -- '门店商品活跃度标签' STRING
    ,t1.dt
FROM base t1
WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
                AND DATEADD(CURRENT_DATE(), 0, "dd")
    AND t1.rnk = 1
;
"""
