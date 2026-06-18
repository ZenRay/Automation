SELECT
    DATE(t1.dt) AS `日期`
    ,t1.mall_id AS `商城id`
    ,t1.sku_id AS `商品id`
    ,t1.sku_code AS `商品编码`
    ,t1.sku_name AS `商品名称`
    ,t2.sku_grade AS `商品等级`
    ,t2.producing_area AS `产地`
    ,t2.packaging_type AS `包装类型`
    ,t2.single_fruit_size AS `单果大小`
    ,t2.color_code AS `色号`
    ,t2.sku_quantity AS `商品头数`
    ,t1.merchant_id AS `商家id`
    ,t1.merchant_name AS `商家名称`
    ,t1.merchant_type_desc AS `商家类型`
    ,t1.back_category_id AS `后台类目id`
    ,t1.back_category_name AS `后台类目名称`
    ,t1.net_weight AS `净重`
    ,t1.gross_weight AS `毛重`

    ,t1.unit_price_catty AS `非试验区域平台销售斤单价`
    ,t1.standard_price AS `非试验区域平台销售件单价`
    ,t1.commision_rate AS `非试验区域抽佣率`
    ,ROUND(
        t1.unit_price_catty / (1 + t1.commision_rate), 2
    ) AS `非试验区域商家供货斤单价`
    ,ROUND(
        t1.standard_price / (1 + t1.commision_rate), 2
    ) AS `非试验区域商家供货件单价`
    ,NVL(t1.is_up_today, 0) AS `是否当日上架`

    ,CASE
        WHEN
            t1.sku_id = 20519020 AND t1.dt >= "2026-06-18"
        THEN 1
    ELSE 0 END AS `是否试验周期`
    ,CASE
        WHEN
            t1.sku_id = 20519020
        THEN 1
    ELSE 0 END AS `是否试验商品`
    -- ,COUNT(1) OVER(PARTITION BY t1.dt, t1.sku_id) AS cnt
FROM datawarehouse_max.dim_goods_daily_full t1
LEFT JOIN datawarehouse_max.dim_goods_extra_info_daily_full t2
    ON t2.dt = DATEADD(DATE(MAX_PT("datawarehouse_max.dim_goods_extra_info_daily_full")), -1, "dd")
    AND t2.sku_id = t1.sku_id
WHERE t1.dt = DATEADD(DATE(MAX_PT("datawarehouse_max.dim_goods_daily_full")), -1, "dd")
    AND t1.mall_id = 871
    AND t1.back_category_id = 2407
    AND NVL(t1.is_up_today,0) + NVL(t1.is_sku_valid, 0) > 0
;