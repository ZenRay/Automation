SELECT
    t1.sku_id AS `商品id`
    ,t1.merchant_id AS `商家id`
    ,t1.category_level1_id AS `一级类目id`
    ,t1.category_level1_name AS `一级类目名称`
    ,t1.category_level4_id AS `四级类目id`
    ,t1.category_level4_name AS `四级类目名称`
FROM datawarehouse_max.dim_goods_daily_full t1
WHERE t1.dt = MAX_PT('datawarehouse_max.dim_goods_daily_full')
    AND t1.mall_id = 871

    -- AND NOT (
    --     NVL(t1.is_sku_valid, 0)=0 AND DATE(t1.update_time) >= DATEADD(CURRENT_DATE(), -20, "dd")
    -- )
    AND t1.category_level1_name = "水果"
;