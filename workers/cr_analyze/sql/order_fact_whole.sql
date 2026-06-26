WITH stores AS(
    SELECT
        t1.dt
        ,t1.customer_store_id
        ,NVL(MAX(IF(t1.mall_id=871, t1.province_id, NULL)), MAX(IF(t1.mall_id <> 871, t1.province_id, NULL))) AS province_id
        ,NVL(MAX(IF(t1.mall_id=871, t1.province_name, NULL)), MAX(IF(t1.mall_id <> 871, t1.province_name, NULL))) AS province_name
        ,NVL(MAX(IF(t1.mall_id=871, t1.city_id, NULL)), MAX(IF(t1.mall_id <> 871, t1.city_id, NULL))) AS city_id
        ,NVL(MAX(IF(t1.mall_id=871, t1.city_name, NULL)), MAX(IF(t1.mall_id <> 871, t1.city_name, NULL))) AS city_name
        ,NVL(MAX(IF(t1.mall_id=871, t1.county_id, NULL)), MAX(IF(t1.mall_id <> 871, t1.county_id, NULL))) AS county_id
        ,NVL(MAX(IF(t1.mall_id=871, t1.county_name, NULL)), MAX(IF(t1.mall_id <> 871, t1.county_name, NULL))) AS county_name

    FROM datawarehouse_max.dim_store_daily_full t1
    WHERE (
        (
            t1.dt BETWEEN "2026-04-01"
                AND CURRENT_DATE()
        ) OR (
            t1.dt BETWEEN "2025-04-01"
                AND "2025-07-31"
        )
        OR (
            t1.dt BETWEEN "2024-04-01"
                AND "2024-07-31"
        )
    )
    GROUP BY t1.dt
        ,t1.customer_store_id
)


,base AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.order_id -- `订单id`
        ,t1.order_item_id -- `明细订单id`

        ,t1.sku_id -- `商品id`
        ,t1.sku_name -- `商品名称`
        ,"水仙芒" AS back_category_name -- `后台类目名称`
        ,CASE
            WHEN 
                t1.merchant_name REGEXP "老顽童"
            THEN "红星老顽童"
            WHEN 
                t1.merchant_name REGEXP "阿玲果行"
            THEN "阿玲果行"
        ELSE t1.merchant_name END AS merchant_name -- `商家名称`
        ,t1.merchant_type_desc -- `商家类型`
        ,t1.settlement_type_desc -- `结算类型`
        ,t1.net_weight -- `净重`
        ,t1.gross_weight -- `毛重`
        ,t2.commission_rate -- `实际抽佣率`
        ,t2.activity_price -- `活动价格`
        ,t2.standard_price -- `平台销售件单价`
        ,t2.unit_price_catty -- `平台销售斤单价`
        ,t2.unit_price_service -- `平台服务费单价`

        ,t1.customer_store_id -- `店铺id`
        ,t3.province_id -- `省id`
        ,t3.province_name -- `省名称`
        ,t3.city_id -- `市id`
        ,CASE
            WHEN 
                t1.city_id = 830300000000
            THEN "萍乡市"
        ELSE t3.city_name 
        END AS city_name -- `市名称`
        ,t3.county_id -- `区县id`
        ,t3.county_name -- `区县名称`
        ,t1.grid_id -- `网格id`
        ,t1.grid_name -- `网格名称`

        ,t1.ordered_goods_num -- `下单数量`
        ,t1.ordered_goods_amt -- `下单金额`
        ,t1.ordered_goods_wgt -- `下单重量`

        ,t1.deliveried_goods_amt -- `送达金额`
        ,t1.deliveried_goods_num -- `送达数量`
        ,t1.deliveried_gross_wgt -- `送达重量`
        ,t1.shipment_amt -- `送达运费`
        ,t1.commission_amt -- `送达抽佣金额`

        ,IF(t1.status NOT IN ("CANCEL"), 1, 0) AS is_valid -- `是否有效订单`

    FROM datawarehouse_max.dwt_order_order_item_daily_asc t1
    LEFT JOIN datawarehouse_max.dwd_order_item_daily_asc t2
        ON (
            (
                t2.dt BETWEEN "2026-04-01"
                    AND CURRENT_DATE()
            ) OR (
                t2.dt BETWEEN "2025-04-01"
                    AND "2025-07-31"
            ) OR (
                t2.dt BETWEEN "2024-04-01"
                    AND "2024-07-31"
            )
        )
        AND t2.order_item_id = t1.order_item_id

    LEFT JOIN stores t3
        ON t3.dt = t1.dt
        AND t3.customer_store_id = t1.customer_store_id

    WHERE (
            (
                t1.dt BETWEEN "2026-04-01"
                    AND CURRENT_DATE()
            ) OR (
                t1.dt BETWEEN "2025-04-01"
                    AND "2025-07-31"
            )OR (
                t1.dt BETWEEN "2024-04-01"
                    AND "2024-07-31"
            )
        )
        AND t1.mall_id = 871
        AND t1.sku_name REGEXP "水仙芒"

)

SELECT
	t1.dt AS `日期`
	,t1.order_id AS `订单id`
	,t1.order_item_id AS `明细订单id`

	,t1.sku_id AS `商品id`
	,t1.sku_name AS `商品名称`
	,t1.back_category_name AS `后台类目名称`
	,t1.merchant_name AS `商家名称`
	,t1.merchant_type_desc AS `商家类型`
	,t1.settlement_type_desc AS `结算类型`
	,t1.net_weight AS `净重`
	,t1.gross_weight AS `毛重`
    ,CASE
        WHEN 
            ISNOTNULL(t2.sku_grade)
        THEN t2.sku_grade
        WHEN 
            INSTR(t1.sku_name, "A级")>0
        THEN "A级"
        WHEN 
            INSTR(t1.sku_name, "B级")>0
        THEN "B级"
        WHEN 
            INSTR(t1.sku_name, "C级")>0
        THEN "C级"
    END AS `商品等级`
    ,CASE
        WHEN 
            ISNOTNULL(t2.producing_area)
        THEN t2.producing_area
        WHEN 
            INSTR(t1.sku_name, "海南")>0
        THEN "海南"
        WHEN 
            INSTR(t1.sku_name, "云南")>0
        THEN "云南"
        WHEN 
            INSTR(t1.sku_name, "广西")>0
        THEN "广西"
    END AS `产地`
    ,CASE
        WHEN 
            ISNOTNULL(t2.packaging_type)
        THEN t2.packaging_type
        WHEN 
            INSTR(t1.sku_name, "纸箱")>0
        THEN "纸箱"
        WHEN 
            INSTR(t1.sku_name, "塑料胶框")>0
        THEN "塑料胶框"
        WHEN 
            INSTR(t1.sku_name, "泡沫箱")>0
        THEN "泡沫箱"
    END AS `包装类型`
    ,CASE
        WHEN 
            ISNOTNULL(t2.single_fruit_size)
        THEN t2.single_fruit_size
        WHEN 
            INSTR(t1.sku_name, "特大果")>0
        THEN "特大果"
        WHEN 
            INSTR(t1.sku_name, "大果")>0
        THEN "大果"
        WHEN 
            INSTR(t1.sku_name, "中大果")>0
        THEN "中大果"
        WHEN 
            INSTR(t1.sku_name, "中小果")>0
        THEN "中小果"
        WHEN 
            INSTR(t1.sku_name, "中果")>0
        THEN "中果"
        WHEN 
            INSTR(t1.sku_name, "小果")>0
        THEN "小果"
    END AS `单果大小`
    ,t2.color_code AS `色号`
	,ROUND(IF(t1.commission_rate<1, t1.commission_rate, t1.commission_rate / 100), 4) AS `实际抽佣率`


    ,ROUND(t1.standard_price / (1 + IF(t1.commission_rate<1, t1.commission_rate, t1.commission_rate / 100)), 2) AS `商家供货件单价`
    ,ROUND(t1.unit_price_catty / (1 + IF(t1.commission_rate<1, t1.commission_rate, t1.commission_rate / 100)), 2) AS `商家供货斤单价`
    ,t1.activity_price AS `活动价格`
	,t1.standard_price AS `平台销售件单价`
	,t1.unit_price_catty AS `平台销售斤单价`
	,t1.unit_price_service AS `平台服务费单价`

	,t1.customer_store_id AS `店铺id`
	,t1.province_id AS `省id`
	,t1.province_name AS `省名称`
	,t1.city_id AS `市id`
	,t1.city_name AS `市名称`
	,t1.county_id AS `区县id`
	,t1.county_name AS `区县名称`
	,t1.grid_id AS `网格id`
	,t1.grid_name AS `网格名称`

	,t1.ordered_goods_num AS `下单数量`
	,t1.ordered_goods_amt AS `下单金额`
	,t1.ordered_goods_wgt AS `下单重量`

	,t1.deliveried_goods_amt AS `送达金额`
	,t1.deliveried_goods_num AS `送达数量`
	,t1.deliveried_gross_wgt AS `送达重量`
	,t1.shipment_amt AS `送达运费`
	,t1.commission_amt AS `送达抽佣金额`

	,t1.is_valid AS `是否有效订单`
FROM base t1
LEFT JOIN datawarehouse_max.dim_goods_extra_info_daily_full t2
    ON (
            (
                t2.dt BETWEEN "2026-04-01"
                    AND CURRENT_DATE()
            ) OR (
                t2.dt BETWEEN "2025-04-01"
                    AND "2025-07-31"
            )OR (
                t1.dt BETWEEN "2024-04-01"
                    AND "2024-07-31"
            )
        )
    AND t2.dt = t1.dt
    AND t2.sku_id = t1.sku_id
    AND t2.mall_id = t1.mall_id
;