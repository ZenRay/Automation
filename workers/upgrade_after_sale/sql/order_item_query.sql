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
        ,NVL(MAX(IF(t1.mall_id=871, t1.grid_id, NULL)), MAX(IF(t1.mall_id <> 871, t1.grid_id, NULL))) AS grid_id
        ,NVL(MAX(IF(t1.mall_id=871, t1.grid_name, NULL)), MAX(IF(t1.mall_id <> 871, t1.grid_name, NULL))) AS grid_name
    FROM datawarehouse_max.dim_store_daily_full t1
    WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                    AND DATEADD(${date_param}, ${end_offset}, "dd")
    GROUP BY t1.dt
        ,t1.customer_store_id
)


,base AS(
    SELECT
        t1.dt -- `日期`
        ,t1.order_item_id -- `明细订单id`
        ,t1.order_item_no -- `明细订单编号`
        ,t1.order_id -- `订单id`
        ,t1.order_no -- `订单编号`

        ,t1.goods_id AS sku_id -- `商品id`
        ,t4.sku_name -- `商品名称`
        ,CASE
            WHEN 
                INSTR(t4.merchant_name, "老顽童") > 0
            THEN "红星老顽童"
            WHEN 
                INSTR(t4.merchant_name, "阿玲果行") > 0
            THEN "阿玲果行"
            ELSE t4.merchant_name
        END AS merchant_name -- `商家名称`


        ,t4.back_category_id -- `后台类目id`
        ,t4.back_category_name -- `后台类目名称`
        ,t5.sku_grade -- `等级`
        ,t5.producing_area -- `产地`
        ,t5.packaging_type -- `包装类型`
        ,t2.customer_store_id -- `店铺id`
        ,t3.province_id -- `省id`
        ,t3.province_name -- `省名称`
        ,t3.city_id -- `市id`
        ,t3.city_name -- `市名称`
        ,t3.county_id -- `区县id`
        ,t3.county_name -- `区县名称`
        ,t3.grid_id -- `网格id`
        ,t3.grid_name -- `网格名称`
        ,t1.goods_count AS ordered_goods_num -- `下单数量`
        ,t1.estimate_amount AS ordered_goods_amt -- `下单金额`
        ,t1.real_amount AS deliveryed_goods_amt -- `送货金额`
        ,t1.real_count AS deliveryed_goods_num -- `送货数量`
        ,t1.payment_amount -- `实付金额`

        ,t1.mall_id -- `商城id`
        ,IF(ISNOTNULL(t6.grid_id), "代理人区域", "直营区域") AS operation_region_type -- `运营区域类型`
    FROM datawarehouse_max.dwd_order_item_daily_asc t1
    LEFT JOIN datawarehouse_max.dwd_order_daily_asc t2
        ON t2.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t2.order_id = t1.order_id
    LEFT JOIN stores t3
        ON t3.customer_store_id = t2.customer_store_id
        AND t3.dt = t1.dt

    LEFT JOIN datawarehouse_max.dim_goods_daily_full t4
        ON t4.dt BETWEEN DATEADD(${date_param}, ${start_offset} - 30, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t4.dt = t1.dt
        AND t4.sku_id = t1.goods_id
    LEFT JOIN datawarehouse_max.dim_goods_extra_info_daily_full t5
        ON t5.dt BETWEEN DATEADD(${date_param}, ${start_offset} - 30, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t5.dt = t1.dt
        AND t5.sku_id = t1.goods_id
        AND t5.mall_id = t1.mall_id

    LEFT JOIN datawarehouse_max.ods_agent_service_agent_agent_grid_full t6
        ON t6.dt BETWEEN DATEADD(${date_param}, ${start_offset} - 30, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t6.company_id = t1.mall_id
        AND t6.dt = t1.dt
        AND t6.grid_id = t3.grid_id
        AND t6.status = 1    
    WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.mall_id = 871
        AND t1.status != "CANCEL"
)



SELECT
	t1.dt AS `日期`
    ,t1.order_item_id AS `明细订单id`
	,t1.order_item_no AS `明细订单编号`
	,t1.order_id AS `订单id`
	,t1.order_no AS `订单编号`

	,t1.sku_id AS `商品id`
	,t1.sku_name AS `商品名称`
	,t1.merchant_name AS `商家名称`


	,t1.back_category_id AS `后台类目id`
	,t1.back_category_name AS `后台类目名称`
	,t1.sku_grade AS `等级`
	,t1.producing_area AS `产地`
	,t1.packaging_type AS `包装类型`
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
	,t1.deliveryed_goods_amt AS `送货金额`
	,t1.deliveryed_goods_num AS `送货数量`
	,t1.payment_amount AS `实付金额`

	,t1.mall_id AS `商城id`
	,t1.operation_region_type AS `运营区域类型`
FROM base t1
;