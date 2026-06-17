WITH upgrade_stat AS(
    SELECT
        t2.dt -- `日期`
        ,t1.mall_id
        ,t3.category_level1_id
        ,t3.category_level4_id
        ,t1.customer_store_id

        ,COUNT(DISTINCT t1.after_sale_upgrade_no) AS as_upgrade_item_ticket -- `升级售后单量`
        ,SUM(NVL(t1.confirmed_amount, 0) + NVL(t1.fruit_grain_payment_amount, 0)) AS total_refund_amount_upgrade -- `升级售后总赔付金额`
        ,SUM(NVL(t1.fruit_grain_payment_amount, 0)) AS platform_refund_amount_upgrade -- `升级售后平台赔付金额`
    FROM datawarehouse_max.dwd_order_after_sale_upgrade_daily_full t1
    JOIN datawarehouse_max.dwd_order_item_daily_asc t2
        ON t2.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                    AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t2.order_item_id = t1.order_item_id
    LEFT JOIN datawarehouse_max.dim_goods_daily_full t3
        ON t3.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                    AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t3.dt = t2.dt
        AND t3.mall_id = t1.mall_id
        AND t3.sku_id = t1.sku_id
    WHERE t1.dt = MAX_PT("datawarehouse_max.dwd_order_after_sale_upgrade_daily_full")
        AND t1.mall_id = 871
    GROUP BY t2.dt -- `日期`
        ,t1.mall_id
        ,t3.category_level1_id
        ,t3.category_level4_id
        ,t1.customer_store_id
)


,base AS(

    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.mall_name AS mall -- `商城`
        ,t1.grid_id -- `网格id`
        ,t1.grid_name -- `网格名称`
        ,IF(ISNOTNULL(t2.grid_id), "代理人区域", "直营区域") AS grid_operate_type -- `网格运营类型`
        ,t1.bd_id -- `bdid`
        ,t1.bd_name -- `bd姓名`
        ,t1.customer_store_id -- `店铺id`
        ,t0.province_id -- `省id`
        ,t0.province_name -- `省名称`
        ,t0.city_id -- `市id`
        ,t0.city_name -- `市名称`
        ,t0.county_id -- `区县id`
        ,t0.county_name -- `区县名称`
        ,t1.category_level1_id -- `一级类目id`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_id -- `四级类目id`
        ,t1.category_level4_name -- `四级类目名称`

        -- 蔬菜和干货下单
        ,IF(t1.category_level1_name IN ("蔬菜", "干货") AND t1.ordered_goods_num>0, 1, 0) AS is_vegitable -- `是否蔬菜/干货类目`

        -- 规模试验区县
        ,CASE 
            WHEN t0.county_name REGEXP "望城|长沙|芙蓉" AND   INSTR(t1.grid_name,"KA网格")>0 THEN 1
            ELSE 0 
        END AS is_trail_region -- `是否试验区域`
        ,CASE
            WHEN 
                t0.county_name REGEXP "望城|长沙|芙蓉" AND   INSTR(t1.grid_name,"KA网格")>0
            THEN "试验区域试验组"
            WHEN t0.county_name REGEXP "望城|长沙|芙蓉" AND   INSTR(t1.grid_name,"KA网格")=0
            THEN "试验区域对照组"
        END AS trail_region_type -- `试验区域类型`

        -- 所见即所得运营
        ,IF(
            t1.category_level4_name IN ("麒麟西瓜", "香蕉"), 1, 0
        ) AS is_ka_operate_cat4 -- `是否ka试点品类`

        -- 特殊品类运营
        ,CASE
            WHEN t1.category_level4_name IN ("金枕榴莲","干尧榴莲","甲仑榴莲","托曼尼榴莲","青尼榴莲")
            THEN "榴莲"
        END AS special_operate_cat -- `特殊品类运营类型`

        ,t1.delivered_goods_amt -- `送达金额`
        ,t1.final_refund_amt_order_time -- `赔付金额`
        ,t1.ordered_goods_num -- `下单数量`
        ,t1.commission_amt -- `抽佣金额`

        -- 售后流失
        ,IF(t1.after_sale_num>0, 1, 0) AS is_after_sale  -- `是否当日售后`
        ,IF(NVL(t1.rebuy_days_num_7d, 0)+NVL(t1.ordered_goods_num,0)>0, 1, 0) AS is_rebuy_cdta7d -- `是否后八日下单`


        ,NVL(t3.total_refund_amount_upgrade,0) AS total_refund_amount_upgrade -- `升级售后总赔付金额`
        ,NVL(t3.platform_refund_amount_upgrade,0) AS platform_refund_amount_upgrade -- `升级售后平台赔付金额`
        -- COUNT(1)
    FROM datawarehouse_max.dws_store_mall_store_category_level4_base_daily_asc t1
    LEFT JOIN datawarehouse_max.dim_store_daily_full t0
        ON t0.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t0.dt = t1.dt
        AND t0.customer_store_id = t1.customer_store_id
        AND t0.mall_id = t1.mall_id
    LEFT JOIN (
        SELECT
            t1.company_id AS mall_id
            ,t1.grid_id
            ,t1.grid_name 
            ,t1.dt
            ,ROW_NUMBER() OVER(PARTITION BY t1.company_id, t1.dt, t1.grid_id) AS rnk
        FROM datawarehouse_max.ods_agent_service_agent_agent_grid_full t1
        WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
            AND t1.company_id = 871
            AND t1.status = 1
        QUALIFY rnk = 1
    ) t2
        ON t2.dt = t1.dt
        AND t2.mall_id = t1.mall_id
        AND t2.grid_id = t1.grid_id
    LEFT JOIN upgrade_stat t3
        ON t3.dt = t1.dt
        AND t3.mall_id = t1.mall_id
        AND t3.category_level1_id = t1.category_level1_id
        AND t3.category_level4_id = t1.category_level4_id
        AND t3.customer_store_id = t1.customer_store_id

    WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.mall_id = 871
        AND t1.mall_name = "标果长沙"
        AND NVL(t1.exposed_cnt,0) + NVL(t1.clicked_cnt,0) +NVL(t1.added_cnt,0)+NVL(t1.ordered_goods_num,0)+NVL(t1.after_sale_num, 0) + NVL(t1.rebuy_goods_num_7d_aftersale, 0)> 0

)


SELECT 
    t1.dt AS `日期`
    ,t1.mall_id AS `商城id`
    ,t1.mall AS `商城`
    ,t1.grid_id AS `网格id`
    ,t1.grid_name AS `网格名称`
    ,t1.grid_operate_type AS `网格运营类型`
    ,t1.bd_id AS `bdid`
    ,t1.bd_name AS `bd姓名`
    ,t1.customer_store_id AS `店铺id`
    ,t1.province_id AS `省id`
    ,t1.province_name AS `省名称`
    ,t1.city_id AS `市id`
    ,t1.city_name AS `市名称`
    ,t1.county_id AS `区县id`
    ,t1.county_name AS `区县名称`
    ,t1.category_level1_id AS `一级类目id`
    ,t1.category_level1_name AS `一级类目名称`
    ,t1.category_level4_id AS `四级类目id`
    ,t1.category_level4_name AS `四级类目名称`

    -- 蔬菜和干货下单
    ,t1.is_vegitable AS `是否蔬菜/干货类目`

    -- 规模试验区县
    ,t1.is_trail_region AS `是否试验区域`
    ,t1.trail_region_type AS `试验区域类型`

    -- 所见即所得运营
    ,t1.is_ka_operate_cat4 AS `是否ka试点品类`
    -- 特殊品类运营
    ,t1.special_operate_cat AS `特殊品类运营类型`
    ,t1.delivered_goods_amt AS `送达金额`
    ,t1.final_refund_amt_order_time AS `赔付金额`
    ,t1.ordered_goods_num AS `下单数量`
    ,t1.commission_amt AS `抽佣金额`

    -- 售后流失
    ,t1.is_after_sale  AS `是否当日售后`
    ,t1.is_rebuy_cdta7d AS `是否后八日下单`

    ,t1.total_refund_amount_upgrade AS `升级售后总赔付金额`
    ,t1.platform_refund_amount_upgrade AS `升级售后平台赔付金额`
FROM base t1
WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.mall_id = 871
;