WITH stores AS(
    SELECT
        DATEADD(${date_param}, ${end_offset}, "dd") AS dt -- "日期" BIGINT
        ,t1.mall_id -- "商城ID" BIGINT
        ,t1.customer_store_id -- "店铺ID" BIGINT
        ,t1.use_status_desc AS use_status -- "使用状态" STRING
        ,t1.province_id -- "省区ID" BIGINT
        ,t1.province_name -- "省区名称" STRING
        ,t1.city_id -- "市ID" BIGINT
        ,t1.city_name -- "市名称" STRING
        ,t1.county_id -- "区县ID" BIGINT
        ,t1.county_name -- "区县名称" STRING
        ,t1.grid_id -- "网格ID" BIGINT
        ,t1.grid_name -- "网格名称" STRING
        ,t1.bd_id -- "BDID" BIGINT
        ,t1.bd_name -- "BD姓名" STRING
        ,IF(
            LENGTH(CONCAT_WS(
                "|"
                ,CASE
                    WHEN
                        ISNULL(t1.grid_id)
                    THEN "未配置网格"
                    WHEN
                        ISNOTNULL(t1.grid_id) AND ISNULL(t1.grid_name)
                    THEN "缺失网格名称"
                    WHEN
                        INSTR(t1.grid_name, "KA") + INSTR(t1.grid_name, "ka") >0 
                    THEN "KA客户"
                    WHEN
                        INSTR(t1.grid_name, "线上") >0
                    THEN "线上维护客户"
                    WHEN
                        INSTR(t1.grid_name, "特展") >0
                    THEN "特战队客户"
                END
                ,IF(ISNOTNULL(t2.grid_id), "代理人客户", NULL)
            )) > 0
            ,CONCAT_WS(
                "|"
                ,CASE
                    WHEN
                        ISNULL(t1.grid_id)
                    THEN "未配置网格"
                    WHEN
                        ISNOTNULL(t1.grid_id) AND ISNULL(t1.grid_name)
                    THEN "缺失网格名称"
                    WHEN
                        INSTR(t1.grid_name, "KA") + INSTR(t1.grid_name, "ka") >0 
                    THEN "KA客户"
                    WHEN
                        INSTR(t1.grid_name, "线上") >0
                    THEN "线上维护客户"
                    WHEN
                        INSTR(t1.grid_name, "特战") >0
                    THEN "特战队客户"
                END
                ,IF(ISNOTNULL(t2.grid_id), "代理人客户", NULL)
            )
            ,"其他类型"
        ) AS operate_type -- "门店运营类型" STRING
        ,t2.entity_id AS agent_id -- "代理人ID" BIGINT
        ,IF(
            INSTR(t2.entity_name, " ") + INSTR(t2.entity_name, "-") = 0
            ,t2.entity_name
            ,SPLIT_PART(SPLIT_PART(t2.entity_name, " ", 1), "-", 2)
        ) AS agent_name -- "代理人姓名" STRING

        ,t2.entity_name

        ,t3.is_central_active
    FROM datawarehouse_max.dim_store_daily_full t1
    LEFT JOIN datawarehouse_max.ods_agent_service_agent_agent_grid_full t2
        ON t2.dt = DATEADD(${date_param}, ${end_offset}, "dd")
        AND t2.dt = t1.dt
        AND t2.company_id = t1.mall_id
        AND t2.grid_id = t1.grid_id
        AND t2.status = 1
    LEFT JOIN datawarehouse_max.ads_store_store_label_daily_full t3
        ON t3.dt = DATEADD(${date_param}, ${end_offset}, "dd")
        AND t3.dt = t1.dt
        AND t3.mall_id = t1.mall_id
        AND t3.customer_store_id = t1.customer_store_id
    WHERE t1.dt = DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.mall_id = 871

        AND t1.use_status= "EFFECTIVE"
)


-- 门店圈选
,store_operate_type AS (
    SELECT
        t1.dt
        ,t1.mall_id
        ,t1.customer_store_id
        ,t1.screen_type
        ,t1.hierarchy_tag
        ,t1.central_activity_tag
    FROM datawarehouse_max.ads_store_mall_store_central_warehouse_fruit_features_daily_asc t1
    WHERE t1.dt IN (
        "2026-09-18", DATEADD(${date_param}, ${end_offset}, "dd")
            ,DATEADD(${date_param}, ${end_offset} - 1, "dd")
    )
        AND t1.mall_id = 871
        AND (ISNOTNULL(t1.screen_type) OR ISNOTNULL(t1.hierarchy_tag) OR ISNOTNULL(t1.central_activity_tag))
)


-- 门店业绩统计
,store_stat AS(
    SELECT
        DATEADD(${date_param}, ${end_offset}, "dd") AS dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.customer_store_id -- `店铺id`


        ,COUNT(DISTINCT IF(
            t1.ordered_goods_num > 0 AND t1.category_level1_name = "水果", t1.dt, NULL
        )) AS ordered_fruit_days_m29dtcd -- `近30天下单水果天数`
        ,SUM(IF(
            t1.ordered_goods_num > 0 AND t1.category_level1_name = "水果", t1.delivered_goods_amt, NULL
        )) AS delivered_fruit_goods_amt_days_m29dtcd -- `近30天下单水果货值`

        ,SUM(IF(
            t1.ordered_goods_num > 0 AND t1.category_level1_name = "水果" AND 
                t1.dt BETWEEN DATEADD(${date_param}, ${end_offset}-6, "dd") AND DATEADD(${date_param}, ${end_offset}, "dd")
            , t1.delivered_goods_amt, NULL
        )) AS delivered_fruit_goods_amt_m6dtcd -- `近7天下单水果货值`
        ,SUM(IF(
            t1.ordered_goods_num > 0 AND t1.category_level1_name = "水果" AND 
                t1.dt BETWEEN DATEADD(${date_param}, ${end_offset}-13, "dd") AND DATEADD(${date_param}, ${end_offset}-7, "dd")
            , t1.delivered_goods_amt, NULL
        )) AS delivered_fruit_goods_amt_m13dtm7d -- `t-13到t-7下单水果货值`
        ,COUNT(DISTINCT IF(
            t1.ordered_goods_num > 0 AND t1.category_level1_name = "水果" AND
                t1.dt BETWEEN DATEADD(${date_param}, ${end_offset}-6, "dd") AND DATEADD(${date_param}, ${end_offset}, "dd")
            , t1.dt, NULL
        )) AS ordered_fruit_days_m6dtcd -- `近7天下单水果天数`
        ,COUNT(DISTINCT IF(
            t1.ordered_goods_num > 0 AND t1.category_level1_name = "水果" AND
                t1.dt BETWEEN DATEADD(${date_param}, ${end_offset}-6, "dd") AND DATEADD(${date_param}, ${end_offset}, "dd")
            , t1.category_level4_id, NULL
        )) AS ordered_fruit_cat4_num_m6dtcd -- `近7天下单水果类目数`

        ,SUM(IF(
            t1.dt = DATEADD(${date_param}, ${end_offset}-1, "dd") AND t1.category_level1_name = "水果"
                , t1.delivered_goods_amt, NULL
        )) AS delivered_fruit_goods_amt_m1d -- "昨日送达水果金额" DECIMAL(10,2)

        ,SUM(IF(
            t1.dt = DATEADD(${date_param}, ${end_offset}, "dd") 
                , t1.ordered_goods_num, NULL
        )) AS ordered_goods_num -- "下单商品数量" BIGINT
        ,SUM(IF(
            t1.dt = DATEADD(${date_param}, ${end_offset}, "dd") AND t1.category_level1_name = "水果"
                , t1.ordered_goods_num, NULL
        )) AS ordered_fruit_goods_num -- "下单水果商品数量" BIGINT
        ,SUM(IF(
            t1.dt = DATEADD(${date_param}, ${end_offset}, "dd") AND t1.category_level1_name = "水果"
                , t1.ordered_goods_amt, NULL
        )) AS ordered_fruit_goods_amt -- "下单水果金额" DECIMAL(10,2)
        ,SUM(IF(
            t1.dt = DATEADD(${date_param}, ${end_offset}, "dd") AND t1.category_level1_name = "水果"
                , t1.delivered_goods_amt, NULL
        )) AS delivered_fruit_goods_amt -- "送达水果金额" DECIMAL(10,2)
        ,SUM(IF(
            t1.dt = DATEADD(${date_param}, ${end_offset}, "dd") AND t1.category_level1_name = "水果"
                , t1.delivered_goods_num, NULL
        )) AS delivered_fruit_goods_num -- "送达水果数量" DECIMAL(10,2)
        ,SUM(IF(
            t1.dt = DATEADD(${date_param}, ${end_offset}, "dd") AND t1.category_level1_name = "水果"
                , t1.delivered_gross_wgt, NULL
        )) AS delivered_fruit_gross_wgt -- "送达水果毛重" DECIMAL(10,2)
        ,SUM(IF(
            t1.dt = DATEADD(${date_param}, ${end_offset}, "dd") AND t1.category_level1_name = "水果"
                , t1.ordered_sku_num, NULL
        )) AS ordered_fruit_sku_num -- "下单水果商品数量" BIGINT
        ,COUNT(DISTINCT IF(
            t1.dt = DATEADD(${date_param}, ${end_offset}, "dd") AND t1.category_level1_name = "水果"
                AND t1.ordered_goods_num>0 , t1.category_level4_id, NULL
        )) AS ordered_fruit_cat4_num -- "下单水果品类数量" BIGINT

        ,SUM(IF(
            t1.dt = DATEADD(${date_param}, ${end_offset}, "dd") AND t1.category_level1_name = "水果"
                , t1.final_refund_amt_order_time, NULL
        )) AS final_refund_amt_order_time -- "售后赔付金额" DECIMAL(10,2)

    FROM(
        SELECT
            t1.dt -- `日期`
            ,t1.mall_id -- `商城id`
            ,t1.customer_store_id -- `店铺id`
            ,t1.category_level1_id -- `一级类目id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level3_id -- `三级类目id`
            ,t1.category_level3_name -- `三级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`

            ,SUM(t1.ordered_goods_num) AS ordered_goods_num -- `下单数量`
            ,SUM(t1.ordered_goods_amt) AS ordered_goods_amt -- `下单金额`
            ,SUM(t1.delivered_goods_amt) AS delivered_goods_amt -- `送达金额`
            ,SUM(t1.delivered_goods_num) AS delivered_goods_num -- `送达数量`
            ,SUM(t1.deliveried_gross_wgt) AS delivered_gross_wgt -- "送达毛重"
            ,SUM(t1.after_sale_num_order_time) AS after_sale_num_order_time -- "售后数量" BIGINT
            ,SUM(t1.final_refund_amt_order_time) AS final_refund_amt_order_time -- "售后赔付金额" DECIMAL(10,2)
            ,COUNT(DISTINCT IF(t1.ordered_goods_num>0, t1.sku_id, NULL )) AS ordered_sku_num -- `下单sku数量`

        FROM datawarehouse_max.dws_store_mall_store_sku_base_daily_asc t1

        WHERE t1.dt BETWEEN DATEADD(${date_param}, ${end_offset} - 29, "dd") 
                AND DATEADD(${date_param}, ${end_offset}, "dd")
            AND t1.mall_id = 871
            AND NVL(t1.ordered_goods_num, 0) > 0
            -- AND t1.status != "CANCEL"

        GROUP BY t1.dt -- `日期`
            ,t1.mall_id -- `商城id`
            ,t1.customer_store_id -- `店铺id`
            ,t1.category_level1_id -- `一级类目id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level3_id -- `三级类目id`
            ,t1.category_level3_name -- `三级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`
    ) t1

    GROUP BY t1.mall_id -- `商城id`
        ,t1.customer_store_id -- `店铺id`
)




-- 门店拜访统计
,visited_stat AS(
    SELECT
        DATEADD(${date_param}, ${end_offset}, "dd") AS dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.customer_store_id -- `店铺id`
        ,COUNT(DISTINCT IF(
            t1.visit_cnt_bystore > 0, t1.dt, NULL
        )) AS visited_station_days_m6dtcd -- `近7天线下拜访天数`
        ,MAX(IF(
            t1.dt = DATEADD(${date_param}, ${end_offset}, "dd"), t1.visit_cnt_bystore, NULL
        )) AS visit_cnt_bystore -- "线下拜访次数" BIGINT
    FROM datawarehouse_max.dws_store_mall_store_base_daily_asc t1

    WHERE t1.dt BETWEEN DATEADD(${date_param}, ${end_offset} - 6, "dd") 
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.mall_id = 871

        -- 拜访筛选
        AND t1.visit_cnt_bystore > 0
    GROUP BY t1.mall_id
        ,t1.customer_store_id
)

-- 门店基准数据
,temp AS(
    SELECT
        t1.dt -- "日期" BIGINT
        ,t1.mall_id -- "商城ID" BIGINT
        ,t1.customer_store_id -- "店铺ID" BIGINT
        ,t1.use_status -- "使用状态" STRING
        ,t1.province_id -- "省区ID" BIGINT
        ,t1.province_name -- "省区名称" STRING
        ,t1.city_id -- "市ID" BIGINT
        ,t1.city_name -- "市名称" STRING
        ,t1.county_id -- "区县ID" BIGINT
        ,t1.county_name -- "区县名称" STRING
        ,t1.grid_id -- "网格ID" BIGINT
        ,t1.grid_name -- "网格名称" STRING
        ,t1.bd_id -- "BDID" BIGINT
        ,t1.bd_name -- "BD姓名" STRING
        ,t1.agent_id -- "代理人ID" BIGINT
        ,t1.agent_name -- "代理人姓名" STRING

        ,t1.is_central_active -- "是否中心仓激活" BIGINT
        ,t1.operate_type -- "门店运营类型" STRING
        ,t3.hierarchy_tag -- "达成等级标签" STRING
        ,NVL(
            IF(
                t1.dt BETWEEN "2026-08-31" AND "2026-09-19", t3.screen_type, t4.screen_type
            ), "普通客户"
        ) AS screen_type -- "门店圈选类型" STRING
        ,t4.central_activity_tag
        ,t04.central_activity_tag AS central_activity_tag_m1d

        ,CASE
            WHEN 
                t5.ordered_fruit_days_m29dtcd  >= 20 AND t5.ordered_fruit_cat4_num_m6dtcd >= 20
                    AND t5.delivered_fruit_goods_amt_days_m29dtcd / NULLIF(t5.ordered_fruit_days_m29dtcd, 0) > 800
            THEN "水果主力进货渠道门店"
        END AS store_operate_result -- "门店运营结果类型:主力进货渠道门店" STRING

        ,t2.visited_station_days_m6dtcd -- `近7天线下拜访天数`
        ,t2.visit_cnt_bystore -- "线下拜访次数" BIGINT

        ,t5.ordered_fruit_days_m29dtcd -- "近30天下单水果天数" DECIMAL(12,2)
        ,t5.delivered_fruit_goods_amt_days_m29dtcd -- "近30天下单水果货值" DECIMAL(12,2)
        ,t5.delivered_fruit_goods_amt_m6dtcd -- "近7天下单水果货值" DECIMAL(12,2)
        ,t5.delivered_fruit_goods_amt_m13dtm7d -- "t-14到t-8下单水果货值" DECIMAL(12,2)
        ,t5.ordered_fruit_days_m6dtcd -- "近7天下单水果天数" BIGINT
        ,t5.ordered_fruit_cat4_num_m6dtcd -- "近7天下单水果类目数" BIGINT

        ,t5.ordered_goods_num -- "下单商品数量" BIGINT
        ,t5.ordered_fruit_goods_num -- "下单水果商品数量" BIGINT
        ,t5.ordered_fruit_goods_amt -- "下单水果金额" DECIMAL(10,2)
        ,t5.delivered_fruit_goods_amt -- "送达水果金额" DECIMAL(10,2)
        ,t5.delivered_fruit_goods_num -- "送达水果数量" DECIMAL(10,2)
        ,t5.delivered_fruit_gross_wgt -- "送达水果毛重" DECIMAL(10,2)
        ,t5.ordered_fruit_sku_num -- "下单水果商品数量" BIGINT
        ,t5.ordered_fruit_cat4_num -- "下单水果品类数量" BIGINT

    FROM stores t1
    LEFT JOIN visited_stat t2
        ON t2.mall_id = t1.mall_id
        AND t2.customer_store_id = t1.customer_store_id

    LEFT JOIN store_operate_type t3
        ON t3.mall_id = t1.mall_id
        AND t3.customer_store_id = t1.customer_store_id
        AND t3.dt = "2026-09-18"

    LEFT JOIN store_operate_type t4
        ON t4.mall_id = t1.mall_id
        AND t4.customer_store_id = t1.customer_store_id
        AND t4.dt = t1.dt

    LEFT JOIN store_operate_type t04
        ON t04.mall_id = t1.mall_id
        AND t04.customer_store_id = t1.customer_store_id
        AND t04.dt = DATEADD(DATE(t1.dt), -1, "dd")

    LEFT JOIN store_stat t5
        ON t5.mall_id = t1.mall_id
        AND t5.dt = t1.dt
        AND t5.customer_store_id = t1.customer_store_id

)



-- 处理出来的门店基础数据，可以存储
INSERT OVERWRITE TABLE datawarehouse_max_dev.changsha_project_store_info_daily_asc PARTITION(dt)
SELECT
        t1.mall_id -- "商城ID" BIGINT
    ,t2.mall -- "商城名称" STRING
        ,t1.customer_store_id -- "店铺ID" BIGINT
        ,t1.use_status -- "使用状态" STRING
        ,t1.province_id -- "省区ID" BIGINT
        ,t1.province_name -- "省区名称" STRING
        ,t1.city_id -- "市ID" BIGINT
        ,t1.city_name -- "市名称" STRING
        ,t1.county_id -- "区县ID" BIGINT
        ,t1.county_name -- "区县名称" STRING
        ,t1.grid_id -- "网格ID" BIGINT
        ,t1.grid_name -- "网格名称" STRING
    ,t1.agent_id -- "代理人ID" BIGINT
    ,t1.agent_name -- "代理人姓名" STRING

    ,"{" || 
        '"门店运营类型": "' || 
        t1.operate_type || '", ' ||
        '"A3圈选客户类型": "' ||
        t1.screen_type || '"' ||
    "}" AS store_operate_type_info -- "门店运营类型信息(包括运营类型和圈选类型)" STRING
    ,"{" || 
        '"是否水果主力进货渠道门店": "' ||
        IF(t1.store_operate_result=="水果主力进货渠道门店", "是", "否") || '",'
        '"是否中心仓激活": "' ||
        IF(t1.is_central_active="激活", "是", "否") || '",'
        '"达成等级标签": "' || NVL(t1.hierarchy_tag, "其他") || '",'
        '"昨日和今日活跃度标签": "' || NVL(t1.central_activity_tag_m1d, "无标签") || "-" || NVL(t1.central_activity_tag, "无标签") || '"'
    "}" AS store_acheive_type_info -- "门店达成信息" STRING

    ,NVL(t1.visited_station_days_m6dtcd,0) AS visited_station_days_m6dtcd -- "近7天线下拜访天数" BIGINT
    ,NVL(t1.ordered_fruit_days_m29dtcd,0) AS ordered_fruit_days_m29dtcd -- "近30天下单水果天数" DECIMAL(12,2)
    ,NVL(t1.delivered_fruit_goods_amt_days_m29dtcd,0) AS delivered_fruit_goods_amt_days_m29dtcd -- "近30天下单水果货值" DECIMAL(12,2)
    ,NVL(t1.delivered_fruit_goods_amt_m6dtcd,0) AS delivered_fruit_goods_amt_m6dtcd -- "近7天下单水果货值" DECIMAL(12,2)
    ,NVL(t1.delivered_fruit_goods_amt_m13dtm7d,0) AS delivered_fruit_goods_amt_m13dtm7d -- "t-14到t-8下单水果货值" DECIMAL(12,2)
    ,NVL(t1.ordered_fruit_days_m6dtcd,0) AS ordered_fruit_days_m6dtcd -- "近7天下单水果天数" BIGINT
    ,NVL(t1.ordered_fruit_cat4_num_m6dtcd,0) AS ordered_fruit_cat4_num_m6dtcd -- "近7天下单水果类目数" BIGINT


    ,NVL(t1.visit_cnt_bystore,0) AS visit_cnt_bystore -- "线下拜访次数" BIGINT
    ,NVL(t1.ordered_goods_num,0) AS ordered_goods_num -- "下单商品数量" BIGINT
    ,NVL(t1.ordered_fruit_goods_num,0) AS ordered_fruit_goods_num -- "下单水果商品数量" BIGINT
    ,NVL(t1.ordered_fruit_goods_amt,0) AS ordered_fruit_goods_amt -- "下单水果金额" DECIMAL(10,2)
    ,NVL(t1.delivered_fruit_goods_amt,0) AS delivered_fruit_goods_amt -- "送达水果金额" DECIMAL(10,2)
    ,NVL(t1.delivered_fruit_goods_num,0) AS delivered_fruit_goods_num -- "送达水果数量" DECIMAL(10,2)
    ,NVL(t1.delivered_fruit_gross_wgt,0) AS delivered_fruit_gross_wgt -- "送达水果毛重" DECIMAL(10,2)
    ,NVL(t1.ordered_fruit_sku_num,0) AS ordered_fruit_sku_num -- "下单水果商品数量" BIGINT
    ,NVL(t1.ordered_fruit_cat4_num,0) AS ordered_fruit_cat4_num -- "下单水果品类数量" BIGINT
    ,t1.dt -- "日期" STRING
FROM temp t1
LEFT JOIN datawarehouse_max.dim_mall_full t2
    ON t2.mall_id = t1.mall_id
WHERE t1.dt =DATEADD(${date_param}, ${end_offset}, "dd")
;