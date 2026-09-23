WITH result AS(
    -- 北极星指标
    SELECT
        t1.dt -- "日期" STRING
        ,t1.mall_id -- "商城ID" BIGINT
        ,t1.mall -- "商城名称" STRING
        ,NULL AS store_type -- "门店类型" STRING
        ,"主力进货渠道门店数" AS metric_name -- "指标名称" STRING
        ,COUNT(DISTINCT IF(
            t1.use_status="有效店铺" AND GET_JSON_OBJECT(t1.store_acheive_type_info, "$.是否水果主力进货渠道门店") = "是", t1.customer_store_id, NULL
        )) AS metric_value -- "水果主力进货渠道门店数" BIGINT
    FROM datawarehouse_max_dev.changsha_project_store_info_daily_asc t1
    WHERE t1.dt = DATEADD(${date_param}, ${end_offset}, "dd")
    GROUP BY t1.dt -- "日期" STRING
        ,t1.mall_id -- "商城ID" BIGINT
        ,t1.mall -- "商城名称" STRING

    UNION ALL

    -- 北极星指标基线
    SELECT
        DATEADD(${date_param}, ${end_offset}, "dd") AS dt -- "日期" STRING
        ,t1.mall_id -- "商城ID" BIGINT
        ,t1.mall -- "商城名称" STRING
        ,NULL AS store_type -- "门店类型" STRING
        ,"主力进货渠道门店数上个双月基线" AS metric_name -- "指标名称" STRING
        ,COUNT(DISTINCT IF(
            t1.use_status="有效店铺" AND GET_JSON_OBJECT(t1.store_acheive_type_info, "$.是否水果主力进货渠道门店") = "是", t1.customer_store_id, NULL
        )) AS fruit_major_store_num -- "水果主力进货渠道门店数" BIGINT
    FROM datawarehouse_max_dev.changsha_project_store_info_daily_asc t1
    WHERE t1.dt = "2026-08-31"
    GROUP BY t1.dt -- "日期" STRING
        ,t1.mall_id -- "商城ID" BIGINT
        ,t1.mall -- "商城名称" STRING

    UNION ALL
    SELECT
        DATEADD(${date_param}, ${end_offset}, "dd") AS dt -- "日期" STRING
        ,t1.mall_id -- "商城ID" BIGINT
        ,t1.mall -- "商城名称" STRING
        ,NULL AS store_type -- "门店类型" STRING
        ,"主力进货渠道门店数周同比" AS metric_name -- "指标名称" STRING
        ,COUNT(DISTINCT IF(
            t1.use_status="有效店铺" AND GET_JSON_OBJECT(t1.store_acheive_type_info, "$.是否水果主力进货渠道门店") = "是", t1.customer_store_id, NULL
        )) AS metric_value -- "水果主力进货渠道门店数" BIGINT
    FROM datawarehouse_max_dev.changsha_project_store_info_daily_asc t1
    WHERE t1.dt = DATEADD(${date_param}, ${end_offset} - 7, "dd")
    GROUP BY t1.dt -- "日期" STRING
        ,t1.mall_id -- "商城ID" BIGINT
        ,t1.mall -- "商城名称" STRING

    UNION ALL

    -- 圈选客户达成等级
    SELECT
        t1.dt -- "日期" STRING
        ,t1.mall_id -- "商城ID" BIGINT
        ,t1.mall -- "商城名称" STRING

        ,CONCAT_WS(
            "-"
            ,GET_JSON_OBJECT(t1.store_operate_type_info, "$.A3圈选客户类型")
            ,GET_JSON_OBJECT(t1.store_acheive_type_info, "$.达成等级标签")
        ) AS store_type -- "门店类型" STRING
        ,"不同圈选类型客户达成等级门店数" AS metric_name -- "指标名称" STRING
        ,COUNT(DISTINCT IF(
            t1.use_status="有效店铺" , t1.customer_store_id, NULL
        )) AS metric_value -- "指标值" STRING
    FROM datawarehouse_max_dev.changsha_project_store_info_daily_asc t1
    WHERE t1.dt = DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.use_status = "有效店铺"

    GROUP BY t1.dt -- "日期" STRING
        ,t1.mall_id -- "商城ID" BIGINT
        ,t1.mall -- "商城名称" STRING
        ,GET_JSON_OBJECT(t1.store_operate_type_info, "$.A3圈选客户类型")
        ,GET_JSON_OBJECT(t1.store_acheive_type_info, "$.达成等级标签")

    UNION ALL

    -- 圈选客户的ARPU
    SELECT
        t1.dt -- "日期" STRING
        ,t1.mall_id -- "商城ID" BIGINT
        ,t1.mall -- "商城名称" STRING

        ,GET_JSON_OBJECT(t1.store_operate_type_info, "$.A3圈选客户类型") AS store_type -- "门店类型" STRING
        ,"不同圈选类型客户拜访ARPU" AS metric_name -- "指标名称" STRING
        ,ROUND(
            (
                SUM(IF(
                    -- t1.use_status="有效店铺" AND 
                    t1.visited_station_days_m6dtcd>0 , t1.delivered_fruit_goods_amt_m6dtcd, 0
                )) - SUM(IF(
                    -- t1.use_status="有效店铺" AND 
                    t1.visited_station_days_m6dtcd>0 , t1.delivered_fruit_goods_amt_m13dtm7d, 0
                ))
            ) / COUNT(DISTINCT IF(
                -- t1.use_status="有效店铺" AND 
                t1.visited_station_days_m6dtcd>0, t1.customer_store_id, NULL
            )) 
        ) AS metric_value -- "指标值" STRING
    FROM datawarehouse_max_dev.changsha_project_store_info_daily_asc t1
    WHERE t1.dt = DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.use_status = "有效店铺"

    GROUP BY t1.dt -- "日期" STRING
        ,t1.mall_id -- "商城ID" BIGINT
        ,t1.mall -- "商城名称" STRING
        ,GET_JSON_OBJECT(t1.store_operate_type_info, "$.A3圈选客户类型")

    UNION ALL

    -- 不同运营类型


    SELECT
        t1.dt -- "日期" STRING
        ,t1.mall_id -- "商城ID" BIGINT
        ,t1.mall -- "商城名称" STRING
        ,"KA客户" AS store_type -- "门店类型" STRING
        ,"不同运营类型活跃率" AS metric_name -- "指标名称" STRING
        ,ROUND(
            COUNT(DISTINCT IF(
                GET_JSON_OBJECT(t1.store_acheive_type_info, "$.是否中心仓激活")="是" AND t1.use_status="有效店铺"
                    AND t1.ordered_fruit_days_m6dtcd>0, t1.customer_store_id, NULL
            ))
            / COUNT(DISTINCT IF(
                GET_JSON_OBJECT(t1.store_acheive_type_info, "$.是否中心仓激活")="是" AND t1.use_status="有效店铺", t1.customer_store_id, NULL
            )), 4
        ) AS metric_value -- "指标值" STRING
    FROM datawarehouse_max_dev.changsha_project_store_info_daily_asc t1
    WHERE t1.dt = DATEADD(${date_param}, ${end_offset}, "dd")
        AND INSTR(GET_JSON_OBJECT(t1.store_operate_type_info, "$.门店运营类型"), "KA") > 0
    GROUP BY t1.dt -- "日期" STRING
        ,t1.mall_id -- "商城ID" BIGINT
        ,t1.mall -- "商城名称" STRING

)


SELECT
	t1.dt AS `日期`
	,t1.mall_id AS `商城id`
	,t1.mall AS `商城名称`
	,t1.store_type AS `门店类型`
	,t1.metric_name AS `指标名称`
    ,t1.metric_value AS `指标值`
FROM result t1
;