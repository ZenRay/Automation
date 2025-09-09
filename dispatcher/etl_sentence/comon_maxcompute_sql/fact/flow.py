#coding:utf-8
"""
Maxcompute Fact Table ETL Sentence
1. 流量相关事实表 fact_flow_sentence: changsha_flow_fact_daily_asc
"""



fact_flow_sentence = """
-- 1. 基础信息： 商城维度信息
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



,fronts AS(
    SELECT
        t1.dt
        ,t1.id AS front_category_id
        ,t1.store_name AS front_category_name
        ,t1.tab_bar_type AS front_category_type
        ,871 AS mall_id
        ,t2.id AS parent_category_id
        ,t2.store_name AS parent_category_name
        ,t2.tab_bar_type AS parent_front_category_type
        ,t2.sort AS parent_sort
    FROM datawarehouse_max.ods_goods_service_front_category_v2_full t1
    LEFT JOIN datawarehouse_max.ods_goods_service_front_category_v2_full t2
        ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
                AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t1.parent_id = t2.id
        AND t1.dt = t2.dt
    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
                AND DATEADD(CURRENT_DATE(), 0, "dd")
)



,base AS(
    SELECT 
        t1.dt
        ,t1.mall_id
        ,t1.mall
        ,t1.sku_id
        ,t1.user_id AS customer_id
        ,t1.customer_store_id
        
        ,IF(
            TOUPPER(NVL(t2.ssp_activity_tag, "other")) NOT IN ("OLD", "LOSS"), "NEW", TOUPPER(t2.ssp_activity_tag)
        ) AS ssp_activity_tag -- STRING
        ,t1.page_path
        ,NVL(t1.page_name, t5.path_name) AS page_name
        ,t1.sort_index
        ,t1.show_standard_price
        ,t1.show_unit_price
        ,t1.front_category_id
        ,NVL(t3.front_category_name, t4.front_category_name) AS front_category_name
        ,NVL(t3.front_category_type, t4.front_category_type) AS front_category_type
        ,NVL(t3.parent_category_id, t4.parent_category_id) AS parent_category_id
        ,NVL(t3.parent_category_name, t4.parent_category_name) AS parent_category_name
        ,NVL(t3.parent_front_category_type, t4.parent_front_category_type) AS parent_front_category_type
        ,CAST(IF(INSTR(t1.event_time, ".")>0, SPLIT(t1.event_time, "\\.")[0], t1.event_time) AS DATETIME) AS event_time
        ,t1.event_type

    FROM (
        SELECT
            t1.dt
            ,t1.mall_id
            ,t2.mall
            ,t1.sku_id
            ,t1.user_id
            ,t1.customer_store_id
            ,IF(t1.page_path REGEXP "^/" OR ISNULL(t1.page_path), t1.page_path, CONCAT("/", t1.page_path)) AS page_path
            ,t1.page_name
            ,t1.sort_index
            ,t1.show_standard_price
            ,t1.show_unit_price
            ,t1.frontfourcategoryid AS front_category_id
            ,t1.event_time
            ,"EXPOSE" AS event_type
        FROM datawarehouse_max.dwd_flow_log_browse_global_daily_asc   t1 
        INNER JOIN mall t2
            ON t2.mall_id = t1.mall_id

        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
                    AND DATEADD(CURRENT_DATE(), 0, "dd")



        UNION

        SELECT
            t1.dt
            ,t1.mall_id
            ,t2.mall
            ,t1.sku_id
            ,t1.user_id
            ,t1.customer_store_id
            ,IF(t1.from_page_path REGEXP "^/" OR ISNULL(t1.from_page_path), t1.from_page_path, CONCAT("/", t1.from_page_path)) AS page_path
            ,t1.from_page_name AS page_name
            ,t1.sort_index
            ,t1.show_standard_price
            ,t1.show_unit_price
            -- ,t1.front_category_level4_id
            ,t1.front_category_level2_id AS front_category_id
            ,t1.event_time
            ,"CLICK" AS event_type
        FROM datawarehouse_max.dwd_flow_log_click_goods_daily_asc t1
        INNER JOIN mall t2
            ON t1.mall_id = t2.mall_id
            

        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
                    AND DATEADD(CURRENT_DATE(), 0, "dd")
            
            -- TODO: 非长沙的数据存在重复和缺少页面信息的情况，先暂时过滤掉无页面信息的数据
            AND (t1.mall_id = 871 OR (t1.mall_id != 871 AND (LENGTH(t1.page_path)>0 OR LENGTH(t1.from_page_path)>0)))


        UNION 
        -- 加购数据
        SELECT 
            t1.dt
            ,t1.mall_id
            ,t2.mall
            ,t1.sku_id
            ,t1.user_id
            ,CAST(t1.customer_store_id AS BIGINT) AS customer_store_id
            ,CASE
                WHEN LENGTH(t1.from_page_path)=0 THEN IF(t1.page_path REGEXP "^/" OR ISNULL(t1.page_path), t1.page_path, CONCAT("/", t1.page_path))
                ELSE IF(t1.from_page_path REGEXP "^/" AND ISNULL(t1.from_page_path), t1.from_page_path, CONCAT("/", t1.from_page_path))
            END AS page_path
            ,CASE
                WHEN LENGTH(t1.from_page_path)=0 THEN t1.page_name
                ELSE t1.from_page_name
            END AS page_name
            ,t1.sort_index
            ,t1.show_standard_price
            ,t1.show_unit_price
            -- ,t1.front_category_level4_id
            ,t1.front_category_level2_id AS front_category_id

            ,t1.event_time
            -- 分为两种类型，直接在页面上加购和发生在详情页加购
            ,IF(t1.page_path REGEXP "detail/index", "ADD_DETAIL", "ADD_DIRECT") AS event_type
        FROM datawarehouse_max.dwd_flow_log_add_cart_daily_asc  t1
        INNER JOIN mall t2
            ON t1.mall_id = t2.mall_id


        WHERE  t1.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
                    AND DATEADD(CURRENT_DATE(), 0, "dd")
            AND (t1.mall_id = 871 OR (t1.mall_id != 871 AND (LENGTH(t1.page_path)>0 OR LENGTH(t1.from_page_path)>0)))
    ) t1
    LEFT JOIN changsha_dim_sku_store_tag_daily_asc t2
        ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t2.dt = t1.dt
        AND t2.mall_id = t1.mall_id
        AND t2.customer_store_id = t1.customer_store_id
        AND t2.sku_id = t1.sku_id
    LEFT JOIN fronts t3
        ON t3.dt = t1.dt
        AND t3.front_category_id = t1.front_category_id
        AND t1.mall_id = 871

    LEFT JOIN fronts t4
        ON t4.front_category_id = t1.front_category_id
        AND t4.dt = "2025-05-12"
        AND t1.dt < "2025-05-12"
        AND t1.mall_id = 871

    LEFT JOIN datawarehouse_max.t_new_mall_page_map t5
        ON t5.path = t1.page_path


    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
                    AND DATEADD(CURRENT_DATE(), 0, "dd")
)




,base_temp AS(
    SELECT
        t1.dt -- '日期' STRING
        ,t1.mall_id -- '商城ID' BIGINT
        ,t1.mall -- '商城' STRING
        ,t1.customer_store_id -- '店铺ID' BIGINT
        ,t1.ssp_activity_tag -- '门店商品活跃度标签' STRING
        ,t1.customer_id -- '用户ID' BIGINT
        ,t1.sku_id -- '商品ID' BIGINT
        ,t1.page_path -- '页面路径' STRING
        ,t1.page_name -- '页面名称' STRING
        ,t1.sort_index -- '排序位置' BIGINT
        ,t1.front_category_id -- '前台页面ID' BIGINT
        ,t1.front_category_name -- '前台页面名称' STRING
        ,t1.parent_category_id -- '前台父页面ID' BIGINT
        ,t1.parent_category_name -- '前台父页面名称' STRING
        ,t1.front_category_type -- '前台页面类型' STRING
        ,t1.parent_front_category_type -- '前台父页面类型' STRING
        ,t1.show_standard_price -- '商城展示件单价' DECIMAL(10,2)
        ,t1.show_unit_price -- '商城展示斤单价' DECIMAL(10,2)
        
        ,MAX(IF(t1.event_type="EXPOSE", DATETIME(t1.event_time), NULL)) AS exposed_time_max -- '最晚曝光时间' DATETIME
        ,MIN(IF(t1.event_type="EXPOSE", DATETIME(t1.event_time), NULL)) AS exposed_time_min -- '最早曝光时间' DATETIME
        ,SUM(IF(t1.event_type="EXPOSE", 1,0)) AS exposed_cnt -- '曝光次数' BIGINT
        ,MAX(IF(t1.event_type="CLICK" OR INSTR(t1.event_type, "ADD") >0, DATETIME(t1.event_time), NULL)) AS clicked_time_max -- '最晚点击时间' DATETIME
        ,MIN(IF(t1.event_type="CLICK" OR INSTR(t1.event_type, "ADD") >0, DATETIME(t1.event_time), NULL)) AS clicked_time_min -- '最早点击时间' DATETIME
        ,SUM(IF(t1.event_type="CLICK" OR INSTR(t1.event_type, "ADD") >0, 1,0)) AS clicked_cnt -- '点击次数' BIGINT
        ,MAX(IF(INSTR(t1.event_type, "ADD") >0, DATETIME(t1.event_time), NULL)) AS added_time_max -- '最晚加购时间' DATETIME
        ,MIN(IF(INSTR(t1.event_type, "ADD") >0, DATETIME(t1.event_time), NULL)) AS added_time_min -- '最早加购时间' DATETIME
        ,SUM(IF(INSTR(t1.event_type, "ADD") >0, 1,0)) AS added_cnt -- '加购次数' BIGINT
    FROM base t1
    GROUP BY t1.dt
        ,t1.mall_id
        ,t1.mall
        ,t1.customer_store_id
        ,t1.customer_id -- '用户ID' BIGINT
        ,t1.ssp_activity_tag
        ,t1.sku_id
        ,t1.page_path
        ,t1.page_name
        ,t1.sort_index
        ,t1.front_category_id
        ,t1.front_category_name
        ,t1.parent_category_id
        ,t1.parent_category_name
        ,t1.front_category_type
        ,t1.parent_front_category_type
        ,t1.show_standard_price
        ,t1.show_unit_price
)



,temp AS(
    SELECT
        t1.dt -- '日期' STRING
        ,t1.mall_id -- '商城ID' BIGINT
        ,t1.mall -- '商城' STRING
        ,t1.customer_store_id -- '店铺ID' BIGINT
        ,t1.customer_id -- '用户ID' BIGINT
        ,IF(NVL(t4.user_type, 0) != 0, "非门店用户", "门店用户") AS customer_type -- '用户类型' STRING
        ,t3.customer_store_name -- '店铺名称' STRING
        ,t3.province_id -- '省区ID' BIGINT
        ,t3.province_name -- '省区名称' STRING
        ,t3.city_id -- '市ID' BIGINT
        ,t3.city_name -- '市名称' STRING
        ,t3.county_id -- '区县ID' BIGINT
        ,t3.county_name -- '区县名称' STRING
        ,NVL(t3.store_type, "UNKOWN") AS store_type -- '店铺类型' STRING
        
        ,t1.ssp_activity_tag -- '门店商品活跃度标签' STRING
        ,t1.sku_id -- '商品ID' BIGINT
        ,t2.sku_name -- '商品名称' STRING
        ,t2.back_category_id AS category_level4_id -- '四级类目ID' BIGINT
        ,t2.back_category_name AS category_level4_name -- '四级类目名称' STRING
        ,t2.category_level1_id -- '一级类目ID' BIGINT
        ,t2.category_level1_name -- '一级类目名称' STRING
        ,NVL(t02.back_category_id, t2.back_category_id) AS map_category_level4_id -- '映射四级类目ID' BIGINT
        ,NVL(t02.back_category_name, t2.back_category_name) AS map_category_level4_name -- '映射四级类目名称' STRING
        ,t2.merchant_id -- '商家ID' BIGINT
        ,t2.merchant_name -- '商家名称' STRING
        ,NVL(t2.is_sku_valid,0) AS is_sku_valid -- '是否有效SKU' BIGINT
        ,NVL(t2.is_up_today,0) AS is_up_today -- '是否上架SKU' BIGINT
        
        ,NVL(t1.page_path, "") AS page_path -- '页面路径' STRING
        ,NVL(t1.page_name, "未知") AS page_name -- '页面名称' STRING
        ,NVL(t1.sort_index, -99L) AS sort_index -- '排序位置' BIGINT
        ,NVL(t1.front_category_id, -99L) AS front_category_id -- '前台标签页ID' BIGINT
        ,NVL(t1.front_category_name, "未知") AS front_category_name -- '前台标签页名称' STRING
        ,NVL(t1.parent_category_id, -99L) AS parent_category_id -- '前台父级标签页ID' BIGINT
        ,NVL(t1.parent_category_name, "未知") AS parent_category_name -- '前台父级标签页面名称' STRING
        ,t1.front_category_type -- '前台标签页面类型' STRING
        ,t1.parent_front_category_type -- '前台父级标签页面类型' STRING
        ,NVL(t1.show_standard_price, -99L) AS show_standard_price -- '商城展示件单价' DECIMAL(10,2)
        ,NVL(t1.show_unit_price, -99L) AS show_unit_price -- '商城展示斤单价' DECIMAL(10,2)
        ,t1.exposed_time_max -- '最晚曝光时间' DATETIME
        ,t1.exposed_time_min -- '最早曝光时间' DATETIME
        ,t1.exposed_cnt -- '曝光次数' BIGINT
        ,t1.clicked_time_max -- '最晚点击时间' DATETIME
        ,t1.clicked_time_min -- '最早点击时间' DATETIME
        ,t1.clicked_cnt -- '点击次数' BIGINT
        ,t1.added_time_max -- '最晚加购时间' DATETIME
        ,t1.added_time_min -- '最早加购时间' DATETIME
        ,t1.added_cnt -- '加购次数' BIGINT
        ,'{"用户ID": ' || t1.customer_id || ", " ||
            '"用户类型": "' || IF(NVL(t4.user_type, 0) != 0, "非门店用户", "门店用户") || '", ' ||
            '"店铺ID": ' || NVL(t1.customer_store_id, -999999L) || ", " ||
            '"店铺类型": "' || NVL(t3.store_type, "未知类型") || '", ' ||
            '"商品ID": ' || NVL(t1.sku_id, -999999L) || ", " ||
            '"当日是否上过架": "' || IF(NVL(t2.is_up_today, 0) > 0, "是", "否") || '", ' ||
            '"页面名称": "' || NVL(t1.page_name, "未知页面") || '", '||
            '"页面排序位置": ' || NVL(t1.sort_index, -99999L) || ', '  ||
            '"前台父级标签页面名称": "' || NVL(t1.parent_category_name, "未知") || '", ' ||
            '"前台标签页名称": "' || NVL(t1.front_category_name, "未知") || '", ' ||
            '"展示斤单价": ' || NVL(t1.show_unit_price, -99999L) || ', ' ||
            '"展示件单价": ' || NVL(t1.show_standard_price, -99999L) || ', ' ||
            '"最晚曝光时间": "' || NVL(t1.exposed_time_max, "") || '", ' ||
            '"最早曝光时间": "' || NVL(t1.exposed_time_min, "") || '", ' ||
            '"最晚点击时间": "' || NVL(t1.clicked_time_max, "") || '", ' ||
            '"最早点击时间": "' || NVL(t1.clicked_time_min, "") || '", ' ||
            '"最晚加购时间": "' || NVL(t1.added_time_max, "") || '", ' ||
            '"最早加购时间": "' || NVL(t1.added_time_min, "") || '", ' ||
            '"曝光次数": ' || NVL(t1.exposed_cnt, 0) || ', ' ||
            '"点击次数": ' || NVL(t1.clicked_cnt, 0) || ', ' ||
            '"加购次数": ' || NVL(t1.added_cnt, 0) || 
        "}" AS flow_detail -- '流量明细信息JSON数组嵌套字典' STRING
    FROM base_temp t1
    LEFT JOIN datawarehouse_max.dim_goods_daily_full t2
        ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t2.dt = t1.dt
        AND t2.mall_id = t1.mall_id
        AND t2.sku_id = t1.sku_id
    LEFT JOIN t_back_category_mapping t02
        ON t02.old_category_level1_name = t2.category_level1_name
        AND t02.old_back_category_id = t2.back_category_id
        AND NVL(t02.mall_id, 99999) = IF(t2.mall_id=871, 871, 99999)
        AND t1.dt BETWEEN  t02.start_date AND NVL(t02.end_date, "2999-12-31")
    LEFT JOIN changsha_dim_store_daily_full t3
        ON t3.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t3.dt = t1.dt
        AND t3.customer_store_id = t1.customer_store_id

    LEFT JOIN datawarehouse_max.dim_user_daily_full t4
        ON t4.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t4.dt = t1.dt
        AND t4.user_id = t1.customer_id
    WHERE (
        ISNOTNULL(t1.sku_id) AND ISNOTNULL(t1.customer_id)
            AND ISNOTNULL(t1.customer_store_id)
    )

)


,result AS(
    SELECT
        t1.dt -- '日期' STRING
        ,t1.mall_id -- '商城ID' BIGINT
        ,t1.mall -- '商城' STRING
        ,IF(
            GROUPING(t1.customer_id)==0, t1.customer_id, -99999L
        ) AS customer_id -- '用户ID' BIGINT
        ,IF(
            GROUPING(t1.customer_type)==0, t1.customer_type, "合计"
        ) AS customer_type -- '用户类型' STRING
        ,IF(
            GROUPING(t1.customer_store_id)==0, t1.customer_store_id, -99999L
        ) AS customer_store_id -- '店铺ID' BIGINT
        ,IF(
            GROUPING(t1.customer_store_name)==0, t1.customer_store_name, "合计"
        ) AS customer_store_name -- '店铺名称' STRING
        ,IF(
            GROUPING(t1.store_type)==0, t1.store_type, "合计"
        ) AS store_type -- '店铺类型' STRING
        ,IF(
            GROUPING(t1.ssp_activity_tag)==0, t1.ssp_activity_tag, "合计"
        ) AS ssp_activity_tag -- '门店商品活跃度标签' STRING
        ,IF(
            GROUPING(t1.province_id)==0, t1.province_id, -99999L
        ) AS province_id -- '省区ID' BIGINT
        ,IF(
            GROUPING(t1.province_name)==0, t1.province_name, "合计"
        ) AS province_name -- '省区名称' STRING
        ,IF(
            GROUPING(t1.city_id)==0, t1.city_id, -99999L
        ) AS city_id -- '市ID' BIGINT
        ,IF(
            GROUPING(t1.city_name)==0, t1.city_name, "合计"
        ) AS city_name -- '市名称' STRING
        ,IF(
            GROUPING(t1.county_id)==0, t1.county_id, -99999L
        ) AS county_id -- '区县ID' BIGINT
        ,IF(
            GROUPING(t1.county_name)==0, t1.county_name, "合计"
        ) AS county_name -- '区县名称' STRING

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
            GROUPING(t1.map_category_level4_id)==0, t1.map_category_level4_id, -99999L
        ) AS map_category_level4_id -- '映射四级类目ID' BIGINT
        ,IF(
            GROUPING(t1.map_category_level4_name)==0, t1.map_category_level4_name, "合计"
        ) AS map_category_level4_name -- '映射四级类目名称' STRING
        ,IF(
            GROUPING(t1.merchant_id)==0, t1.merchant_id, -99999L
        ) AS merchant_id -- '商家ID' BIGINT
        ,IF(
            GROUPING(t1.merchant_name)==0, t1.merchant_name, "合计"
        ) AS merchant_name -- '商家名称' STRING
        ,IF(
            GROUPING(t1.is_sku_valid)==0, t1.is_sku_valid, -99999L
        ) AS is_sku_valid -- '是否有效SKU' BIGINT
        ,IF(
            GROUPING(t1.is_up_today)==0, t1.is_up_today, -99999L
        ) AS is_up_today -- '是否上架SKU' BIGINT

        ,IF(
            GROUPING(t1.page_path)==0, t1.page_path, "合计"
        ) AS page_path -- '页面路径' STRING
        ,IF(
            GROUPING(t1.page_name)==0, t1.page_name, "合计"
        ) AS page_name -- '页面名称' STRING
        ,IF(
            GROUPING(t1.sort_index)==0, t1.sort_index, -99999L
        ) AS sort_index -- '排序位置' BIGINT
        ,IF(
            GROUPING(t1.front_category_id)==0, t1.front_category_id, -99999L
        ) AS front_category_id -- '前台标签页ID' BIGINT
        ,IF(
            GROUPING(t1.front_category_name)==0, t1.front_category_name, "合计"
        ) AS front_category_name -- '前台标签页名称' STRING
        ,IF(
            GROUPING(t1.parent_category_id)==0, t1.parent_category_id, -99999L
        ) AS parent_category_id -- '前台父级标签页ID' BIGINT
        ,IF(
            GROUPING(t1.parent_category_name)==0, t1.parent_category_name, "合计"
        ) AS parent_category_name -- '前台父级标签页面名称' STRING
        ,GROUPING_ID(
            t1.customer_id -- '用户ID' BIGINT
            ,t1.customer_type -- '用户类型' STRING
            ,t1.customer_store_id -- '店铺ID' BIGINT
            ,t1.customer_store_name -- '店铺名称' STRING
            ,t1.store_type -- '店铺类型' STRING
            ,t1.ssp_activity_tag -- '门店商品活跃度标签' STRING
            ,t1.province_id -- '省区ID' BIGINT
            ,t1.province_name -- '省区名称' STRING
            ,t1.city_id -- '市ID' BIGINT
            ,t1.city_name -- '市名称' STRING
            ,t1.county_id -- '区县ID' BIGINT
            ,t1.county_name -- '区县名称' STRING

            ,t1.sku_id -- '商品ID' BIGINT
            ,t1.sku_name -- '商品名称' STRING
            ,t1.category_level4_id -- '四级类目ID' BIGINT
            ,t1.category_level4_name -- '四级类目名称' STRING
            ,t1.category_level1_id -- '一级类目ID' BIGINT
            ,t1.category_level1_name -- '一级类目名称' STRING
            ,t1.map_category_level4_id -- '映射四级类目ID' BIGINT
            ,t1.map_category_level4_name -- '映射四级类目名称' STRING
            ,t1.merchant_id -- '商家ID' BIGINT
            ,t1.merchant_name -- '商家名称' STRING
            ,t1.is_sku_valid -- '是否有效SKU' BIGINT
            ,t1.is_up_today -- '是否上架SKU' BIGINT

            ,t1.page_path -- '页面路径' STRING
            ,t1.page_name -- '页面名称' STRING
            ,t1.sort_index -- '排序位置' BIGINT
            ,t1.front_category_id -- '前台标签页ID' BIGINT
            ,t1.front_category_name -- '前台标签页名称' STRING
            ,t1.parent_category_id -- '前台父级标签页ID' BIGINT
            ,t1.parent_category_name -- '前台父级标签页面名称' STRING
        ) AS grouping_id -- '分组聚合等级ID' BIGINT
        -- 流量基础统计
        ,MAX(t1.exposed_time_max) AS exposed_time_max -- '最晚曝光时间' DATETIME
        ,MIN(t1.exposed_time_min) AS exposed_time_min -- '最早曝光时间' DATETIME
        ,SUM(t1.exposed_cnt) AS exposed_cnt -- '曝光次数' BIGINT
        ,MAX(t1.clicked_time_max) AS clicked_time_max -- '最晚点击时间' DATETIME
        ,MIN(t1.clicked_time_min) AS clicked_time_min -- '最早点击时间' DATETIME
        ,SUM(t1.clicked_cnt) AS clicked_cnt -- '点击次数' BIGINT
        ,MAX(t1.added_time_max) AS added_time_max -- '最晚加购时间' DATETIME
        ,MIN(t1.added_time_min) AS added_time_min -- '最早加购时间' DATETIME
        ,SUM(t1.added_cnt) AS added_cnt -- '加购次数' BIGINT
        
        -- 交叉统计
        ,COUNT(DISTINCT IF(t1.exposed_cnt>0, t1.customer_id, NULL)) AS exposed_customer_num -- '曝光用户数量' BIGINT
        ,COUNT(DISTINCT IF(t1.clicked_cnt>0, t1.customer_id, NULL)) AS clicked_customer_num -- '点击用户数量' BIGINT
        ,COUNT(DISTINCT IF(
            t1.exposed_cnt>0 AND t1.store_type !="STAFF_TYPE"
                AND t1.customer_type = "门店用户" AND t1.is_up_today >0, t1.customer_id, NULL
        )) AS exposed_customer_num_valid -- '曝光有效用户数量' BIGINT
        ,COUNT(DISTINCT IF(
            t1.clicked_cnt>0 AND t1.store_type !="STAFF_TYPE" 
                AND t1.customer_type = "门店用户" AND t1.is_up_today >0, t1.customer_id, NULL
        )) AS clicked_customer_num_valid -- '点击有效用户数量' BIGINT

        ,COUNT(DISTINCT IF(t1.exposed_cnt>0, t1.customer_store_id, NULL)) AS exposed_store_num -- '曝光店铺数量' BIGINT
        ,COUNT(DISTINCT IF(t1.clicked_cnt>0, t1.customer_id, NULL)) AS clicked_store_num -- '点击店铺数量' BIGINT
        ,COUNT(DISTINCT IF(t1.added_cnt>0, t1.customer_id, NULL)) AS added_store_num -- '加购店铺数量' BIGINT
        ,COUNT(DISTINCT IF(
            t1.exposed_cnt>0 AND t1.store_type !="STAFF_TYPE"
                AND t1.customer_type = "门店用户" AND t1.is_up_today >0, t1.customer_store_id, NULL
        )) AS exposed_store_num_valid -- '曝光有效店铺数量' BIGINT
        ,COUNT(DISTINCT IF(
            t1.clicked_cnt>0 AND t1.store_type !="STAFF_TYPE" 
                AND t1.customer_type = "门店用户" AND t1.is_up_today >0, t1.customer_store_id, NULL
        )) AS clicked_store_num_valid -- '点击有效店铺数量' BIGINT
        ,COUNT(DISTINCT IF(
            t1.added_cnt>0 AND t1.store_type !="STAFF_TYPE" 
                AND t1.customer_type = "门店用户" AND t1.is_up_today >0, t1.customer_store_id, NULL
        )) AS added_store_num_valid -- '加购有效店铺数量' BIGINT

        ,COUNT(DISTINCT IF(t1.exposed_cnt>0, t1.sku_id, NULL)) AS exposed_sku_num -- '曝光商品数量' BIGINT
        ,COUNT(DISTINCT IF(t1.clicked_cnt>0, t1.sku_id, NULL)) AS clicked_sku_num -- '点击商品数量' BIGINT
        ,COUNT(DISTINCT IF(t1.added_cnt>0, t1.sku_id, NULL)) AS added_sku_num -- '加购商品数量' BIGINT
        ,COUNT(DISTINCT IF(
            t1.exposed_cnt>0 AND t1.store_type !="STAFF_TYPE" 
                AND t1.customer_type = "门店用户" AND t1.is_up_today >0, t1.sku_id, NULL
        )) AS exposed_sku_num_valid -- '有效曝光商品数量' BIGINT
        ,COUNT(DISTINCT IF(
            t1.clicked_cnt>0 AND t1.store_type !="STAFF_TYPE" 
                AND t1.customer_type = "门店用户" AND t1.is_up_today >0, t1.sku_id, NULL
        )) AS clicked_sku_num_valid -- '有效点击商品数量' BIGINT
        ,COUNT(DISTINCT IF(
            t1.added_cnt>0 AND t1.store_type !="STAFF_TYPE" 
                AND t1.customer_type = "门店用户" AND t1.is_up_today >0, t1.sku_id, NULL
        )) AS added_sku_num_valid -- '有效加购商品数量' BIGINT


        ,COUNT(DISTINCT IF(t1.exposed_cnt>0, t1.category_level4_id, NULL)) AS exposed_cat4_num -- '曝光四级类目数量' BIGINT
        ,COUNT(DISTINCT IF(t1.clicked_cnt>0, t1.category_level4_id, NULL)) AS clicked_cat4_num -- '点击四级类目数量' BIGINT
        ,COUNT(DISTINCT IF(t1.added_cnt>0, t1.category_level4_id, NULL)) AS added_cat4_num -- '加购四级类目数量' BIGINT
        ,COUNT(DISTINCT IF(
            t1.exposed_cnt>0 AND t1.store_type !="STAFF_TYPE" 
                AND t1.customer_type = "门店用户" AND t1.is_up_today >0, t1.category_level4_id, NULL
        )) AS exposed_cat4_num_valid -- '有效曝光四级类目数量' BIGINT
        ,COUNT(DISTINCT IF(
            t1.clicked_cnt>0 AND t1.store_type !="STAFF_TYPE" 
                AND t1.customer_type = "门店用户" AND t1.is_up_today >0, t1.category_level4_id, NULL
        )) AS clicked_cat4_num_valid -- '有效点击四级类目数量' BIGINT
        ,COUNT(DISTINCT IF(
            t1.added_cnt>0 AND t1.store_type !="STAFF_TYPE" 
                AND t1.customer_type = "门店用户" AND t1.is_up_today >0, t1.category_level4_id, NULL
        )) AS added_cat4_num_valid -- '有效加购四级类目数量' BIGINT

        ,MIN(t1.show_standard_price) AS show_standard_price_min -- '最低商城展示件单价' DECIMAL(10,2)
        ,MIN(t1.show_unit_price) AS show_unit_price_min -- '最低商城展示斤单价' DECIMAL(10,2)
        ,MAX(t1.show_standard_price) AS show_standard_price_max -- '最高商城展示件单价' DECIMAL(10,2)
        ,MAX(t1.show_unit_price) AS show_unit_price_max -- '最高商城展示斤单价' DECIMAL(10,2)

        ,COUNT(DISTINCT IF(
            t1.exposed_cnt>0, CONCAT("s:", t1.customer_store_id, "g:", t1.sku_id), NULL
        )) AS exposed_uscp_num -- '门店商品曝光量' BIGINT
        ,COUNT(DISTINCT IF(
            t1.clicked_cnt>0, CONCAT("s:", t1.customer_store_id, "g:", t1.sku_id), NULL
        )) AS clicked_uscp_num -- '门店商品点击量' BIGINT
        ,COUNT(DISTINCT IF(
            t1.added_cnt>0, CONCAT("s:", t1.customer_store_id, "g:", t1.sku_id), NULL
        )) AS added_uscp_num -- '门店商品加购量' BIGINT

        -- 流量详情信息
        ,CONCAT("[", WM_CONCAT(", ", t1.flow_detail), "]") AS flow_detail -- '流量明细信息JSON数组嵌套字典(US+页面排序级别统计)' STRING
    FROM temp t1
    
    GROUP BY t1.dt -- '日期' STRING
        ,t1.mall_id -- '商城ID' BIGINT
        ,t1.mall -- '商城' STRING
    ,GROUPING SETS(
            -- 中心仓/商城
            ()
            -- 页面排序+用户+门店+SKU
            ,(
                t1.customer_id -- '用户ID' BIGINT
                ,t1.customer_type -- '用户类型' STRING
                ,t1.customer_store_id -- '店铺ID' BIGINT
                ,t1.customer_store_name -- '店铺名称' STRING
                ,t1.store_type -- '店铺类型' STRING
                ,t1.ssp_activity_tag -- '门店商品活跃度标签' STRING
                ,t1.province_id -- '省区ID' BIGINT
                ,t1.province_name -- '省区名称' STRING
                ,t1.city_id -- '市ID' BIGINT
                ,t1.city_name -- '市名称' STRING
                ,t1.county_id -- '区县ID' BIGINT
                ,t1.county_name -- '区县名称' STRING

                ,t1.sku_id -- '商品ID' BIGINT
                ,t1.sku_name -- '商品名称' STRING
                ,t1.category_level4_id -- '四级类目ID' BIGINT
                ,t1.category_level4_name -- '四级类目名称' STRING
                ,t1.category_level1_id -- '一级类目ID' BIGINT
                ,t1.category_level1_name -- '一级类目名称' STRING
                ,t1.map_category_level4_id -- '映射四级类目ID' BIGINT
                ,t1.map_category_level4_name -- '映射四级类目名称' STRING
                ,t1.merchant_id -- '商家ID' BIGINT
                ,t1.merchant_name -- '商家名称' STRING
                ,t1.is_sku_valid -- '是否有效SKU' BIGINT
                ,t1.is_up_today -- '是否上架SKU' BIGINT

                ,t1.page_path -- '页面路径' STRING
                ,t1.page_name -- '页面名称' STRING
                ,t1.sort_index -- '排序位置' BIGINT
                ,t1.front_category_id -- '前台标签页ID' BIGINT
                ,t1.front_category_name -- '前台标签页名称' STRING
                ,t1.parent_category_id -- '前台父级标签页ID' BIGINT
                ,t1.parent_category_name -- '前台父级标签页面名称' STRING
            )
            -- 门店+SKU维度
            ,(
                t1.customer_store_id -- '店铺id' BIGINT
                ,t1.customer_store_name -- '店铺名称' STRING
                ,t1.store_type -- '店铺类型' STRING
                ,t1.ssp_activity_tag -- '门店商品活跃度标签' STRING
                ,t1.province_id -- '省ID' BIGINT
                ,t1.province_name -- '省名称' STRING
                ,t1.city_id -- '市ID' BIGINT
                ,t1.city_name -- '市名称' STRING
                ,t1.county_id -- '区ID' BIGINT
                ,t1.county_name -- '区名称' STRING

                ,t1.sku_id -- '商品id' BIGINT
                ,t1.sku_name -- '商品名称' STRING
                ,t1.category_level4_id -- '后台四级类目ID' BIGINT
                ,t1.category_level4_name -- '后台类目名称' STRING
                ,t1.category_level1_id -- '一级类目ID' BIGINT
                ,t1.category_level1_name -- '一级类目名称' STRING
                ,t1.map_category_level4_id -- '映射后台四级类目ID' BIGINT
                ,t1.map_category_level4_name -- '映射后台四级类目名称' STRING
                ,t1.merchant_id -- '商家ID' BIGINT
                ,t1.merchant_name -- '商家名称' STRING

            )
            -- 区县+SKU
            ,(
                t1.province_id -- '省ID' BIGINT
                ,t1.province_name -- '省名称' STRING
                ,t1.city_id -- '市ID' BIGINT
                ,t1.city_name -- '市名称' STRING
                ,t1.county_id -- '区ID' BIGINT
                ,t1.county_name -- '区名称' STRING

                ,t1.sku_id -- '商品id' BIGINT
                ,t1.sku_name -- '商品名称' STRING
                ,t1.category_level4_id -- '后台四级类目ID' BIGINT
                ,t1.category_level4_name -- '后台类目名称' STRING
                ,t1.category_level1_id -- '一级类目ID' BIGINT
                ,t1.category_level1_name -- '一级类目名称' STRING
                ,t1.map_category_level4_id -- '映射后台四级类目ID' BIGINT
                ,t1.map_category_level4_name -- '映射后台四级类目名称' STRING
                ,t1.merchant_id -- '商家ID' BIGINT
                ,t1.merchant_name -- '商家名称' STRING
            )

            -- 区县+四级类目
            ,(
                t1.province_id -- '省ID' BIGINT
                ,t1.province_name -- '省名称' STRING
                ,t1.city_id -- '市ID' BIGINT
                ,t1.city_name -- '市名称' STRING
                ,t1.county_id -- '区ID' BIGINT
                ,t1.county_name -- '区名称' STRING

                ,t1.category_level4_id -- '后台四级类目ID' BIGINT
                ,t1.category_level4_name -- '后台类目名称' STRING
                ,t1.category_level1_id -- '一级类目ID' BIGINT
                ,t1.category_level1_name -- '一级类目名称' STRING
            )
            -- 页面排序
            ,(
                t1.page_path -- '页面路径' STRING
                ,t1.page_name -- '页面名称' STRING
                ,t1.sort_index -- '排序位置' BIGINT
                ,t1.front_category_id -- '前台标签页ID' BIGINT
                ,t1.front_category_name -- '前台标签页名称' STRING
                ,t1.parent_category_id -- '前台父级标签页ID' BIGINT
                ,t1.parent_category_name -- '前台父级标签页面名称' STRING
            )
            -- SKU 维度
            ,(
                t1.sku_id -- '商品id' BIGINT
                ,t1.sku_name -- '商品名称' STRING
                ,t1.category_level4_id -- '后台四级类目ID' BIGINT
                ,t1.category_level4_name -- '后台类目名称' STRING
                ,t1.category_level1_id -- '一级类目ID' BIGINT
                ,t1.category_level1_name -- '一级类目名称' STRING
                ,t1.map_category_level4_id -- '映射后台四级类目ID' BIGINT
                ,t1.map_category_level4_name -- '映射后台四级类目名称' STRING
                ,t1.merchant_id -- '商家ID' BIGINT
                ,t1.merchant_name -- '商家名称' STRING
            )
            -- 四级类目
            ,(
                t1.category_level4_id -- '后台四级类目ID' BIGINT
                ,t1.category_level4_name -- '后台类目名称' STRING
                ,t1.category_level1_id -- '一级类目ID' BIGINT
                ,t1.category_level1_name -- '一级类目名称' STRING
            )
    )
)


,report AS(
    SELECT
        t1.mall_id -- '商城ID' BIGINT
        ,t1.mall -- '商城' STRING
        ,t1.customer_id -- '用户ID' BIGINT
        ,t1.customer_type -- '用户类型' STRING
        ,t1.customer_store_id -- '店铺ID' BIGINT
        ,t1.customer_store_name -- '店铺名称' STRING
        ,t1.store_type -- '店铺类型' STRING
        ,t1.ssp_activity_tag -- '门店商品活跃度标签' STRING
        ,t1.province_id -- '省区ID' BIGINT
        ,t1.province_name -- '省区名称' STRING
        ,t1.city_id -- '市ID' BIGINT
        ,t1.city_name -- '市名称' STRING
        ,t1.county_id -- '区县ID' BIGINT
        ,t1.county_name -- '区县名称' STRING

        ,t1.sku_id -- '商品ID' BIGINT
        ,t1.sku_name -- '商品名称' STRING
        ,t1.category_level4_id -- '四级类目ID' BIGINT
        ,t1.category_level4_name -- '四级类目名称' STRING
        ,t1.category_level1_id -- '一级类目ID' BIGINT
        ,t1.category_level1_name -- '一级类目名称' STRING
        ,t1.map_category_level4_id -- '映射四级类目ID' BIGINT
        ,t1.map_category_level4_name -- '映射四级类目名称' STRING
        ,t1.merchant_id -- '商家ID' BIGINT
        ,t1.merchant_name -- '商家名称' STRING
        ,t1.is_sku_valid -- '是否有效SKU' BIGINT
        ,t1.is_up_today -- '是否上架SKU' BIGINT

        ,t1.page_path -- '页面路径' STRING
        ,t1.page_name -- '页面名称' STRING
        ,t1.sort_index -- '排序位置' BIGINT
        ,t1.front_category_id -- '前台标签页ID' BIGINT
        ,t1.front_category_name -- '前台标签页名称' STRING
        ,t1.parent_category_id -- '前台父级标签页ID' BIGINT
        ,t1.parent_category_name -- '前台父级标签页面名称' STRING
        ,t1.grouping_id -- '分组聚合等级ID' BIGINT

        -- 流量基础统计
        ,t1.exposed_time_max -- '最晚曝光时间' DATETIME
        ,t1.exposed_time_min -- '最早曝光时间' DATETIME
        ,t1.exposed_cnt -- '曝光次数' BIGINT
        ,t1.clicked_time_max -- '最晚点击时间' DATETIME
        ,t1.clicked_time_min -- '最早点击时间' DATETIME
        ,t1.clicked_cnt -- '点击次数' BIGINT
        ,t1.added_time_max -- '最晚加购时间' DATETIME
        ,t1.added_time_min -- '最早加购时间' DATETIME
        ,t1.added_cnt -- '加购次数' BIGINT

        -- 交叉统计
        ,t1.exposed_customer_num -- '曝光用户数量' BIGINT
        ,t1.clicked_customer_num -- '点击用户数量' BIGINT
        ,t1.exposed_customer_num_valid -- '曝光有效用户数量' BIGINT
        ,t1.clicked_customer_num_valid -- '点击有效用户数量' BIGINT

        ,t1.exposed_store_num -- '曝光店铺数量' BIGINT
        ,t1.clicked_store_num -- '点击店铺数量' BIGINT
        ,t1.added_store_num -- '加购店铺数量' BIGINT
        ,t1.exposed_store_num_valid -- '曝光有效店铺数量' BIGINT
        ,t1.clicked_store_num_valid -- '点击有效店铺数量' BIGINT
        ,t1.added_store_num_valid -- '加购有效店铺数量' BIGINT

        ,t1.exposed_sku_num -- '曝光商品数量' BIGINT
        ,t1.clicked_sku_num -- '点击商品数量' BIGINT
        ,t1.added_sku_num -- '加购商品数量' BIGINT
        ,t1.exposed_sku_num_valid -- '有效曝光商品数量' BIGINT
        ,t1.clicked_sku_num_valid -- '有效点击商品数量' BIGINT
        ,t1.added_sku_num_valid -- '有效加购商品数量' BIGINT

        ,t1.exposed_cat4_num -- '曝光四级类目数量' BIGINT
        ,t1.clicked_cat4_num -- '点击四级类目数量' BIGINT
        ,t1.added_cat4_num -- '加购四级类目数量' BIGINT
        ,t1.exposed_cat4_num_valid -- '有效曝光四级类目数量' BIGINT
        ,t1.clicked_cat4_num_valid -- '有效点击四级类目数量' BIGINT
        ,t1.added_cat4_num_valid -- '有效加购四级类目数量' BIGINT

        ,t1.show_standard_price_min -- '最低商城展示件单价' DECIMAL(10,2)
        ,t1.show_unit_price_min -- '最低商城展示斤单价' DECIMAL(10,2)
        ,t1.show_standard_price_max -- '最高商城展示件单价' DECIMAL(10,2)
        ,t1.show_unit_price_max -- '最高商城展示斤单价' DECIMAL(10,2)

        ,t1.exposed_uscp_num -- '门店商品曝光量' BIGINT
        ,t1.clicked_uscp_num -- '门店商品点击量' BIGINT
        ,t1.added_uscp_num -- '门店商品加购量' BIGINT

        -- 流量详情信息
        ,t1.flow_detail -- '流量明细信息JSON数组嵌套字典(US+页面排序级别统计)' STRING
        ,CASE
            WHEN 
                ALL_MATCH(
                    ARRAY(
                        t1.customer_type ,t1.customer_store_name ,t1.store_type ,t1.ssp_activity_tag 
                        ,t1.province_name ,t1.city_name ,t1.county_name  ,t1.sku_name 
                        ,t1.category_level4_name ,t1.category_level1_name ,t1.map_category_level4_name 
                        ,t1.merchant_name ,t1.front_category_name ,t1.parent_category_name 
                        ,t1.page_name
                    )
                    , item -> item = "合计"
                ) AND t1.sort_index = -99999L
            THEN
                "商城"
            WHEN 
                ALL_MATCH(
                    ARRAY(
                        t1.customer_type ,t1.customer_store_name ,t1.store_type ,t1.ssp_activity_tag 
                        ,t1.province_name ,t1.city_name ,t1.county_name  ,t1.sku_name 
                        ,t1.category_level4_name ,t1.category_level1_name ,t1.map_category_level4_name 
                        ,t1.merchant_name ,t1.front_category_name ,t1.parent_category_name 
                        ,t1.page_name
                    )
                    , item -> item != "合计"
                ) AND t1.sort_index > -99999L
            THEN
                "商城+页面排序+用户+门店+SKU"
            WHEN 
                ALL_MATCH(
                    ARRAY(
                        t1.customer_type ,t1.page_name, t1.front_category_name ,t1.parent_category_name 
                    )
                    , item -> item = "合计"
                ) AND
                ALL_MATCH(
                    ARRAY(
                        t1.customer_store_name ,t1.store_type ,t1.ssp_activity_tag 
                        ,t1.province_name ,t1.city_name ,t1.county_name  ,t1.sku_name 
                        ,t1.category_level4_name ,t1.category_level1_name ,t1.map_category_level4_name 
                        ,t1.merchant_name 
                    )

                    
                    , item -> item != "合计"
                ) AND t1.sort_index = -99999L
            THEN
                "商城+门店+SKU"
            WHEN 
                ALL_MATCH(
                    ARRAY(
                        t1.customer_type ,t1.customer_store_name ,t1.store_type ,t1.ssp_activity_tag 
                        ,t1.page_name, t1.front_category_name , t1.parent_category_name 
                    )
                    , item -> item = "合计"
                ) AND
                ALL_MATCH(
                    ARRAY(
                        t1.province_name ,t1.city_name ,t1.county_name  ,t1.sku_name 
                        ,t1.category_level4_name ,t1.category_level1_name ,t1.map_category_level4_name 
                        ,t1.merchant_name 
                    )
                    , item -> item != "合计"
                ) AND t1.sort_index = -99999L
            THEN
                "商城+区县+SKU"
            WHEN 
                ALL_MATCH(
                    ARRAY(
                        t1.customer_type, t1.customer_store_name ,t1.store_type ,t1.ssp_activity_tag ,t1.page_name, t1.sku_name 
                        ,t1.map_category_level4_name,t1.merchant_name ,t1.front_category_name ,t1.parent_category_name 
                    )
                    , item -> item = "合计"
                ) AND
                ALL_MATCH(
                    ARRAY(
                        t1.province_name ,t1.city_name ,t1.county_name
                        ,t1.category_level4_name ,t1.category_level1_name 
                    )
                    , item -> item != "合计"
                ) AND t1.sort_index=-99999L
            THEN
                "商城+区县+四级类目"
            WHEN 
                ALL_MATCH(
                    ARRAY(
                        t1.customer_type ,t1.customer_store_name ,t1.store_type ,t1.ssp_activity_tag 
                        ,t1.province_name ,t1.city_name ,t1.county_name
                        ,t1.sku_name ,t1.category_level4_name ,t1.category_level1_name ,t1.map_category_level4_name 
                        ,t1.merchant_name 
                    )
                    , item -> item = "合计"
                ) AND
                ALL_MATCH(
                    ARRAY(
                        t1.page_name,t1.front_category_name ,t1.parent_category_name 
                        
                    )
                    , item -> item != "合计"
                ) AND t1.sort_index > -99999L
            THEN
                "商城+页面排序"
            WHEN 
                ALL_MATCH(
                    ARRAY(
                         t1.customer_type,t1.customer_store_name ,t1.store_type ,t1.ssp_activity_tag 
                         ,t1.province_name ,t1.city_name ,t1.county_name
                        , t1.page_name,t1.front_category_name ,t1.parent_category_name
                    )
                    , item -> item = "合计"
                ) AND
                ALL_MATCH(
                    ARRAY(
                        t1.sku_name,t1.category_level4_name ,t1.category_level1_name
                        ,t1.map_category_level4_name, t1.merchant_name 
                        
                    )
                    , item -> item != "合计"
                ) AND t1.sort_index = -99999L
            THEN
                "商城+SKU"
            WHEN 
                ALL_MATCH(
                    ARRAY(
                        t1.sku_name ,t1.map_category_level4_name ,t1.merchant_name 
                        ,t1.page_name,t1.front_category_name ,t1.parent_category_name, t1.customer_type
                        ,t1.customer_store_name ,t1.store_type ,t1.ssp_activity_tag 
                        ,t1.province_name ,t1.city_name ,t1.county_name
                    )
                    , item -> item = "合计"
                ) AND
                ALL_MATCH(
                    ARRAY(
                        t1.category_level4_name ,t1.category_level1_name 
                    )
                    , item -> item != "合计"
                ) AND t1.sort_index = -99999L
            THEN
                "商城+四级类目"
            ELSE "其他"
        END AS dimention_type -- '维度类型' STRING

        ,t1.dt -- '日期' STRING
    FROM result t1
)



INSERT OVERWRITE TABLE changsha_flow_fact_daily_asc PARTITION(dt)
SELECT
    t1.mall_id -- '商城ID' BIGINT
    ,t1.mall -- '商城' STRING
    ,t1.customer_id -- '用户ID' BIGINT
    ,t1.customer_type -- '用户类型' STRING
    ,t1.customer_store_id -- '店铺ID' BIGINT
    ,t1.customer_store_name -- '店铺名称' STRING
    ,t1.store_type -- '店铺类型' STRING
    ,t1.ssp_activity_tag -- '门店商品活跃度标签' STRING
    ,t1.province_id -- '省区ID' BIGINT
    ,t1.province_name -- '省区名称' STRING
    ,t1.city_id -- '市ID' BIGINT
    ,t1.city_name -- '市名称' STRING
    ,t1.county_id -- '区县ID' BIGINT
    ,t1.county_name -- '区县名称' STRING

    ,t1.sku_id -- '商品ID' BIGINT
    ,t1.sku_name -- '商品名称' STRING
    ,t1.category_level4_id -- '四级类目ID' BIGINT
    ,t1.category_level4_name -- '四级类目名称' STRING
    ,t1.category_level1_id -- '一级类目ID' BIGINT
    ,t1.category_level1_name -- '一级类目名称' STRING
    ,t1.map_category_level4_id -- '映射四级类目ID' BIGINT
    ,t1.map_category_level4_name -- '映射四级类目名称' STRING
    ,t1.merchant_id -- '商家ID' BIGINT
    ,t1.merchant_name -- '商家名称' STRING
    ,t1.is_sku_valid -- '是否有效SKU' BIGINT
    ,t1.is_up_today -- '是否上架SKU' BIGINT

    ,t1.page_path -- '页面路径' STRING
    ,t1.page_name -- '页面名称' STRING
    ,t1.sort_index -- '排序位置' BIGINT
    ,t1.front_category_id -- '前台标签页ID' BIGINT
    ,t1.front_category_name -- '前台标签页名称' STRING
    ,t1.parent_category_id -- '前台父级标签页ID' BIGINT
    ,t1.parent_category_name -- '前台父级标签页面名称' STRING
    ,t1.grouping_id -- '分组聚合等级ID' BIGINT

    -- 流量基础统计
    ,t1.exposed_time_max -- '最晚曝光时间' DATETIME
    ,t1.exposed_time_min -- '最早曝光时间' DATETIME
    ,t1.exposed_cnt -- '曝光次数' BIGINT
    ,t1.clicked_time_max -- '最晚点击时间' DATETIME
    ,t1.clicked_time_min -- '最早点击时间' DATETIME
    ,t1.clicked_cnt -- '点击次数' BIGINT
    ,t1.added_time_max -- '最晚加购时间' DATETIME
    ,t1.added_time_min -- '最早加购时间' DATETIME
    ,t1.added_cnt -- '加购次数' BIGINT

    -- 交叉统计
    ,t1.exposed_customer_num -- '曝光用户数量' BIGINT
    ,t1.clicked_customer_num -- '点击用户数量' BIGINT
    ,t1.exposed_customer_num_valid -- '曝光有效用户数量' BIGINT
    ,t1.clicked_customer_num_valid -- '点击有效用户数量' BIGINT

    ,t1.exposed_store_num -- '曝光店铺数量' BIGINT
    ,t1.clicked_store_num -- '点击店铺数量' BIGINT
    ,t1.added_store_num -- '加购店铺数量' BIGINT
    ,t1.exposed_store_num_valid -- '曝光有效店铺数量' BIGINT
    ,t1.clicked_store_num_valid -- '点击有效店铺数量' BIGINT
    ,t1.added_store_num_valid -- '加购有效店铺数量' BIGINT

    ,t1.exposed_sku_num -- '曝光商品数量' BIGINT
    ,t1.clicked_sku_num -- '点击商品数量' BIGINT
    ,t1.added_sku_num -- '加购商品数量' BIGINT
    ,t1.exposed_sku_num_valid -- '有效曝光商品数量' BIGINT
    ,t1.clicked_sku_num_valid -- '有效点击商品数量' BIGINT
    ,t1.added_sku_num_valid -- '有效加购商品数量' BIGINT

    ,t1.exposed_cat4_num -- '曝光四级类目数量' BIGINT
    ,t1.clicked_cat4_num -- '点击四级类目数量' BIGINT
    ,t1.added_cat4_num -- '加购四级类目数量' BIGINT
    ,t1.exposed_cat4_num_valid -- '有效曝光四级类目数量' BIGINT
    ,t1.clicked_cat4_num_valid -- '有效点击四级类目数量' BIGINT
    ,t1.added_cat4_num_valid -- '有效加购四级类目数量' BIGINT

    ,t1.show_standard_price_min -- '最低商城展示件单价' DECIMAL(10,2)
    ,t1.show_unit_price_min -- '最低商城展示斤单价' DECIMAL(10,2)
    ,t1.show_standard_price_max -- '最高商城展示件单价' DECIMAL(10,2)
    ,t1.show_unit_price_max -- '最高商城展示斤单价' DECIMAL(10,2)

    ,t1.exposed_uscp_num -- '门店商品曝光量' BIGINT
    ,t1.clicked_uscp_num -- '门店商品点击量' BIGINT
    ,t1.added_uscp_num -- '门店商品加购量' BIGINT

    -- 流量详情信息
    ,IF(
        t1.dimention_type IN ("商城+页面排序+用户+门店+SKU")
        ,COMPRESS(CAST(t1.flow_detail AS BINARY))
        ,NULL
    ) AS flow_detail -- '流量明细信息JSON数组嵌套字典(US+页面排序级别统计)' BINARY
    


    ,t1.dimention_type -- '维度类型' STRING
    ,t1.dt -- '日期' STRING
FROM report t1
WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
;
"""
