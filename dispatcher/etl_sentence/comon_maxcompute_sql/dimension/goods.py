#coding:utf-8
"""
Maxcompute Dimention Goods Table ETL Sentence
1. 商品基础维度表 dim_goods_sentence
2. 商品属性表 dim_changsha_goods_property_sentence
"""


dim_goods_sentence = """
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


,downs AS(
    SELECT
        *
    FROM (
    VALUES 
    (10008333, 0, "2024-12-01"),(10008341, 0, "2024-12-01"),(10009098, 0, "2024-12-01"),(10008895, 0, "2024-12-01"),
    (10008893, 0, "2024-12-01"),(10008897, 0, "2024-12-01"),(10008896, 0, "2024-12-01"),(10006387, 0, "2024-12-01"),
    (10005528, 0, "2024-12-01"),(10005296, 0, "2024-12-01"),(10002417, 0, "2024-12-01"),(10002418, 0, "2024-12-01"),
    (10006296, 0, "2024-12-01"),(10006257, 0, "2024-12-01"),(10005742, 0, "2024-12-01"),(10005830, 0, "2024-12-01"),
    (10005871, 0, "2024-12-01"),(10006193, 0, "2024-12-01"),(10005639, 0, "2024-12-01"),(10005026, 0, "2024-12-01"),
    (10003454, 0, "2024-12-01"),(10004091, 0, "2024-12-01"),(10002326, 0, "2024-12-01"),(10003889, 0, "2024-12-01"),
    (10002325, 0, "2024-12-01"),(10003343, 0, "2024-12-01"),(10003346, 0, "2024-12-01"),(10002307, 0, "2024-12-01"),
    (10000379, 0, "2024-12-01"),(10000077, 0, "2024-12-01"),(10000274, 0, "2024-12-01"),(10000048, 0, "2024-12-01"),
    (10000351, 0, "2024-12-01"),(10000324, 0, "2024-12-01"),(104002, 0, "2024-12-01"),(124231, 0, "2024-12-01"),
    (218345, 0, "2024-12-01"),(10000290, 0, "2024-12-01"),(10000261, 0, "2024-12-01"),(10000276, 0, "2024-12-01"),
    (10000275, 0, "2024-12-01"),(10000189, 0, "2024-12-01"),(10000247, 0, "2024-12-01"),(10000213, 0, "2024-12-01"),
    (10000206, 0, "2024-12-01"),(10000205, 0, "2024-12-01"),(10000064, 0, "2024-12-01"),(298930, 0, "2024-12-01"),
    (10000187, 0, "2024-12-01"),(10000160, 0, "2024-12-01"),(296157, 0, "2024-12-01"),(297896, 0, "2024-12-01"),
    (127352, 0, "2024-12-01"),(278861, 0, "2024-12-01"),(294003, 0, "2024-12-01"),(10000036, 0, "2024-12-01"),
    (200088, 0, "2024-12-01"),(10000070, 0, "2024-12-01"),(290674, 0, "2024-12-01"),(299553, 0, "2024-12-01"),
    (133536, 0, "2024-12-01"),(10000044, 0, "2024-12-01"),(128958, 0, "2024-12-01"),(134582, 0, "2024-12-01"),
    (257235, 0, "2024-12-01"),(93263, 0, "2024-12-01"),(10000105, 0, "2024-12-01"),(10000106, 0, "2024-12-01"),
    (10000107, 0, "2024-12-01"),(203814, 0, "2024-12-01"),(10000101, 0, "2024-12-01"),(230065, 0, "2024-12-01"),
    (230063, 0, "2024-12-01"),(278859, 0, "2024-12-01"),(10000085, 0, "2024-12-01"),(10000003, 0, "2024-12-01"),
    (10000099, 0, "2024-12-01"),(297893, 0, "2024-12-01"),(128729, 0, "2024-12-01"),(235948, 0, "2024-12-01"),
    (10000092, 0, "2024-12-01"),(294536, 0, "2024-12-01"),(10000068, 0, "2024-12-01"),(273791, 0, "2024-12-01"),
    (10000017, 0, "2024-12-01"),(10000076, 0, "2024-12-01"),(10000026, 0, "2024-12-01"),(253743, 0, "2024-12-01"),
    (274950, 0, "2024-12-01"),(119523, 0, "2024-12-01"), (71956, 0, "2024-12-01"), (10000022, 0, "2024-12-01"), 
    (139041, 0, "2024-12-01"), (10000016, 0, "2024-12-01"), (10000037, 0, "2024-12-01"), (294608, 0, "2024-12-01"), 
    (293925, 0, "2024-12-01"),(10000011, 0, "2024-12-01"),(210040, 0, "2024-12-01"),(127438, 0, "2024-12-01"),
    (105706, 0, "2024-12-01"),(105705, 0, "2024-12-01"),(154500, 0, "2024-12-01"),(293586, 0, "2024-12-01"),
    (125008, 0, "2024-12-01"),(282637, 0, "2024-12-01"),(290808, 0, "2024-12-01"), (298819, 0, "2024-12-01"),
    (294527, 0, "2024-12-01"), (273561, 0, "2024-12-01"), (162355, 0, "2024-12-01"), (97614, 0, "2024-12-01"),
    (295794, 0, "2024-12-01"), (97616, 0, "2024-12-01"), (123735, 0, "2024-12-01"), (301139, 0, "2024-12-01"),
    (10000046, 0, "2024-12-01"),(291111, 0, "2024-12-01"), (10000005, 0, "2024-12-01"), (271219, 0, "2024-12-01"),
    (287597, 0, "2024-12-01"), (253700, 0, "2024-12-01"), (289291, 0, "2024-12-01"), (289302, 0, "2024-12-01"),
    (296159, 0, "2024-12-01"), (286078, 0, "2024-12-01"), (287017, 0, "2024-12-01"),(132446, 0, "2024-12-01"),
    (257369, 0, "2024-12-01"), (10000006, 0, "2024-12-01"), (285591, 0, "2024-12-01"), (285593, 0, "2024-12-01"), 
    (285595, 0, "2024-12-01"), (285597, 0, "2024-12-01"), (109423, 0, "2024-12-01"), (10000067, 0, "2024-12-01"),
    (285564, 0, "2024-12-01"), (293885, 0, "2024-12-01"), (93962, 0, "2024-12-01"), (297897, 0, "2024-12-01"),
    (71554, 0, "2024-12-01"), (238434, 0, "2024-12-01"), (266634, 0, "2024-12-01"), (263092, 0, "2024-12-01"), 
    (280652, 0, "2024-12-01"), (252512, 0, "2024-12-01"), (286885, 0, "2024-12-01"),
    (290567, 0, "2024-12-01"), (258209, 0, "2024-12-01"), (293437, 0, "2024-12-01"), (204745, 0, "2024-12-01"), 
    (155971, 0, "2024-12-01"), (10000045, 0, "2024-12-01"), (10000027, 0, "2024-12-01"), (294658, 0, "2024-12-01"),
    (270901, 0, "2024-12-01"), (287373, 0, "2024-12-01"), (290596, 0, "2024-12-01"),
    (10000042, 0, "2024-12-01"), (154333, 0, "2024-12-01"), (10000032, 0, "2024-12-01"), (10000020, 0, "2024-12-01"), 
    (10000018, 0, "2024-12-01"), (10000013, 0, "2024-12-01"), (10000002, 0, "2024-12-01"), (295143, 0, "2024-12-01"),
    (178024, 0, "2024-12-01"), (257689, 0, "2024-12-01"),(121804, 0, "2024-12-01"), (296082, 0, "2024-12-01"),
    (132443, 0, "2024-12-01"), (289293, 0, "2024-12-01"), (124051, 0, "2024-12-01"), (295792, 0, "2024-12-01"),
    (295793, 0, "2024-12-01"), (280588, 0, "2024-12-01"), (125010, 0, "2024-12-01"), (256549, 0, "2024-12-01"),
    (127750, 0, "2024-12-01"), (294064, 0, "2024-12-01"), (98690, 0, "2024-12-01"), (69053, 0, "2024-12-01"),
    (10014790, 0, "2025-06-20"), (10014643, 0, "2025-06-20"), (10014498, 0, "2025-06-20"), (10014871, 0, "2025-06-20"), 
    (10014872, 0, "2025-06-20"), (10013336, 0, "2025-06-20"), (10015022, 0, "2025-06-20"), (10013617, 0, "2025-06-20"),
    (10014377, 0, "2025-06-20"),(10014778, 0, "2025-06-20"),(10014777, 0, "2025-06-20"),(10012472, 0, "2025-06-20"),
    (10012471, 0, "2025-06-20"),(10013019, 0, "2025-06-20"),(10013098, 0, "2025-06-20"),(10013097, 0, "2025-06-20"),
    (10011599, 0, "2025-06-20")
    ) T(sku_id, is_up_today, dt)
)


-- 确定运营活动品
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
        ,BIGINT(t3.mapping_id) AS sku_id
    FROM datawarehouse_max.ods_goods_service_front_category_v2_full t1
    LEFT JOIN datawarehouse_max.ods_goods_service_front_category_v2_full t2
        ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
                AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t1.parent_id = t2.id
        AND t1.dt = t2.dt
    JOIN datawarehouse_max.ods_goods_service_front_category_v2_mapping_full t3
        ON t3.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
                AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t3.dt = t1.dt
        AND t3.front_category_id = t1.id
        AND t3.deleted = 0
    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
                AND DATEADD(CURRENT_DATE(), 0, "dd")
)

-- 运营活动筛选出来的备选品
,candidates AS(

    SELECT
        t1.dt
        ,t1.sku_id
        ,t1.mall_id
        ,WM_CONCAT(DISTINCT "|", t1.candidate_type) AS candidate_labels
        ,WM_CONCAT(DISTINCT "|", t1.price_type_strategy) AS price_type_candidate
        ,WM_CONCAT(DISTINCT "|", t1.enroll_type) AS enroll_type
        ,WM_CONCAT(DISTINCT "|", t1.labels) AS labels
        ,WM_CONCAT(DISTINCT "|", IF(LENGTH(t1.operation_types)>0, t1.operation_types, NULL)) AS operation_types
        ,WM_CONCAT(DISTINCT "|", t1.tbl) AS tbles
        ,CONCAT("{", WM_CONCAT(", ", t1.comments),"}") AS operation_comments
        
    FROM(
        SELECT
            DATE(t1.task_date) AS dt
            ,t1.goods_id AS sku_id
            ,871 AS mall_id
            ,DECODE(
                t1.task_type
                ,"SYSTEM", "推荐好货-策略筛选"
                ,"MANUAL", "推荐好货-手动筛选"
                ,t1.task_type
            ) AS candidate_type
            ,DECODE(
                t1.price_type
                ,"UP", "推荐好货-涨价"
                ,"DOWN", "推荐好货-降价"
                ,"NONE", "推荐好货-不限制"
                ,t1.price_type
            ) AS price_type_strategy
            ,DECODE(
                t1.task_status
                ,"NO_SELECT", "推荐好货-未参与"
                ,"INVOLVED", "推荐好货-已参与"
                ,"NO_INVOLVED", "推荐好货-不参与"
                ,t1.task_status
            ) AS enroll_type
            ,DECODE(
                t1.task_status
                ,"INVOLVED", "推荐好货-已上架"
            ) AS labels
            ,ARRAY_JOIN(ARRAY(
                IF(t1.data_status="DISABLE", "推荐好货-下架", NULL )
            ), "|") AS operation_types
            
            ,IF(t1.task_status="INVOLVED",
                CONCAT(
                    '"推荐好货": {'
                    ,CONCAT_WS(
                        ", ",TRANSFORM(
                            MAP_ENTRIES(
                                MAP(
                                    "限制基准价", t1.limit_base_price, 
                                    "限制新客价", t1.limit_new_customer_price, 
                                    "限制召回客价", t1.limit_recall_customer_price, 
                                    "限制库存", t1.inventory_limit
                                )
                            ), entry -> concat('"', entry.key, '": ', entry.value, '')
                        )
                    ),
                    "}"
                )
            ,NULL) AS comments
            ,"T1" AS tbl
            
        FROM datawarehouse_max.ods_seller_service_demeter_goods_recommend_full t1

        WHERE t1.dt =  DATEADD(CURRENT_DATE(), 0, "dd")
            AND TO_DATE(t1.task_date) BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
                    AND DATEADD(CURRENT_DATE(), 0, "dd")

        UNION
        SELECT
            DATE(t1.dt) AS dt
            ,t1.sku_id
            ,t1.mall_id
            ,t1.candidate_type
            ,NULL AS price_type_strategy
            ,NULL AS enroll_type
            ,NULL AS labels
            ,NULL AS operation_types
            ,NULL AS comments
            ,"T2" AS tbl
        FROM t_changsha_activity_manual t1
        WHERE t1.dt  BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
                    AND DATEADD(CURRENT_DATE(), 0, "dd")
            

        UNION 

        SELECT
            DATE(t1.dt) AS dt
            ,BIGINT(t3.mapping_id) AS sku_id
            ,DECODE(
                t1.stock_id
                ,20L, 871L
            ) AS mall_id
            ,NULL AS candidate_type
            ,NULL AS price_type_strategy

            ,CASE
                WHEN INSTR(t2.store_name, "红星品牌货") >0 THEN "红星品牌货-已参与"
                WHEN INSTR(t2.store_name, "标果兜底") >0 THEN "标果兜底-已参与"
                WHEN INSTR(t2.store_name, "进口专区") >0 THEN "进口专区-已参与"
            END AS enroll_type
            ,CASE
                WHEN INSTR(t2.store_name, "红星品牌货") >0 THEN "红星品牌货-已上架"
                WHEN INSTR(t2.store_name, "标果兜底") >0 THEN "标果兜底-已上架"
                WHEN INSTR(t2.store_name, "进口专区") >0 THEN "进口专区-已上架"
            END AS labels
            ,NULL AS operation_types
            ,NULL AS comments
            ,"T3" AS tbl
            -- ,t2.tab_bar_type
            -- ,t1.tab_bar_type
        FROM datawarehouse_max.ods_goods_service_front_category_v2_full t1
        LEFT JOIN datawarehouse_max.ods_goods_service_front_category_v2_full t2
            ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
                    AND DATEADD(CURRENT_DATE(), 0, "dd")
            AND t1.parent_id = t2.id
            AND t1.dt = t2.dt
        JOIN datawarehouse_max.ods_goods_service_front_category_v2_mapping_full t3
            ON t3.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
                    AND DATEADD(CURRENT_DATE(), 0, "dd")
            AND t3.dt = t1.dt
            AND t3.front_category_id = t1.id
            AND t3.deleted = 0
        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
                    AND DATEADD(CURRENT_DATE(), 0, "dd")
            AND t2.store_name IN ("红星品牌货","标果兜底","进口专区")
            AND t2.tab_bar_type NOT IN ("PLATFORM_SAFETY_NET")
            AND t3.tab_bar_type = "GOODS"

        
        UNION
        SELECT
            DATE(t1.task_date) AS dt
            ,t1.goods_id AS sku_id
            ,871 AS mall_id
            ,DECODE(
                t1.task_type
                ,"SYSTEM", "今日降价-策略筛选"
                ,"MANUAL", "今日降价-手动筛选"
                ,t1.task_type
            ) AS candidate_type
            ,NULL AS price_type_strategy
            ,NULL AS enroll_type
            ,"今日降价-已上架" AS labels
            ,ARRAY_JOIN(ARRAY(
                IF(t1.old_customer_can_see=1, "今日降价-老客户可见", NULL )
                ,IF(t1.new_customer_can_see=1, "今日降价-新客户可见", NULL )
                ,IF(t1.new_customer_can_see=1, "今日降价-召回客户可见", NULL )
                ,IF(t1.data_status="DISABLE", "今日降价-下架", NULL )
            ), "|") AS operation_types
            ,NULL AS comments
            ,"T5" AS tbl
        FROM datawarehouse_max.ods_fruition_core_price_reduced_goods_full t1
        WHERE t1.dt =  DATEADD(CURRENT_DATE(), 0, "dd")
            AND TO_DATE(t1.task_date) BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
                    AND DATEADD(CURRENT_DATE(), 0, "dd")

        UNION 
        SELECT
            DATE(t1.task_date) AS dt
            ,t1.goods_id AS sku_id
            ,871 AS mall_id
            ,IF(
                t1.channel_src != "SYSTEM", "标果兜底-人工筛选-" || DECODE(t1.channel_src, "ADMIN_BACKEND", "商城运营", "FRUITION_BEE", "商家经理", "SELLER", "商家", "ADMIN_BACKEND"),
                "标果兜底-系统筛选"
            ) AS candidate_type
            ,NULL AS price_type_strategy
            ,IF(ISNOTNULL(t1.channel_src), "标果兜底-已参与", "标果兜底-未参与") AS enroll_type
            ,DECODE(t1.participate_status, "SELECTED", "标果兜底-已上架") AS labels
            ,DECODE(t1.enable_status, "DISABLE", "标果兜底-已下架") AS operation_types
            ,'"标果兜底": {"运营周期内天数": "' ||  t1.activity_duration_days || '"}' AS comments
            ,"T6" AS tbl
        FROM datawarehouse_max.ods_seller_service_platform_safety_net_goods_asc t1
        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -3 + -3, "dd") -- 修改为-10天以满足实际需求
            AND DATEADD(CURRENT_DATE(), 0, "dd")
            AND DATE(t1.task_date) BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")

    ) t1
    GROUP BY t1.dt
        ,t1.sku_id
        ,t1.mall_id
)



-- 新老客户价格信息
,prices AS(
    SELECT
        t1.dt
        ,871 AS mall_id
        ,t1.goods_id AS sku_id
        ,CAST(DECODE(
            t1.settlement_type
            ,"STANDER", t1.new_customer_price_piece
            ,"CATTY", t1.new_customer_price_jin
        ) AS DECIMAL(10,2)) AS mct_supplied_price_new -- '新客户商家供货价' DECIMAL(10,2)
        ,CAST(DECODE(
            t1.settlement_type
            ,"STANDER", t1.recall_customer_price_piece
            ,"CATTY", t1.recall_customer_price_jin
        ) AS DECIMAL(10,2)) AS mct_supplied_price_recall -- '召回客户商家供货价' DECIMAL(10,2)
        ,CAST(DECODE(
            t1.settlement_type
            ,"STANDER", t1.unit_price_piece
            ,"CATTY", t1.unit_price_jin
        ) AS DECIMAL(10,2)) AS mct_supplied_price_base -- '基准商家供货价' DECIMAL(10,2)
        
    FROM datawarehouse_max.ods_goods_service_goods_customer_type_price_snapshot_full t1

    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
                AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND ISNULL(t1.last_use_time)
)



-- 1. 基础信息: 商品维度信息，包括上下架、运营标签
,skus AS(
    SELECT
        t1.dt
        ,t1.mall_id -- '商城ID' BIGINT
        ,t2.mall -- '商城' STRING
        ,t1.sku_id -- '商品ID' BIGINTs
        ,t1.sku_name -- '商品名称' STRING

        ,t1.back_category_id -- '原始后台类目ID' BIGINT
        ,t1.back_category_name -- '原始后台类目名称' STRING
        ,t1.category_level1_id -- '一级类目ID' BIGINT
        ,t1.category_level1_name -- '一级类目名称' STRING

        ,NVL(t0.back_category_id, t1.back_category_id) AS map_back_category_id -- '映射后台类目ID' BIGINT
        ,NVL(t0.back_category_name, t1.back_category_name) AS map_back_category_name -- '映射后台类目名称' STRING
        ,CASE
            WHEN t1.mall_id = 871 AND ISNULL(t0.comments) THEN "标果长沙"
            WHEN t1.mall_id != 871 AND ISNULL(t0.comments) THEN "有调整需处理"
        ELSE t0.comments END  AS map_comments -- '映射后备注' STRING
        ,t1.merchant_id -- '商家ID' BIGINT
        ,t1.merchant_name -- '商家名称' STRING
        ,CASE
            WHEN t1.mall_id = 871 AND t1.merchant_id = 10000135 THEN "全品类商家"
            WHEN t1.mall_id = 871 THEN "商家"
        ELSE t1.merchant_type END merchant_type -- '商家类型' STRING
        ,CASE
            WHEN t1.mall_id = 871 AND t1.merchant_id = 10000135 THEN "全品类商家"
            WHEN t1.mall_id = 871 THEN "商家"
        ELSE t1.merchant_type_desc END merchant_type_desc -- '商家类型' STRING
        
        ,t1.operator_user_id -- '商家运营ID' BIGINT
        ,t1.operator_user_name -- '商家运营姓名' STRING
        ,NVL(t7.is_up_today, t1.is_up_today) AS is_up_today -- '当日是否上架' BIGINT
        ,t1.is_sku_valid -- '是否有效SKU' BIGINT
        ,t1.source_type -- '数据来源' STRING
        ,t1.killed -- '是否作废' BIGINT


        ,t4.available_sale_num AS inventory_num -- '当日最新库存数量' BIGINT
        ,t4.update_time AS inventory_latest_update_time -- '当日最后更新时间' DATETIME
        ,t5.attach_url AS video_attach_url -- '视频URL' STRING
        ,t5.update_time AS video_latest_update_time -- '视频最后更新时间' DATETIME
        -- 运营过程信息
        ,t6.labels -- 前端标签信息 STRING
        ,t6.candidate_labels -- '备选前端标签' STRING
        ,t6.price_type_candidate -- '备选价格类型' STRING
        ,t6.enroll_type -- '商家选择类型' STRING
        ,t6.operation_types -- '运营操作类型' STRING
        ,t6.operation_comments -- '运营备注信息' STRING

        ,t1.gross_weight -- '毛重' DECIMAL(10,2)
        ,t1.settlement_type -- '结算类型' STRING
        ,t1.settlement_type_desc -- '结算类型' STRING
        ,t1.sale_price -- '商品单价' DECIMAL(10,2)
        ,CAST(NVL(t8.fixed_commission_rate, t11.cate4_commission_rate) AS DECIMAL(10,4)) AS cate4_commission_rate -- '四级类目抽佣率' DECIMAL(10,4)
        ,CAST(t9.sku_commission_rate AS DECIMAL(10,4)) AS sku_commission_rate -- '商品抽佣率' DECIMAL(10,4)

        ,t10.mct_supplied_price_new -- '新客户商家供货价' DECIMAL(10,2)
        ,t10.mct_supplied_price_recall -- '召回客户商家供货价' DECIMAL(10,2)
        ,NVL(t10.mct_supplied_price_base, t1.sale_price) AS mct_supplied_price_base -- '基准商家供货价' DECIMAL(10,2)
        ,SUM(COALESCE(t7.is_up_today, t1.is_up_today,0) + NVL(t1.is_sku_valid, 0)) OVER(
            PARTITION BY t1.sku_id, t1.mall_id ORDER BY t1.dt ASC
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS valids
        
        ,t1.create_time
    FROM datawarehouse_max.dim_goods_daily_full   t1
    LEFT JOIN t_back_category_mapping t0
        ON t0.old_category_level1_name = t1.category_level1_name
        AND t0.old_back_category_id = t1.back_category_id
        AND NVL(t0.mall_id, 99999) = IF(t1.mall_id=871, 871, 99999)
        AND t1.dt BETWEEN  t0.start_date AND NVL(t0.end_date, "2999-12-31")
    INNER JOIN mall t2
        ON t1.mall_id = t2.mall_id
        
    LEFT JOIN datawarehouse_max.ods_goods_service_goods_inventory_full t4
        ON t4.dt  BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t4.dt = t1.dt
        AND t4.company_id = t1.mall_id
        AND t4.goods_id = t1.sku_id
        AND t4.delete_flag = 0
        AND t4.inventory_type = "ORDINARY"

    LEFT JOIN datawarehouse_max.ods_goods_service_goods_attach_full t5
        ON t5.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
                AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t5.dt = t1.dt
        AND t1.mall_id = 871
        AND t5.goods_id = t1.sku_id
        AND t5.media_type = "VIDEO"
        AND t5.attach_type = "MAIN_VIDEO"
        -- 过滤不必要的重复数据
        AND t5.goods_id <> 196451
        
    LEFT JOIN candidates t6
        ON t6.dt = t1.dt
        AND t6.mall_id = t1.mall_id
        AND t6.sku_id = t1.sku_id

    LEFT JOIN downs t7
        ON t7.sku_id = t1.sku_id
        AND t1.mall_id = 871
        AND t1.dt BETWEEN t7.dt AND "2999-12-31"

    LEFT JOIN datawarehouse_max.ods_fruition_extractor_commission_rate_full t8
        ON t8.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t8.dt = t1.dt
        AND NVL(t8.company_id, 871) = t1.mall_id
        AND t8.back_category_id = t1.back_category_id
        AND t8.deleted = 0


    LEFT JOIN (
        SELECT
            t1.dt
            ,871 AS mall_id
            ,t1.goods_id AS sku_id
            ,t1.fixed_commission_rate AS sku_commission_rate
            ,t1.deleted
            ,t1.update_time
            ,ROW_NUMBER() OVER(PARTITION BY t1.dt, t1.goods_id ORDER BY t1.update_time DESC) AS rnk
        FROM datawarehouse_max.ods_fruition_extractor_goods_commission_rate_full t1
        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
                AND DATEADD(CURRENT_DATE(), 0, "dd")
            AND (deleted=0 OR DATE(t1.update_time)=t1.dt)
        QUALIFY rnk = 1
            
    ) t9
        ON t9.dt = t1.dt
        AND t9.mall_id = t1.mall_id
        AND t9.sku_id = t1.sku_id
        
    LEFT JOIN prices t10
        ON t10.dt = t1.dt
        AND t10.mall_id = t1.mall_id
        AND t10.sku_id =t1.sku_id

    LEFT JOIN changsha_dim_sku_daily_asc t11
        ON t11.dt BETWEEN DATEADD(CURRENT_DATE(), -5 + -3, "dd")
                AND "2025-08-05"
        AND t11.dt = t1.dt
        AND t11.sku_id = t1.sku_id
        AND t11.mall_id = t1.mall_id
        
    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -5 + -3, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")

    
    QUALIFY valids > 0

)



INSERT OVERWRITE TABLE changsha_dim_sku_daily_asc PARTITION(dt)
SELECT
	t1.mall_id -- '商城ID' BIGINT
	,t1.mall -- '商城' STRING
	,t1.sku_id -- '商品ID' BIGINT
	,t1.sku_name -- '商品名称' STRING

	,t1.back_category_id -- '原始后台类目ID' BIGINT
	,t1.back_category_name -- '原始后台类目名称' STRING
	,t1.category_level1_id -- '一级类目ID' BIGINT
	,t1.category_level1_name -- '一级类目名称' STRING

	,t1.map_back_category_id -- '映射后台类目ID' BIGINT
	,t1.map_back_category_name -- '映射后台类目名称' STRING
	,t1.map_comments -- '映射后备注' STRING
	,t1.merchant_id -- '商家ID' BIGINT
	,t1.merchant_name -- '商家名称' STRING
	,t1.merchant_type -- '商家类型' STRING
	,t1.merchant_type_desc -- '商家类型' STRING
        
	,t1.operator_user_id -- '商家运营ID' BIGINT
	,t1.operator_user_name -- '商家运营姓名' STRING
	,t1.is_up_today -- '当日是否上架' BIGINT
	,t1.is_sku_valid -- '是否有效SKU' BIGINT
	,t1.source_type -- '数据来源' STRING
	,t1.killed -- '是否作废' BIGINT

    

	,t1.inventory_num -- '当日最新库存数量' BIGINT
	,t1.inventory_latest_update_time -- '当日最后更新时间' DATETIME
	,t1.video_attach_url -- '视频URL' STRING
	,t1.video_latest_update_time -- '视频最后更新时间' DATETIME

	,t1.labels -- 前端标签信息 STRING
	,t1.candidate_labels -- '备选前端标签' STRING
	,t1.price_type_candidate -- '备选价格类型' STRING
	,t1.enroll_type -- '商家选择类型' STRING
    ,t1.operation_types -- '运营操作类型' STRING
    ,t1.operation_comments -- '运营备注信息' STRING

    ,NVL(t2.ordered_store_num_old_user_7d,0) AS pool_store_old -- '老客户池' BIGINT
    ,NVL(t2.ordered_store_num_new_user_7d,0) AS pool_store_new -- '新客户池' BIGINT
    ,NVL(t2.ordered_store_num_loss_user_7d,0) AS pool_store_recall -- '召回客户池' BIGINT

	,t1.gross_weight -- '毛重' DECIMAL(10,2)
	,t1.settlement_type -- '结算类型' STRING
	,t1.settlement_type_desc -- '结算类型' STRING
	,t1.sale_price -- '商品单价' DECIMAL(10,2)
	,t1.cate4_commission_rate -- '四级类目抽佣率' DECIMAL(10,4)
	,t1.sku_commission_rate -- '商品抽佣率' DECIMAL(10,4)

	,t1.mct_supplied_price_new -- '新客户商家供货价' DECIMAL(10,2)
	,t1.mct_supplied_price_recall -- '召回客户商家供货价' DECIMAL(10,2)
	,t1.mct_supplied_price_base -- '基准商家供货价' DECIMAL(10,2)

    ,t1.create_time -- '创建时间' DATETIME
    ,t1.dt

    
FROM skus t1
LEFT JOIN datawarehouse_max.dws_mct_mall_sku_extra_daily_asc t2
    ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
        AND DATEADD(CURRENT_DATE(), 0, "dd")
    AND t2.dt = t1.dt
    AND t2.mall_id = t1.mall_id
    AND t2.sku_id = t1.sku_id

WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
        AND DATEADD(CURRENT_DATE(), 0, "dd")

;

"""




dim_changsha_goods_property_sentence = """

WITH base AS(
    SELECT
        t1.goods_id AS sku_id 
        ,871 AS mall_id 
        ,t3.goods_prop_base_id AS property_id 
        ,t5.name AS property_name 
        ,t3.property_value_id 
        ,t3.property_value 
        ,IF(LENGTH(t5.property_value_unit)>0, t5.property_value_unit, NULL) AS property_value_unit 
        ,t1.create_time 

        ,t1.update_time 
        ,t1.snapshot_version 
        ,"2" AS back_service_type 
        ,'{"data": ' || t1.snapshot_content || "}"  AS content
        ,t1.dt
        ,ROW_NUMBER() OVER(PARTITION BY t1.dt,t1.goods_id, t5.name ORDER BY t1.update_time DESC) AS rnk
    FROM datawarehouse_max.ods_goods_service_goods_item_snapshot_full t1
    JOIN datawarehouse_max.ods_goods_service_goods_full t2
        ON t2.id = t1.goods_id
        AND t2.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t2.dt = t1.dt
        AND t2.snapshot_version = t1.snapshot_version

    LATERAL VIEW EXPLODE_FLAT_UDTF(
        '{"data": ' || t1.snapshot_content || "}" , "flat", "", "data:goodsPropBaseId propertyValueId propertyValue"
    ) record AS props
        LATERAL VIEW JSON_TUPLE(
            record.props, "goodsPropBaseId", "propertyValueId", "propertyValue"
        ) t3 AS goods_prop_base_id, property_value_id, property_value

    LEFT JOIN datawarehouse_max.ods_goods_service_goods_basic_property_full t5
        ON t5.id = t3.goods_prop_base_id
        AND t5.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t5.dt = t1.dt
    WHERE t1.snapshot_type = "GOODS_PROP"
        AND t1.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
    QUALIFY  rnk = 1
)



INSERT OVERWRITE TABLE changsha_dim_sku_property_daily_full PARTITION(dt)
SELECT
	t1.sku_id 
	,t1.mall_id 
	,t1.property_id 
	,t1.property_name 
	,t1.property_value_id 
	,t1.property_value 
	,t1.property_value_unit 
	,t1.create_time 

	,t1.update_time 
	,t1.snapshot_version 
	,t1.back_service_type 

    ,t1.dt
    
FROM base t1
WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
    AND DATEADD(CURRENT_DATE(), 0, "dd")
;
"""
