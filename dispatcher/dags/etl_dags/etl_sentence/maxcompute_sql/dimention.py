#coding:utf-8
"""
Maxcompute Dimention Table ETL Sentence
1. 商品属性表 dim_changsha_goods_property_sentence
2. 门店历史交易统计临时表 t_changsha_store_trade_t0d 用于支撑创建门店维度表的扩展信息
3. 门店维度表 dim_changsha_store_daily_full 该门店表主要关注的是长沙仓的有真实意义的门店，因此其他仓和员工测试店铺不是我们关注的重点
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
        AND t2.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
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
        AND t5.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t5.dt = t1.dt
    WHERE t1.snapshot_type = "GOODS_PROP"
        AND t1.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
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
WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
    AND DATEADD(CURRENT_DATE(), 0, "dd")
;
"""



dim_store_history_trade_temp = """
WITH base AS(

    SELECT /*+ MAPJOIN(t2) */
        t1.customer_store_id -- '店铺id'
        ,STRING(t2.dates) AS dt
        ,t1.mall_id -- '商城id'

        ,MIN(IF(
            t1.dt BETWEEN DATEADD(t2.dates, BIGINT("${start_offset}"), "dd")
                AND t2.dates, t1.dt, NULL
        )) AS earliest_ordered_date -- '最早下单日期'
        ,MAX(IF(
            t1.dt BETWEEN DATEADD(t2.dates, BIGINT("${start_offset}"), "dd")
                AND t2.dates, t1.dt, NULL
        )) AS latest_ordered_date -- '最近下单日期'
        ,COUNT(DISTINCT IF(
            t1.dt BETWEEN DATEADD(t2.dates, BIGINT("${start_offset}"), "dd")
                AND t2.dates, t1.dt, NULL
        )) AS ordered_cnt -- '总下单次数'
        ,SUM(IF(
            t1.dt BETWEEN DATEADD(t2.dates, BIGINT("${start_offset}"), "dd")
                AND t2.dates, t1.ordered_goods_amt, NULL
        )) AS ordered_goods_amt -- '总下单金额'
        ,SUM(IF(
            t1.dt BETWEEN DATEADD(t2.dates, BIGINT("${start_offset}"), "dd")
                AND t2.dates, t1.deliveried_goods_amt, NULL
        )) AS delivered_goods_amt -- '总送货金额'
        ,COUNT(DISTINCT IF(
            t1.dt BETWEEN DATEADD(t2.dates, -59, "dd") AND t2.dates, t1.dt, NULL
        )) AS ordered_cnt_m59dt0d -- '近60天下单次数'
        ,COUNT(DISTINCT IF(
            t1.dt BETWEEN DATEADD(t2.dates, -29, "dd") AND t2.dates, t1.dt, NULL
        )) AS ordered_cnt_m29dt0d -- '近30天下单次数'
        ,COUNT(DISTINCT IF(
            t1.dt BETWEEN DATEADD(t2.dates, -14, "dd") AND t2.dates, t1.dt, NULL
        )) AS ordered_cnt_m14dt0d -- '近15天下单次数'
        ,COUNT(DISTINCT IF(
            t1.dt BETWEEN DATEADD(t2.dates, -6, "dd") AND t2.dates, t1.dt, NULL
        )) AS ordered_cnt_m6dt0d -- '近7天下单次数'
        ,"recent" AS types
    FROM datawarehouse_max.dim_dates_attr t2
    JOIN datawarehouse_max.dwt_order_order_item_daily_asc t1
        ON t1.dt BETWEEN DATEADD(t2.dates, -60, "dd")
                AND t2.dates
    WHERE t2.dates BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t1.dt BETWEEN DATEADD(TO_DATE("${dt}"), -60 + BIGINT("${start_offset}"), "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t1.status <> "CANCEL"

    GROUP BY t1.mall_id
        ,STRING(t2.dates)
        ,t1.customer_store_id
    
    HAVING ISNOTNULL(earliest_ordered_date)

    UNION ALL

    SELECT /*+ MAPJOIN(t2) */
        t1.customer_store_id
        ,STRING(t2.dates) AS dt
        ,record.mall_id
        ,record.earliest_ordered_date
        ,record.latest_ordered_date
        ,record.ordered_cnt
        ,record.ordered_goods_amt
        ,record.delivered_goods_amt
        ,record.ordered_cnt_m59dt0d
        ,record.ordered_cnt_m29dt0d
        ,record.ordered_cnt_m14dt0d
        ,record.ordered_cnt_m6dt0d
        ,"old" AS types
    FROM t_changsha_store_trade_t0d t1
    JOIN datawarehouse_max.dim_dates_attr t2
        ON t2.dates BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
    LATERAL VIEW EXPLODE_FLAT_UDTF(
        '{"data": ' || t1.trade_json || "}" 
            , "flat"
            , ""
            , "data:mall_id earliest_ordered_date latest_ordered_date ordered_cnt " ||
                "ordered_goods_amt delivered_goods_amt ordered_cnt_m59dt0d "||
                "ordered_cnt_m29dt0d ordered_cnt_m14dt0d ordered_cnt_m6dt0d"
    ) records AS item
        LATERAL VIEW JSON_TUPLE(
            records.item, "mall_id", "earliest_ordered_date", "latest_ordered_date"
                , "ordered_cnt", "ordered_goods_amt", "delivered_goods_amt"
                , "ordered_cnt_m59dt0d", "ordered_cnt_m29dt0d"
                , "ordered_cnt_m14dt0d", "ordered_cnt_m6dt0d"
        ) record AS mall_id, earliest_ordered_date, latest_ordered_date, ordered_cnt
            , ordered_goods_amt, delivered_goods_amt, ordered_cnt_m59dt0d
            , ordered_cnt_m29dt0d, ordered_cnt_m14dt0d, ordered_cnt_m6dt0d



    WHERE t1.dt = DATEADD(TO_DATE("${dt}"), BIGINT("${start_offset}") - 1, "dd")
)


INSERT OVERWRITE TABLE t_changsha_store_trade_t0d PARTITION(dt)
SELECT
	t1.customer_store_id -- '店铺id' BIGINT

	,MIN(t1.earliest_ordered_date) AS earliest_ordered_date -- '最早下单日期' DATE
	,MAX(t1.latest_ordered_date) AS latest_ordered_date -- '最近下单日期' DATE

    
    ,"[" || WM_CONCAT(
        ", "
        ,'{"mall_id": ' || t1.mall_id || ', ' ||
            '"earliest_ordered_date": "' || t1.earliest_ordered_date || '", ' ||
            '"latest_ordered_date": "' || t1.latest_ordered_date || '", ' ||
            '"ordered_cnt": ' || t1.ordered_cnt || ", " ||
            '"ordered_goods_amt": ' || t1.ordered_goods_amt || ", " ||
            '"delivered_goods_amt": ' || t1.delivered_goods_amt || ", " ||
            '"ordered_cnt_m59dt0d": ' || t1.ordered_cnt_m59dt0d || ", " ||
            '"ordered_cnt_m29dt0d": ' || t1.ordered_cnt_m29dt0d || ", " ||
            '"ordered_cnt_m14dt0d": ' || t1.ordered_cnt_m14dt0d || ", " ||
            '"ordered_cnt_m6dt0d": ' || t1.ordered_cnt_m6dt0d ||
        '}'
    ) || "]" AS trade_json -- '全部交易的JSON数据' STRING
    ,t1.dt
FROM(
    SELECT
        t1.customer_store_id
        ,t1.mall_id
        ,MIN(t1.earliest_ordered_date) AS earliest_ordered_date
        ,MAX(t1.latest_ordered_date) AS latest_ordered_date
        ,SUM(NVL(t1.ordered_cnt,0)) AS ordered_cnt
        ,SUM(NVL(t1.ordered_goods_amt,0)) AS ordered_goods_amt
        ,SUM(NVL(t1.delivered_goods_amt,0)) AS delivered_goods_amt
        ,MAX(IF(t1.types="recent", t1.ordered_cnt_m59dt0d, 0)) AS ordered_cnt_m59dt0d
        ,MAX(IF(t1.types="recent", t1.ordered_cnt_m29dt0d, 0)) AS ordered_cnt_m29dt0d
        ,MAX(IF(t1.types="recent", t1.ordered_cnt_m14dt0d, 0)) AS ordered_cnt_m14dt0d
        ,MAX(IF(t1.types="recent", t1.ordered_cnt_m6dt0d, 0)) AS ordered_cnt_m6dt0d
        ,t1.dt
    FROM base t1
    
    GROUP BY t1.customer_store_id
        ,t1.mall_id
        ,t1.dt
) t1

WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
    AND DATEADD(CURRENT_DATE(), 0, "dd")

GROUP BY t1.customer_store_id
    ,t1.dt
;
"""


dim_changsha_store_daily_full = """
WITH store_base AS(
    SELECT 
        t1.customer_store_id
        ,t1.dt
        ,WM_CONCAT(DISTINCT ", ", record.mall_id) AS mall_ids
    FROM t_changsha_store_trade_t0d t1
    LATERAL VIEW EXPLODE_FLAT_UDTF(
        '{"data": ' || t1.trade_json || "}" 
            , "flat"
            , ""
            , "data:mall_id earliest_ordered_date latest_ordered_date ordered_cnt " ||
                "ordered_goods_amt delivered_goods_amt ordered_cnt_m59dt0d "||
                "ordered_cnt_m29dt0d ordered_cnt_m14dt0d ordered_cnt_m6dt0d"
    ) records AS item
        LATERAL VIEW JSON_TUPLE(
            records.item, "mall_id", "ordered_cnt_m6dt0d"
        ) record AS mall_id, ordered_cnt_m6dt0d
    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND record.ordered_cnt_m6dt0d > 0
    GROUP BY t1.customer_store_id
        ,t1.dt
)

-- 门店关联的用户信息
,customer AS(
    SELECT
        t1.customer_store_id
        ,CONCAT(
            "["
            , WM_CONCAT(
                ', '
                ,'{"customer_id": ' || t1.customer_id ||
                    ',"age_group": "' || NVL(t1.age_group, "") || '"' ||
                    ',"gender": "' || t1.gender || '"' ||
                    ',"customer_type": "' || t1.customer_type || '"' ||
                    ',"customer_auth_time": "' || NVL(t1.customer_auth_time, "") || '"' ||
                    ',"customer_auth_status": "' || t1.customer_auth_status || '"' ||
                    ',"customer_activate_time": "' || NVL(t1.customer_activate_time, "") || '"' ||
                    ',"customer_create_time": "' || NVL(t1.customer_create_time, "") || '"' || 
                    ',"is_valid": ' || t1.is_valid ||
                    '}'
                )
            ,"]"
        ) AS customer_json
        ,t1.dt
    FROM(
        SELECT
            t1.customer_id
            ,t1.customer_store_id
            ,t2.age_group AS age_group
            ,DECODE(
                t2.gender
                ,0L, "未知"
                ,1L, "男"
                ,2L, "女"
            ) AS gender
            ,DECODE(
                t2.user_type
                ,0L, "门店用户"
                ,10L, "非门店用户"
                ,"" || t2.user_type || ""
            ) AS customer_type
            ,t2.auth_time AS customer_auth_time
            ,t2.auth_status AS customer_auth_status
            ,t2.active_time AS customer_activate_time
            ,t2.create_time AS customer_create_time
            ,t2.acc_status AS is_valid
            ,t1.dt
            ,ROW_NUMBER() OVER(PARTITION BY t1.dt, t1.customer_store_id, t1.customer_id) AS rnk
        FROM datawarehouse_max.dim_user_store_ref_daily_full t1
        LEFT JOIN datawarehouse_max.ods_bg_users_demeter_users_full t2
            ON t2.pt BETWEEN DATE_FORMAT(DATEADD(CURRENT_DATE(), -2, "dd"), "yyyyMMdd000000")
                AND DATE_FORMAT(DATEADD(CURRENT_DATE(), 0, "dd"), "yyyyMMdd000000")
            AND t2.pt = DATE_FORMAT(t1.dt, "yyyyMMdd000000")
            AND t2.id = t1.customer_id
        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        QUALIFY rnk = 1
    ) t1

    GROUP BY t1.customer_store_id
        ,t1.dt
)
-- 配送仓信息
-- TODO: 下面的 mall_id 不严谨，严谨的方式需要通过服务区域来判断它是属于那个商城，需要一个省市区的维度表，在这个维度表上去解决归属商城
,stocks AS(
    SELECT 
        stock_id AS delivery_stock_id
        ,stock_no AS delivery_stock_no
        ,871 AS mall_id
        ,t1.area_id
        ,t1.area_name
        ,t1.province_id
        ,t1.city_id
        ,t1.county_id
        ,t1.street_id
        ,CASE
            WHEN t1.area_id = t1.street_id THEN "STREET"
            WHEN t1.area_id=t1.county_id THEN "COUNTY"
            WHEN t1.area_id = t1.city_id THEN "CITY"
        END area_type
        ,t1.dt
    FROM datawarehouse_max.ods_wms_demeter_stock_service_area_full t1

    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t1.deleted = 0
    GROUP BY stock_id
        ,stock_no
        ,area_id
        ,t1.area_name
        ,t1.province_id
        ,t1.city_id
        ,t1.county_id
        ,t1.street_id
        ,dt
)


,base AS(
    SELECT
        t1.customer_store_id -- '店铺id' BIGINT
        ,t1.customer_store_name -- '店铺名称' STRING
        ,t1.customer_id -- '用户id' BIGINT
        ,t1.customer_name -- '用户名称' STRING
        ,t1.delivery_stock_id -- '配送仓库id' BIGINT
        ,t1.delivery_stock_no -- '配送仓库编码' STRING
        ,t1.province_id -- '省id' BIGINT
        ,t1.province_name -- '省名称' STRING
        ,t1.city_id -- '市id' BIGNT
        ,t1.city_name -- '市名称' STRING
        ,t1.county_id -- '区县id' BIGINT
        ,t1.county_name -- '区县名称' STRING
        ,t1.street_id -- '街道id' BIGINT
        ,t1.street_name -- '街道名称' STRING
        ,t1.address -- '详细地址' STRING
        ,t1.latitude -- '维度' DOUBLE
        ,t1.longitude  -- '经度' DOUBLE
        ,t1.grid_id -- '网格id' BIGINT
        ,t1.area_id -- '片区iD' BIGINT
        ,t1.area_name -- '片区名称' STRING
        ,t1.bd_id -- 'BDID' BIGINT
        ,t1.bd_name -- 'BD姓名' STRING
        ,t1.operator_user_id -- '客服ID' BIGINT
        ,t1.operator_user_name -- '客服姓名' STRING
        ,t1.create_time -- '创建时间' DATETIME
        ,t1.frist_auth_time -- '首次认证时间' DATETIME
        ,t1.active_time -- '激活时间' DATETIME
        ,t1.use_status -- '店铺使用状态' STRING
        ,t1.store_type -- '店铺类型' STRING
        ,t1.link_man -- '联系人' STRING
        ,t1.link_phone -- '联系电话' STRING
        ,t1.dt

        ,t1.mall_id
        ,ROW_NUMBER() OVER(PARTITION BY t1.dt, t1.customer_store_id ORDER BY NVL(t1.mall_id, 999)) AS rnk
    FROM datawarehouse_max.dim_store_daily_full t1

    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
    QUALIFY rnk = 1
)



INSERT OVERWRITE TABLE changsha_dim_store_daily_full PARTITION(dt)
SELECT
    t1.customer_store_id -- '店铺id' BIGINT
    ,t1.customer_store_name -- '店铺名称' STRING
    ,t1.customer_id -- '用户id' BIGINT
    ,t1.customer_name -- '用户名称' STRING
    ,t1.link_man -- '联系人' STRING
    ,t1.link_phone -- '联系电话' STRING
    ,t1.delivery_stock_id -- '配送仓库id' BIGINT
    ,t1.delivery_stock_no -- '配送仓库编码' STRING
    ,t1.province_id -- '省id' BIGINT
    ,t1.province_name -- '省名称' STRING
    ,t1.city_id -- '市id' BIGINT
    ,t1.city_name -- '市名称' STRING
    ,t1.county_id -- '区县id' BIGINT
    ,t1.county_name -- '区县名称' STRING
    ,t1.street_id -- '街道id' BIGINT
    ,t1.street_name -- '街道名称' STRING
    ,t1.address -- '详细地址' STRING
    ,t1.latitude -- '维度' DOUBLE
    ,t1.longitude  -- '经度' DOUBLE
    ,t1.grid_id -- '网格id' BIGINT
    ,t1.area_id -- '片区iD' BIGINT
    ,t1.area_name -- '片区名称' STRING
    ,t1.bd_id -- 'BDID' BIGINT
    ,t1.bd_name -- 'BD姓名' STRING
    ,t1.operator_user_id -- '客服ID' BIGINT
    ,t1.operator_user_name -- '客服姓名' STRING
    ,t1.create_time -- '创建时间' DATETIME
    ,t1.frist_auth_time -- '首次认证时间' DATETIME
    ,t1.active_time -- '激活时间' DATETIME
    ,t1.use_status -- '店铺使用状态' STRING
    ,t1.store_type -- '店铺类型' STRING
    
    ,DECODE(
        t1.use_status, "EFFECTIVE", "有效店铺", "INVALID", "无效店铺", "NON_REGISTER", "未注册", t1.use_status
    ) AS use_status_desc -- '店铺使用状态描述' STRING
    
    ,COALESCE(
        t3.mall_id, t03.mall_id, t003.mall_id
        ,IF(
            ANY_MATCH(SPLIT(t00.mall_ids, ","), item -> BIGINT(item) == 871)
            ,871
            ,NULL
        )
        ,IF(
            ANY_MATCH(SPLIT(t00.mall_ids, ","), item -> BIGINT(item) == 2)
            ,2
            ,NULL
        )
        ,IF(
            ANY_MATCH(SPLIT(t00.mall_ids, ","), item -> BIGINT(item) == 870)
            ,870
            ,NULL
        )
        ,IF(
            ANY_MATCH(SPLIT(t00.mall_ids, ","), item -> BIGINT(item) == 1327)
            ,1327
            ,NULL
        )
        ,IF(
            ANY_MATCH(SPLIT(t00.mall_ids, ","), item -> BIGINT(item) == 2004)
            ,2004
            ,NULL
        )
        ,IF(
            ANY_MATCH(SPLIT(t00.mall_ids, ","), item -> BIGINT(item) == 2798)
            ,2798
            ,NULL
        )
        ,IF(
            ANY_MATCH(SPLIT(t00.mall_ids, ","), item -> BIGINT(item) == 11366)
            ,11366
            ,NULL
        ) 
        ,IF(
            ANY_MATCH(SPLIT(t00.mall_ids, ","), item -> BIGINT(item) == 11368)
            ,11368
            ,NULL
        )
        ,t1.mall_id
    ) AS mall_id -- '商城id' BIGINT

    ,MAX(IF(t1.use_status != "INVALID", 1, 0)) OVER(
        PARTITION BY t1.customer_store_id ORDER BY t1.dt
        ROWS BETWEEN 3 FOLLOWING AND CURRENT ROW
    ) AS is_valid_store_m3dt0d -- 'T-3到T是否有效店铺' BIGINT
    ,t4.earliest_ordered_date -- '最早下单日期' DATE
    ,t4.latest_ordered_date -- '最近下单日期' DATE
    ,t4.trade_json -- '截止当日交易信息JSON数组' STRING
    ,t5.customer_json -- '用户信息JSON数组' STRING
    ,t1.dt
FROM base t1

LEFT JOIN stocks t3
    ON t3.dt = t1.dt
    AND t3.area_type = "STREET"
    AND t3.area_id = t1.street_id
    AND t3.city_id = t1.city_id
    AND t3.county_id = t1.county_id
    AND t3.province_id = t1.province_id


LEFT JOIN stocks t03
    ON t03.dt = t1.dt
    AND t03.area_type= "COUNTY"
    AND t03.area_id = t1.county_id
    AND t03.city_id = t1.city_id
    AND t03.province_id = t1.province_id

LEFT JOIN stocks t003
    ON t003.dt = t1.dt
    AND t003.area_type = "CITY"
    AND t003.area_id = t1.city_id
    AND t003.province_id = t1.province_id

LEFT JOIN t_changsha_store_trade_t0d t4
    ON t4.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
        AND DATEADD(CURRENT_DATE(), 0, "dd")
    AND t4.dt = t1.dt
    AND t4.customer_store_id = t1.customer_store_id

LEFT JOIN customer t5
    ON t5.dt = t1.dt
    AND t5.customer_store_id = t1.customer_store_id
-- TODO: 店铺维度表时可以考虑取消下面的筛选关系
LEFT JOIN store_base t00
    ON t00.customer_store_id = t1.customer_store_id
    AND t00.dt = t1.dt


WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -2, "dd")
        AND DATEADD(CURRENT_DATE(), 0, "dd")

;
"""