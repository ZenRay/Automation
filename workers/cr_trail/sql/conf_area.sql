WITH base AS(
    SELECT
        t1.id AS region_id -- '区域ID(省市区县行政ID)' BIGINT
        ,t1.name AS region_name -- '区域名称' -- STRING
        ,t1.short_name AS region_alias_name -- '区域别名' STRING


        ,t1.level AS area_level -- '区域等级' BIGINT
        ,CASE t1.level
            WHEN 0 THEN "PROVICE"
            WHEN 1 THEN "CITY"
            WHEN 2 THEN "COUNTY"
            WHEN 3 THEN "STREET"
            WHEN 4 THEN "ADDRESS"
            ELSE "WAIT_FIX" -- 有问题需处理检核
        END AS area_type -- '区域类型' STRING
        ,t1.lng AS longitude -- '经度' STRING
        ,t1.lat AS latitude -- '纬度' STRING
        ,t1.used AS is_valid -- '是否有效' BIGINT
        ,t1.parent_id
        ,t1.dt

    FROM datawarehouse_max.ods_bg_users_demeter_region_full t1

    WHERE t1.dt = MAX_PT("datawarehouse_max.ods_bg_users_demeter_region_full")
        AND t1.used = 1
)


,result AS(
    SELECT
        t1.region_id -- '区域ID(省市区县行政ID)' BIGINT
        ,t1.region_name -- '区域名称' -- STRING
        ,REGEXP_REPLACE(t1.region_name, "\\\d", "") AS fix_region_name
        ,t1.region_alias_name -- '区域别名' STRING


        ,t2.region_id AS address_id -- '地址ID' BIGINT
        ,t2.region_name AS address_name -- '地址名称' STRING
        ,NVL(t3.region_id, t7.region_id) AS street_id -- '街道ID' BIGINT
        ,NVL(t3.region_name, t7.region_name) AS street_name -- '街道名称' STRING
        ,COALESCE(t4.region_id, t8.region_id, t11.region_id) AS county_id -- '区县ID' BIGINT
        ,COALESCE(t4.region_name, t8.region_name, t11.region_name) AS county_name -- '区县名称' STRING
        ,COALESCE(t5.region_id, t9.region_id, t12.region_id, t14.region_id) AS city_id -- '城市ID' BIGINT
        ,COALESCE(t5.region_name, t9.region_name, t12.region_name, t14.region_name) AS city_name -- '城市名称' STRING
        ,COALESCE(t6.region_id, t10.region_id, t13.region_id, t15.region_id, t16.region_id) AS province_id -- '省份ID' BIGINT
        ,COALESCE(t6.region_name, t10.region_name, t13.region_name, t15.region_name, t16.region_name) AS province_name -- '省份名称' STRING

        ,t1.longitude -- '经度' STRING
        ,t1.latitude -- '纬度' STRING

        ,t1.parent_id AS parent_region_id -- '父级区域ID' BIGINT
        ,t1.is_valid -- '是否有效' BIGINT

        ,t1.area_level -- '区域等级' BIGINT
        ,t1.area_type -- '区域类型' STRING
        ,t1.dt -- '日期' STRING
    FROM base t1
    LEFT JOIN base t2
        ON t2.area_type = "ADDRESS"
        AND t2.region_id = t1.region_id
        AND t2.dt = t1.dt
    LEFT JOIN base t3
        ON t3.area_type = "STREET"
        AND t3.region_id = t2.parent_id
        AND t3.dt = t2.dt
    LEFT JOIN base t4
        ON t4.area_type = "COUNTY"
        AND t4.region_id = t3.parent_id
        AND t4.dt = t3.dt

    LEFT JOIN base t5
        ON t5.area_type = "CITY"
        AND t5.region_id = t4.parent_id
        AND t5.dt = t4.dt
    LEFT JOIN base t6
        ON t6.area_type = "PROVICE"
        AND t6.region_id = t5.parent_id
        AND t6.dt = t5.dt

    -- 街道关联
    LEFT JOIN base t7
        ON t7.region_id = t1.region_id
        AND t1.area_type = "STREET"
        AND t7.dt = t1.dt

    LEFT JOIN base t8
        ON t8.region_id = t7.parent_id
        AND t8.dt = t1.dt
        AND t8.area_type = "COUNTY"
    LEFT JOIN base t9
        ON t9.region_id = t8.parent_id
        AND t9.dt = t1.dt
        AND t9.area_type = "CITY"
    LEFT JOIN base t10
        ON t10.region_id = t9.parent_id
        AND t10.dt = t1.dt
        AND t10.area_type = "PROVICE"

    -- 区县关联
    LEFT JOIN base t11
        ON t11.region_id = t1.region_id
        AND t11.dt = t1.dt
        AND t1.area_type = "COUNTY"

    LEFT JOIN base t12
        ON t12.region_id = t11.parent_id
        AND t12.dt = t1.dt
        AND t12.area_type = "CITY"
    LEFT JOIN base t13
        ON t13.region_id = t12.parent_id
        AND t13.dt = t1.dt
        AND t13.area_type = "PROVICE"

    -- 市关联
    LEFT JOIN base t14
        ON t14.region_id = t1.region_id
        AND t14.dt = t1.dt
        AND t1.area_type = "CITY"
    LEFT JOIN base t15
        ON t15.region_id = t14.parent_id
        AND t15.dt = t1.dt
        AND t15.area_type = "PROVICE"

    -- 省信息
    LEFT JOIN base t16
        ON t16.region_id = t1.region_id
        AND t15.dt = t1.dt
        AND t1.area_type="PROVINCE"
)


SELECT
	t1.dt AS `日期`
    ,t1.region_id AS `试验区域id`
	,t1.region_name AS `试验区域名称`
    ,t1.fix_region_name AS `修正区域名称`
	,t1.region_alias_name AS `区域别名`


	,t1.address_id AS `地址id`
	,t1.address_name AS `地址名称`
	,t1.street_id AS `街道id`
	,t1.street_name AS `街道名称`
	,t1.county_id AS `区县id`
	,t1.county_name AS `区县名称`
	,t1.city_id AS `市id`
	,t1.city_name AS `市名称`
	,t1.province_id AS `省id`
	,t1.province_name AS `省名称`

	,t1.longitude AS `经度`
	,t1.latitude AS `纬度`

	,t1.parent_region_id AS `父级区域id`
	,t1.is_valid AS `是否有效`

	,t1.area_level AS `区域等级`
	,t1.area_type AS `试验区域类型`
	,IF(ISNOTNULL(t2.mall_id) AND t1.area_type = "COUNTY", "代理人区域", "自营区域") AS `运营类型`
    -- ,CASE
    --     WHEN
    --         t1.city_name REGEXP "萍乡|邵阳"
    --     THEN "对照组"
    --     WHEN
    --         t1.city_name REGEXP "益阳|咸宁"
    --     THEN "试验组一"
    --     WHEN
    --         t1.city_name REGEXP "湘潭|株洲"
    --     THEN "试验组二"
    --     WHEN
    --         t1.city_name REGEXP "长沙|岳阳"
    --     THEN "试验组三"
    -- END AS `试验分组`
    -- ,"2026-06-18" AS `试验起始日期`
    -- ,"2026-06-24" AS `试验结束日期`
    -- ,"摸底期" AS `试验阶段`

FROM result t1
LEFT JOIN (
    SELECT
        t1.company_id AS mall_id
        ,t1.grid_id
        ,t1.grid_name 
        ,SPLIT(t1.grid_name, "-")[0] AS county_name
        ,t2.area_id
        ,t2.area_name
        ,t1.dt
    FROM datawarehouse_max.ods_agent_service_agent_agent_grid_full t1
    LEFT JOIN datawarehouse_max.dim_grid_daily_full t2
        ON t2.dt = MAX_PT('datawarehouse_max.dim_grid_daily_full')
        AND t2.grid_id = t1.grid_id
    WHERE t1.dt = MAX_PT('datawarehouse_max.ods_agent_service_agent_agent_grid_full')
        AND t1.company_id = 871
        AND t1.status = 1
) t2
    ON t2.county_name= t1.fix_region_name
-- 本次试验过程中只需要到市和区县
WHERE t1.area_type IN ("COUNTY")
;