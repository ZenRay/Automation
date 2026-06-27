-- 客户售后问题类型
WITH as_problem_selected AS(

    SELECT
        t1.after_sale_id
        ,MAP_AGG('"' || t1.problem_type || '"', t1.point_name) AS customer_applied_problem -- `客户申请问题`
    FROM(
        SELECT
            t1.after_sale_id
            ,t1.problem_type
            ,COLLECT_SET('"' || t1.point_name || '"') AS point_name
        FROM datawarehouse_max.ods_css_demeter_after_sale_order_apply_reason_asc t1
        WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                    AND DATEADD(${date_param}, ${end_offset}, "dd")
            AND t1.deleted =  0
        GROUP BY t1.after_sale_id
            ,t1.problem_type

    ) t1
    GROUP BY t1.after_sale_id
)



-- 门店信息
,stores AS(
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
    WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset} - 30, "dd")
                    AND DATEADD(${date_param}, ${end_offset}, "dd")
    GROUP BY t1.dt
        ,t1.customer_store_id
)


,base AS(
    SELECT
        t1.id AS after_sale_id -- `售后单id`
        ,t1.after_sale_no -- `售后编号`
        ,t1.after_sale_refund_no -- `售后赔付单号`
        ,DECODE(
            t1.apply_type
            ,"OOS", "缺货"
            ,"AFTER_SALE", "售后"
            ,"AFTER_SALE_AGAIN", "申述"
            ,t1.apply_type
        ) AS apply_type -- `售后类型`
        ,t1.order_id -- `订单id`
        ,t1.order_no -- `售后单编号`
        ,t1.order_item_id -- `明细订单id`
        ,t1.order_item_no -- `明细订单编号`
        ,t1.goods_id AS sku_id -- `商品id`
        ,t1.sku_code -- `商品编码`
        ,t4.sku_name -- `商品名称`
        ,t0.defective_rate -- `商品不良率`

        ,t4.back_category_id -- `后台类目id`
        ,t4.back_category_name -- `后台类目名称`
        ,t5.sku_grade -- `等级`
        ,t5.producing_area -- `产地`
        ,t5.packaging_type -- `包装类型`

        ,CASE
            WHEN 
                INSTR(t4.merchant_name, "老顽童") > 0
            THEN "红星老顽童"
            WHEN 
                INSTR(t4.merchant_name, "阿玲果行") > 0
            THEN "阿玲果行"
            ELSE t4.merchant_name
        END AS merchant_name -- `商家名称`



        ,t1.store_id AS customer_store_id -- `店铺id`
        ,t3.province_id -- `省id`
        ,t3.province_name -- `省名称`
        ,t3.city_id -- `市id`
        ,t3.city_name -- `市名称`
        ,t3.county_id -- `区县id`
        ,t3.county_name -- `区县名称`
        ,TRANSFORM(SPLIT(REGEXP_REPLACE(t7.track_file, "\\\[|\\\]", ""), ","), item -> '"' || REGEXP_REPLACE(item, '"', "") || '"') AS track_file -- `送达签收照片`
        ,t1.expected_compensation_amount -- `门店预期赔付金额`
        ,t0.weight_of_bad_fruits -- `门店申请重量`
        ,t0.number_of_bad_fruits -- `门店申请数量`



        ,t2.customer_applied_problem -- `客户申请问题`

        ,TRANSFORM(SPLIT(REGEXP_REPLACE(t1.images, "\\\[|\\\]", ""), ","), item -> '"' || "https://ugc-pro.biaoguoworks.com" || REGEXP_REPLACE(item, '"', "") || '"') AS customer_applied_images -- `客户申请举证图片`
        ,TRANSFORM(SPLIT(t1.video, ","), item -> '"' || "https://ugc-pro.biaoguoworks.com" || item || '"') AS customer_applied_video -- `客户申请举证视频`


        ,t1.handle_user_id -- `客服id`
        ,t1.handle_user_name -- `客服姓名`
        ,DECODE(
            t1.after_sale_type
            ,"BE_SHORT_OF_WEIGHT", "缺斤少两"
            ,"QUALITY_ISSUES", "品质问题"
            ,"APPEARANCE_PROBLEM", "品相问题"
            ,"CARGO_WRONG_EDITION", "货不对版"
            ,"FRESHNESS_PROBLEM", "鲜度问题"
            ,"MATURITY_PROBLEM", "熟度问题"
            ,"SHORTAGE", "缺货"
            ,"LOGISTIC_PROBLEM", "物流问题"
            ,"PURCHASE_AGREE_PAY_CHANNEL", "免赔"
            ,"TASTE_PROBLEMS", "口感问题"
            ,"INSPECTION_REJECTION", "现场拒收"
            ,"OTHERS_MILK", "奶品问题"
            ,t1.after_sale_type
        ) AS after_sale_type -- `判责售后类型`

        ,t1.after_sale_remark -- `客户申请售后备注`
        ,t1.after_sale_type_problem_remark -- `客户申请问题类型`
        ,t1.handle_remark -- `判责后门店接收信息`
        ,t1.handle_remark_service -- `判责后后台信息`


        ,t1.customer_final_recive_money -- `客户实收金额`


        ,t1.status -- `售后单状态`
        ,t1.real_status -- `实际售后单状态`


        ,t1.arrival_time -- `到货时间`
        ,t1.create_time AS apply_time -- `申请时间`
        ,t1.first_processing_time AS claim_time -- `申领时间`
        ,t1.handle_time -- `处理时间`
        ,t0.handle_duration -- `处理时长`
        ,IF(ISNOTNULL(t6.grid_id), "代理人区域", "直营区域") AS operation_region_type -- `运营区域类型`
        ,t1.company_id AS mall_id -- `商城id`

    FROM datawarehouse_max.ods_css_demeter_after_sale_order_full t1
    LEFT JOIN datawarehouse_max.ods_css_demeter_after_sale_order_item_full t0
        ON t0.dt = MAX_PT("datawarehouse_max.ods_css_demeter_after_sale_order_item_full")
        AND t0.after_sale_id = t1.id
    LEFT JOIN as_problem_selected t2
        ON t2.after_sale_id = t1.id

    LEFT JOIN datawarehouse_max.dwt_order_order_item_daily_asc  t01
        ON t01.dt BETWEEN DATEADD(${date_param}, ${start_offset} -30, "dd")
                        AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t01.order_item_id = t1.order_item_id

    LEFT JOIN stores t3
        ON t3.dt = t01.dt
        AND t3.customer_store_id = t1.store_id

    LEFT JOIN datawarehouse_max.dim_goods_daily_full t4
        ON t4.dt BETWEEN DATEADD(${date_param}, ${start_offset} - 30, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t4.dt = t01.dt
        AND t4.sku_id = t1.goods_id
    LEFT JOIN datawarehouse_max.dim_goods_extra_info_daily_full t5
        ON t5.dt BETWEEN DATEADD(${date_param}, ${start_offset} - 30, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t5.dt = t01.dt
        AND t5.sku_id = t1.goods_id
        AND t5.mall_id = t1.company_id

    LEFT JOIN datawarehouse_max.ods_agent_service_agent_agent_grid_full t6
        ON t6.dt BETWEEN DATEADD(${date_param}, ${start_offset} - 30, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t6.company_id = t1.company_id
        AND t6.dt = t01.dt
        AND t6.grid_id = t3.grid_id
        AND t6.status = 1
    LEFT JOIN (
        SELECT
            t1.order_id
            ,t1.order_no
            ,REGEXP_REPLACE(track_file, '\\?watermark[^"]*', '') AS track_file
        FROM datawarehouse_max.ods_bg_scm_demeter_order_deliver_track_asc t1
        WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset} - 30, "dd")
                        AND DATEADD(${date_param}, ${end_offset}, "dd")
            AND t1.deliver_status = "COMPLETE"
            AND t1.company_id = 871
    ) t7
        ON t7.order_no = t01.order_no
    WHERE t1.dt = MAX_PT("datawarehouse_max.ods_css_demeter_after_sale_order_full")
            AND t1.company_id = 871

        AND DATE(t1.create_time) BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                        AND DATEADD(${date_param}, ${end_offset}, "dd")

        AND t1.status != "CANCEL"
)



SELECT
	DATE(t1.apply_time) AS `申请日期`
    ,t1.after_sale_id AS `售后单id`
	,t1.after_sale_no AS `售后编号`
	,t1.after_sale_refund_no AS `售后赔付单号`
	,t1.apply_type AS `售后类型`
	,t1.order_id AS `订单id`
	,t1.order_no AS `售后单编号`
	,t1.order_item_id AS `明细订单id`
	,t1.order_item_no AS `明细订单编号`
	,t1.sku_id AS `商品id`
	,t1.sku_code AS `商品编码`
	,t1.sku_name AS `商品名称`
	,t1.defective_rate AS `商品不良率`

	,t1.back_category_id AS `后台类目id`
	,t1.back_category_name AS `后台类目名称`
	,t1.sku_grade AS `等级`
	,t1.producing_area AS `产地`
	,t1.packaging_type AS `包装类型`

	,t1.merchant_name AS `商家名称`



	,t1.customer_store_id AS `店铺id`
	,t1.province_id AS `省id`
	,t1.province_name AS `省名称`
	,t1.city_id AS `市id`
	,t1.city_name AS `市名称`
	,t1.county_id AS `区县id`
	,t1.county_name AS `区县名称`
    ,t1.track_file AS `送达签收照片`
	,t1.expected_compensation_amount AS `门店预期赔付金额`
	,t1.weight_of_bad_fruits AS `门店申请重量`
	,t1.number_of_bad_fruits AS `门店申请数量`



	,t1.customer_applied_problem AS `客户申请问题`

	,t1.customer_applied_images AS `客户申请举证图片`
	,t1.customer_applied_video AS `客户申请举证视频`


	,t1.handle_user_id AS `客服id`
	,t1.handle_user_name AS `客服姓名`
	,t1.after_sale_type AS `判责售后类型`

	,t1.after_sale_remark AS `客户申请售后备注`
	,t1.after_sale_type_problem_remark AS `客户申请问题类型`
	,t1.handle_remark AS `判责后门店接收信息`
	,t1.handle_remark_service AS `判责后后台信息`
	,t1.customer_final_recive_money AS `客户实收金额`
	,t1.status AS `售后单状态`
	,t1.real_status AS `实际售后单状态`


	,t1.arrival_time AS `到货时间`
	,t1.apply_time AS `申请时间`
	,t1.claim_time AS `申领时间`
	,t1.handle_time AS `处理时间`
	,t1.handle_duration AS `处理时长`

	,t1.mall_id AS `商城id`
    ,t1.operation_region_type AS `运营区域类型`
FROM base t1
WHERE DATE(t1.apply_time) BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                        AND DATEADD(${date_param}, ${end_offset}, "dd")
;