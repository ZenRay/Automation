#coding:utf-8
"""
Maxcompute Fact Table ETL Sentence
1. 交易相关事实表 fact_trade_sentence: changsha_trade_fact_daily_asc
"""

fact_trade_sentence = """
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


-- FIXED: 明细订单级售后统计信息
,css AS(
    SELECT
        t3.dt
        ,t1.order_item_id -- '明细订单id' BIGINT
        ,t1.order_id -- '订单id' BIGINT
        ,t3.order_submit_record_id -- '订单提交记录id' BIGINT
        ,t1.mall_id -- '商城id' BIGINT
        ,t2.mall -- '商城' STRING
        ,t1.sku_id -- '商品id' BIGINT
        ,t1.customer_store_id -- '店铺id' BIGINT
        ,t1.customer_id -- '用户id' BIGINT
        ,TO_DATE(t1.statement_date) AS statement_date -- '归属日期' DATE
        ,t1.source_type -- '服务来源' STRING
        ,t3.settlement_type -- '结算类型' STRING
        ,t3.gross_weight -- '毛重' DECIMAL(10,2)
        ,t3.net_weight -- '净重' DECIMAL(10,2)
        ,t3.create_time -- '订单创建时间' DATETIME

        ,WM_CONCAT(
            ", "
            ,'"' || NVL(t1.after_sale_order_channel, t1.apply_type) || '": "' || 
                NVL(t1.after_sale_type_problem, "") || '"'
        ) AS after_sale_apply_problem -- '售后申请问题' STRING
        ,WM_CONCAT(
            ", "
            , '"' || COALESCE(t1.after_sale_type_problem, t1.after_sale_type_problem_title, "") || 
                '": "' ||  COALESCE(t1.after_sale_remark, t1.after_sale_type_problem_remark, "") || '"'
        ) AS after_sale_handle_remark -- '售后处理意见' STRING

        ,WM_CONCAT(
            ","
            ,'"' || COALESCE(t1.after_sale_level1_type, t1.after_sale_type_problem_title, "") || 
                '": "' || COALESCE(t1.after_sale_level2_type, "") || '"'
        ) AS after_sale_handle_problem -- '售后处理判定问题' STRING
        ,SUM(t1.after_sale_number_apply) AS after_sale_applied_num -- '售后申请数量' DECIMAL(20,2)
        ,SUM(NVL(t1.final_refund_amt, t1.customer_final_recive_amt)) AS after_sale_refund_amt -- '售后赔付金额' DECIMAL(10,2)
        ,SUM(t1.after_sale_number) AS after_sale_refund_num -- '售后赔付数量' BIGINT
        ,SUM(t1.liability_amt) AS after_sale_liability_amt -- '总赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
        ,SUM(t1.merchant_liability_amt) AS after_sale_mct_liability_amt -- '商家承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
        ,SUM(t1.platform_liability_amt) AS after_sale_plat_liability_amt -- '平台承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
        ,SUM(t1.logistics_liability_amt) AS after_sale_logist_liability_amt -- '物流承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
        ,SUM(t1.care_refund_amt) AS after_sale_oos_refund_amt -- '缺货赔付金额' DECIMAL(20,2)
        ,SUM(t1.merchat_care_amt) AS after_sale_mct_oos_refund_amt -- '商家承担缺货赔付金额' DECIMAL(20,2)
        ,SUM(t1.logistics_care_amt) AS after_sale_logist_oos_refund_amt -- '物流承担缺货赔付金额' DECIMAL(20,2)

        ,COUNT(DISTINCT t1.after_sale_order_id) AS after_sale_applied_ticket_num -- '售后单提交数量' BIGINT
        ,MIN(t1.create_time) AS after_sale_applied_time_min -- '售后申请最早时间' DATETIME
        ,MAX(t1.create_time) AS after_sale_applied_time_max -- '售后申请最晚时间' DATETIME
        ,MIN(t1.handle_refund_time) AS after_sale_handled_time_min -- '售后处理最早时间' DATETIME
        ,MAX(t1.handle_refund_time) AS after_sale_handled_time_max -- '售后处理最晚时间' DATETIME
        ,COUNT(t1.handle_refund_time) AS after_sale_handled_tick_num -- '首单处理数量' BIGINT

        ,"CSS" AS event_type
    FROM datawarehouse_max.dwt_order_after_sale_daily_asc  t1
    INNER JOIN mall t2
        ON t2.mall_id= t1.mall_id

    LEFT JOIN datawarehouse_max.dwt_order_order_item_daily_asc t3
        ON t3.dt BETWEEN DATEADD(CURRENT_DATE(), -10 + -7, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t3.order_item_id = t1.order_item_id
        AND NVL(t3.source_type, "OLD") = NVL(t1.source_type, "OLD")


    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -7, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t1.status != "CANCEL"

    GROUP BY t3.dt
        ,t1.order_item_id -- '明细订单id' BIGINT
        ,t1.order_id -- '订单id' BIGINT
        ,t3.order_submit_record_id -- '订单提交记录id' BIGINT
        ,t1.mall_id -- '商城id' BIGINT
        ,t2.mall -- '商城' STRING
        ,t1.sku_id -- '商品id' BIGINT
        ,t1.customer_store_id -- '店铺id' BIGINT
        ,t1.customer_id -- '用户id' BIGINT
        ,TO_DATE(t1.statement_date) -- '归属日期' DATE
        ,t1.source_type -- '服务来源' STRING
        ,t3.settlement_type -- '结算类型' STRING
        ,t3.gross_weight -- '毛重' DECIMAL(10,2)
        ,t3.net_weight -- '净重' DECIMAL(10,2)
        ,t3.create_time -- '订单创建时间' DATETIME
)

-- 聚合统计
,temp AS(
    SELECT
        t1.dt
        ,t1.mall_id -- '商城id' BIGINT
        ,t1.mall -- '商城' STRING
        
        ,t1.customer_store_id -- '店铺id' BIGINT
        ,t3.customer_store_name -- '店铺名称' STRING
        ,IF(
            TOUPPER(NVL(t2.ssp_activity_tag, "other")) NOT IN ("LOSS", "OLD"), "NEW", TOUPPER(t2.ssp_activity_tag)
        ) AS ssp_activity_tag -- '门店活跃度标签' STRING
        ,t3.province_id -- '省ID' BIGINT
        ,t3.province_name -- '省名称' STRING
        ,t3.city_id -- '市ID' BIGINT
        ,t3.city_name -- '市名称' STRING
        ,t3.county_id -- '区ID' BIGINT
        ,t3.county_name -- '区名称' STRING
        ,t1.customer_id -- '用户id' BIGINT

        ,t1.sku_id -- '商品id' BIGINT
        ,t4.sku_name -- '商品名称' STRING
        ,t4.back_category_id AS category_level4_id -- '后台四级类目ID' BIGINT
        ,t4.back_category_name AS category_level4_name -- '后台类目名称' STRING
        ,t4.category_level1_id -- '一级类目ID' BIGINT
        ,t4.category_level1_name -- '一级类目名称' STRING
        ,t4.map_back_category_id AS map_category_level4_id -- '映射后台四级类目ID' BIGINT
        ,t4.map_back_category_name AS map_category_level4_name -- '映射后台四级类目名称' STRING
        ,t4.merchant_id -- '商家ID' BIGINT
        ,t4.merchant_name -- '商家名称' STRING
        

        ,t1.ordered_commission_rate -- '订单抽佣率' DECIMAL(10,4)
        ,t1.ordered_catty_price -- '下单斤单价' DECIMAL(10,2)
        ,t1.ordered_stander_price -- '下单件单价' DECIMAL(10,2)
        ,'{"用户ID": ' || t1.customer_id || ", " ||
            '"店铺ID": ' || t1.customer_store_id || ", " ||
            '"商品ID": ' || t1.sku_id || "," ||
            '"下单斤单价": ' || t1.ordered_catty_price || ", " ||
            '"下单件单价": ' || t1.ordered_stander_price || ", " ||
            '"下单抽佣率": ' || t1.ordered_commission_rate || ', ' ||
            '"下单数量": ' || NVL(t1.ordered_goods_num, 0) || ', ' ||
            '"下单金额": ' || NVL(t1.ordered_goods_amt, 0) || ", " ||
            '"下单重量": ' || NVL(t1.ordered_goods_wgt, 0) || ", " ||
            '"最早下单时间": "' || t1.ordered_time_min || '", ' ||
            '"最晚下单时间": "' || t1.ordered_time_max || '", ' ||
            '"下单订单单量": ' || NVL(t1.ordered_ticket_num, 0) || ", " ||
            '"送货件单价": ' || NVL(t1.delivered_stander_price, -999) || ", " ||
            '"送货斤单价": ' || NVL(t1.delivered_unity_price, -999) || ", " ||
            '"送货数量": ' || NVL(t1.delivered_goods_num, 0) || ", " ||
            '"送货重量": ' || NVL(t1.delivered_gross_wgt, 0) || ", " ||
            '"送货金额": ' || NVL(t1.delivered_goods_amt, 0) || ", " ||
            '"送货订单单量": ' || IF(t1.delivered_goods_num>0, t1.ordered_ticket_num, 0) ||
        "}" AS ordered_detail -- '下单详情JSON嵌套字段' STRING
        
        -- 订单交易信息
        ,t1.ordered_goods_num -- '下单数量' BIGINT
        ,t1.ordered_goods_amt -- '下单金额' DECIMAL(10,2)
        ,t1.ordered_goods_wgt -- '下单重量' DECIMAL(10,2)
        ,t1.delivered_goods_num -- '送货数量' BIGINT
        ,t1.delivered_goods_amt -- '送货金额' DECIMAL(10,2)
        ,t1.delivered_gross_wgt -- '送货重量' DECIMAL(10,2)

        ,t1.platform_commission_amt -- '平台抽佣金额' DECIMAL(10,2)
        ,t1.logistics_oos_goods_num -- '物流原因缺货数量' INT
        ,t1.mct_oos_goods_num -- '商家原因缺货数量' INT

        ,t1.ordered_time_min -- '最早订单创建时间' DATETIME
        ,t1.ordered_time_max -- '最早订单创建时间' DATETIME
        ,t1.ordered_item_ticket_num -- '明细订单数量' BIGINT
        ,t1.ordered_ticket_num -- '订单数量' BIGINT
        ,t1.ordered_submit_ticket_num -- '订单提交记录数量' BIGINT


        -- TODO: 目前还缺少费项信息
        ,t1.fee_detail -- '费项详情信息JSON数组' STRING
        ,t1.ordered_customer_id -- '下单用户ID 数组' STRING
        ,t1.ordered_order_id -- '下单订单ID 数组' STRING
        ,t1.ordered_submit_order_id -- '下单提交订单记录ID 数组' STRING

        -- 售后信息
        ,t1.after_sale_applied_ticket_num -- '售后单提交数量' BIGINT
        ,t1.after_sale_applied_time_min -- '售后申请最早时间' DATETIME
        ,t1.after_sale_applied_time_max -- '售后申请最晚时间' DATETIME
        ,t1.after_sale_applied_num -- '售后申请数量' DECIMAL(20,2)
        ,t1.after_sale_refund_num -- '售后赔付数量' BIGINT
        ,t1.after_sale_refund_amt -- '售后赔付金额' DECIMAL(10,2)
        -- TODO: 售后单处理数量数据可能不准确，待验证是否应该使用处理时间来判断
        ,t1.after_sale_handled_tick_num -- '售后单处理数量' BIGINT
        ,t1.after_sale_handled_time_min -- '售后处理最早时间' DATETIME
        ,t1.after_sale_handled_time_max -- '售后处理最晚时间' DATETIME

        ,t1.after_sale_liability_amt -- '总赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
        ,t1.after_sale_mct_liability_amt -- '商家承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
        ,t1.after_sale_plat_liability_amt -- '平台承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
        ,t1.after_sale_logist_liability_amt -- '物流承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
        ,t1.after_sale_oos_refund_amt -- '缺货赔付金额' DECIMAL(20,2)
        ,t1.after_sale_mct_oos_refund_amt -- '商家承担缺货赔付金额' DECIMAL(20,2)
        ,t1.after_sale_logist_oos_refund_amt -- '物流承担缺货赔付金额' DECIMAL(20,2)

        ,t1.after_sale_apply_problem -- '售后申请问题' STRING
        ,t1.after_sale_handle_remark -- '售后处理意见' STRING
        ,t1.after_sale_handle_problem -- '售后处理判定问题' STRING
    FROM(
            SELECT
                t1.dt
                ,t1.mall_id -- '商城id' BIGINT
                ,t1.mall -- '商城' STRING
                ,t1.sku_id -- '商品id' BIGINT
                ,t1.customer_store_id -- '店铺id' BIGINT
                ,t1.customer_id -- '用户id' BIGINT
                ,t1.ordered_commission_rate -- '订单抽佣率' DECIMAL(10,4)
                ,t1.ordered_catty_price -- '下单斤单价' DECIMAL(10,2)
                ,t1.ordered_stander_price -- '下单件单价' DECIMAL(10,2)
                ,CAST(
                    SUM(t1.deliveried_goods_amt) / SUM(t1.deliveried_goods_num)
                    AS DECIMAL(10,2)
                ) AS delivered_stander_price -- '送货件单价' DECIMAL(10,2)
                ,CAST(
                    SUM(t1.deliveried_goods_amt) / SUM(t1.deliveried_gross_wgt)
                    AS DECIMAL(10,2)
                ) AS delivered_unity_price -- '送货斤单价' DECIMAL(10,2)

                -- 订单交易信息
                ,SUM(t1.ordered_goods_num) AS ordered_goods_num -- '下单数量' BIGINT
                ,SUM(t1.ordered_goods_amt) AS ordered_goods_amt -- '下单金额' DECIMAL(10,2)
                ,SUM(t1.ordered_goods_wgt) AS ordered_goods_wgt -- '下单重量' DECIMAL(10,2)
                ,SUM(t1.deliveried_goods_num) AS delivered_goods_num -- '送货数量' BIGINT
                ,SUM(t1.deliveried_goods_amt) AS delivered_goods_amt -- '送货金额' DECIMAL(10,2)
                ,SUM(t1.deliveried_gross_wgt) AS delivered_gross_wgt -- '送货重量' DECIMAL(10,2)

                ,SUM(t1.platform_commission_amt) AS platform_commission_amt -- '平台抽佣金额' DECIMAL(10,2)
                ,SUM(t1.logistics_oos_goods_num) AS logistics_oos_goods_num -- '物流原因缺货数量' INT
                ,SUM(t1.mct_oos_goods_num) AS mct_oos_goods_num -- '商家原因缺货数量' INT

                ,MIN(t1.create_time) AS ordered_time_min -- '最早订单创建时间' DATETIME
                ,MAX(t1.create_time) AS ordered_time_max -- '最早订单创建时间' DATETIME
                ,COUNT(DISTINCT t1.order_item_id) AS ordered_item_ticket_num -- '明细订单数量' BIGINT
                ,COUNT(DISTINCT t1.order_id) AS ordered_ticket_num -- '订单数量' BIGINT
                ,COUNT(DISTINCT t1.order_submit_record_id) AS ordered_submit_ticket_num -- '订单提交记录数量' BIGINT


                -- TODO: 目前还缺少费项信息
                ,MAX(t1.fee_detail) AS fee_detail -- '费项详情信息JSON数组' STRING
                ,WM_CONCAT(DISTINCT ", ", t1.customer_id) AS ordered_customer_id -- '下单用户ID 数组' STRING
                ,WM_CONCAT(DISTINCT ", ", t1.order_id) AS ordered_order_id -- '下单订单ID 数组' STRING
                ,WM_CONCAT(DISTINCT ", ", t1.order_submit_record_id) AS ordered_submit_order_id -- '下单提交订单记录ID 数组' STRING

                -- 售后信息
                ,SUM(t1.after_sale_applied_ticket_num) AS after_sale_applied_ticket_num -- '售后单提交数量' BIGINT
                ,MIN(t1.after_sale_applied_time_min) AS after_sale_applied_time_min -- '售后申请最早时间' DATETIME
                ,MAX(t1.after_sale_applied_time_max) AS after_sale_applied_time_max -- '售后申请最晚时间' DATETIME
                ,SUM(t1.after_sale_applied_num) AS after_sale_applied_num -- '售后申请数量' DECIMAL(20,2)
                ,SUM(t1.after_sale_refund_num) AS after_sale_refund_num -- '售后赔付数量' BIGINT
                ,SUM(t1.after_sale_refund_amt) AS after_sale_refund_amt -- '售后赔付金额' DECIMAL(10,2)
                -- TODO: 售后单处理数量数据可能不准确，待验证是否应该使用处理时间来判断
                ,SUM(t1.after_sale_handled_tick_num) AS after_sale_handled_tick_num -- '售后单处理数量' BIGINT
                ,MIN(t1.after_sale_handled_time_min) AS after_sale_handled_time_min -- '售后处理最早时间' DATETIME
                ,MAX(t1.after_sale_handled_time_max) AS after_sale_handled_time_max -- '售后处理最晚时间' DATETIME

                ,SUM(t1.after_sale_liability_amt) AS after_sale_liability_amt -- '总赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
                ,SUM(t1.after_sale_mct_liability_amt) AS after_sale_mct_liability_amt -- '商家承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
                ,SUM(t1.after_sale_plat_liability_amt) AS after_sale_plat_liability_amt -- '平台承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
                ,SUM(t1.after_sale_logist_liability_amt) AS after_sale_logist_liability_amt -- '物流承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
                ,SUM(t1.after_sale_oos_refund_amt) AS after_sale_oos_refund_amt -- '缺货赔付金额' DECIMAL(20,2)
                ,SUM(t1.after_sale_mct_oos_refund_amt) AS after_sale_mct_oos_refund_amt -- '商家承担缺货赔付金额' DECIMAL(20,2)
                ,SUM(t1.after_sale_logist_oos_refund_amt) AS after_sale_logist_oos_refund_amt -- '物流承担缺货赔付金额' DECIMAL(20,2)

                ,WM_CONCAT(
                    ", "
                    ,t1.after_sale_apply_problem
                ) AS after_sale_apply_problem -- '售后申请问题' STRING
                ,WM_CONCAT(
                    ", "
                    ,t1.after_sale_handle_remark
                ) AS after_sale_handle_remark -- '售后处理意见' STRING
                ,WM_CONCAT(
                    ", "
                    ,t1.after_sale_handle_problem
                ) AS after_sale_handle_problem -- '售后处理判定问题' STRING
            FROM(
                SELECT
                    t1.dt
                    ,t1.order_item_id -- '明细订单id' BIGINT
                    ,t1.order_id -- '订单id' BIGINT
                    ,t1.order_submit_record_id -- '订单提交记录id' BIGINT
                    ,t1.mall_id -- '商城id' BIGINT
                    ,t2.mall -- '商城' STRING
                    ,t1.sku_id -- '商品id' BIGINT
                    ,t1.customer_store_id -- '店铺id' BIGINT
                    ,t1.customer_id -- '用户id' BIGINT
                    ,TO_DATE(t1.statement_date) AS statement_date -- '归属日期' DATE
                    ,t1.source_type -- '服务来源' STRING

                    ,t1.settlement_type -- '结算类型' STRING
                    ,t1.gross_weight -- '毛重' DECIMAL(10,2)
                    ,t1.net_weight -- '净重' DECIMAL(10,2)
                    ,t1.create_time -- '订单创建时间' DATETIME
                    ,CAST(t1.commission_rate AS DECIMAL(10,3)) AS ordered_commission_rate -- '订单抽佣率' DECIMAL(10,4)
                    ,t3.standard_price AS ordered_price -- '下单价格' DECIMAL(10,2)
                    ,CAST(t3.standard_price / t1.gross_weight AS DECIMAL(10,2)) AS ordered_catty_price -- '下单斤单价' DECIMAL(10,2)
                    ,t3.standard_price AS ordered_stander_price -- '下单件单价' DECIMAL(10,2)
                    ,t1.ordered_goods_num -- '下单数量' BIGINT
                    ,t1.ordered_goods_amt -- '下单金额' DECIMAL(10,2)
                    ,t1.ordered_goods_wgt -- '下单重量' DECIMAL(10,2)
                    ,t1.deliveried_goods_num -- '送货数量' BIGINT
                    ,t1.deliveried_goods_amt -- '送货金额' DECIMAL(10,2)
                    ,t1.deliveried_gross_wgt -- '送货重量' DECIMAL(10,2)
                    ,t1.commission_amt AS platform_commission_amt -- '平台抽佣金额' DECIMAL(10,2)
                    ,t1.logistics_oos_goods_num -- '物流原因缺货数量' INT
                    ,t1.mct_oos_goods_num -- '商家原因缺货数量' INT
                    ,NULL AS fee_detail -- '费项详情信息JSON数组' STRING
                        
                    ,t4.after_sale_apply_problem -- '售后申请问题' STRING
                    ,t4.after_sale_handle_remark -- '售后处理意见' STRING
                    ,t4.after_sale_handle_problem -- '售后处理判定问题' STRING
                    ,t4.after_sale_applied_num -- '售后申请数量' DECIMAL(20,2)
                    ,t4.after_sale_refund_amt -- '售后赔付金额' DECIMAL(10,2)
                    ,t4.after_sale_refund_num -- '售后赔付数量' BIGINT
                    ,t4.after_sale_liability_amt -- '总赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
                    ,t4.after_sale_mct_liability_amt -- '商家承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
                    ,t4.after_sale_plat_liability_amt -- '平台承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
                    ,t4.after_sale_logist_liability_amt -- '物流承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
                    ,t4.after_sale_oos_refund_amt -- '缺货赔付金额' DECIMAL(20,2)
                    ,t4.after_sale_mct_oos_refund_amt -- '商家承担缺货赔付金额' DECIMAL(20,2)
                    ,t4.after_sale_logist_oos_refund_amt -- '物流承担缺货赔付金额' DECIMAL(20,2)

                    ,t4.after_sale_applied_ticket_num -- '售后单提交数量' BIGINT
                    ,t4.after_sale_applied_time_min -- '售后申请最早时间' DATETIME
                    ,t4.after_sale_applied_time_max -- '售后申请最晚时间' DATETIME
                    ,t4.after_sale_handled_time_min -- '售后处理最早时间' DATETIME
                    ,t4.after_sale_handled_time_max -- '售后处理最晚时间' DATETIME
                    ,t4.after_sale_handled_tick_num -- '首单处理数量' BIGINT
                FROM datawarehouse_max.dwt_order_order_item_daily_asc t1
                JOIN mall t2
                    ON t2.mall_id= t1.mall_id
                LEFT JOIN datawarehouse_max.dwd_order_item_daily_asc t3
                    ON t3.dt BETWEEN DATEADD(CURRENT_DATE(), -7, "dd")
                        AND DATEADD(CURRENT_DATE(), 0, "dd")
                    AND t3.dt = t1.dt
                    AND t3.order_item_id = t1.order_item_id
                    AND NVL(t3.source_type, "OLD") = NVL(t1.source_type, "OLD")
                -- 关联订单级售后信息
                LEFT JOIN css t4
                    ON t4.order_item_id = t1.order_item_id
                    AND t4.mall_id = t1.mall_id
                    AND t4.dt BETWEEN DATEADD(CURRENT_DATE(), -7, "dd")
                        AND DATEADD(CURRENT_DATE(), 0, "dd")

                WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -7, "dd")
                        AND DATEADD(CURRENT_DATE(), 0, "dd")
                    AND t1.status != "CANCEL"


                    
            ) t1
            WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -7, "dd")
                        AND DATEADD(CURRENT_DATE(), 0, "dd")
            GROUP BY t1.dt
                ,t1.mall_id -- '商城id' BIGINT
                ,t1.mall -- '商城' STRING
                ,t1.sku_id -- '商品id' BIGINT
                ,t1.customer_id -- '用户id' BIGINT
                ,t1.customer_store_id -- '店铺id' BIGINT
                ,t1.ordered_commission_rate -- '订单抽佣率' DECIMAL(10,4)
                ,t1.ordered_catty_price -- '下单斤单价' DECIMAL(10,2)
                ,t1.ordered_stander_price -- '下单件单价' DECIMAL(10,2)
    ) t1
    LEFT JOIN changsha_dim_sku_store_tag_daily_asc t2
        ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -7, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t2.dt = t1.dt
        AND t2.mall_id = t1.mall_id
        AND t2.customer_store_id = t1.customer_store_id
        AND t2.sku_id = t1.sku_id

    LEFT JOIN changsha_dim_store_daily_full t3
        ON t3.dt BETWEEN DATEADD(CURRENT_DATE(), -7, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t3.dt = t1.dt
        AND t3.customer_store_id = t1.customer_store_id
    LEFT JOIN changsha_dim_sku_daily_asc t4
        ON t4.dt BETWEEN DATEADD(CURRENT_DATE(), -7, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        AND t4.dt = t1.dt
        AND t4.mall_id = t1.mall_id
        AND t4.sku_id = t1.sku_id
    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -7, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")
        
)





,result AS(
    SELECT
        t1.dt
        ,t1.mall_id -- '商城id' BIGINT
        ,t1.mall -- '商城' STRING

        ,IF(
            GROUPING(t1.customer_store_id)==0, t1.customer_store_id, -99999L
        ) AS customer_store_id -- '店铺id' BIGINT
        ,IF(
            GROUPING(t1.customer_store_name)==0, t1.customer_store_name, "合计"
        ) AS customer_store_name -- '店铺名称' STRING
        ,IF(
            GROUPING(t1.ssp_activity_tag)==0, t1.ssp_activity_tag, "合计"
        ) AS ssp_activity_tag -- '门店商品活跃度标签' STRING
        ,IF(
            GROUPING(t1.province_id)==0, t1.province_id, -99999L
        ) AS province_id -- '省ID' BIGINT
        ,IF(
            GROUPING(t1.province_name)==0, t1.province_name, "合计"
        ) AS province_name -- '省名称' STRING
        ,IF(
            GROUPING(t1.city_id)==0, t1.city_id, -99999L
        ) AS city_id -- '市ID' BIGINT
        ,IF(
            GROUPING(t1.city_name)==0, t1.city_name, "合计"
        ) AS city_name -- '市名称' STRING
        ,IF(
            GROUPING(t1.county_id)==0, t1.county_id, -99999L
        ) AS county_id -- '区ID' BIGINT
        ,IF(
            GROUPING(t1.county_name)==0, t1.county_name, "合计"
        ) AS county_name -- '区名称' STRING

        ,IF(
            GROUPING(t1.sku_id)==0, t1.sku_id, -99999L
        ) AS sku_id -- '商品id' BIGINT
        ,IF(
            GROUPING(t1.sku_name)==0, t1.sku_name, "合计"
        ) AS sku_name -- '商品名称' STRING
        ,IF(
            GROUPING(t1.category_level4_id)==0, t1.category_level4_id, -99999L
        ) AS category_level4_id -- '后台四级类目ID' BIGINT
        ,IF(
            GROUPING(t1.category_level4_name)==0, t1.category_level4_name, "合计"
        ) AS category_level4_name -- '后台类目名称' STRING
        ,IF(
            GROUPING(t1.category_level1_id)==0, t1.category_level1_id, -99999L
        ) AS category_level1_id -- '一级类目ID' BIGINT
        ,IF(
            GROUPING(t1.category_level1_name)==0, t1.category_level1_name, "合计"
        ) AS category_level1_name -- '一级类目名称' STRING
        ,IF(
            GROUPING(t1.map_category_level4_id)==0, t1.map_category_level4_id, -99999L
        ) AS map_category_level4_id -- '映射后台四级类目ID' BIGINT
        ,IF(
            GROUPING(t1.map_category_level4_name)==0, t1.map_category_level4_name, "合计"
        ) AS map_category_level4_name -- '映射后台四级类目名称' STRING
        ,IF(
            GROUPING(t1.merchant_id)==0, t1.merchant_id, -99999L
        ) AS merchant_id -- '商家ID' BIGINT
        ,IF(
            GROUPING(t1.merchant_name)==0, t1.merchant_name, "合计"
        ) AS merchant_name -- '商家名称' STRING
        ,GROUPING_ID(
            t1.customer_store_id -- '店铺id' BIGINT
            ,t1.customer_store_name
            ,t1.ssp_activity_tag -- '门店商品活跃度标签' STRING
            ,t1.province_id -- '省ID' BIGINT
            ,t1.province_name -- '省名称' STRING
            ,t1.city_id -- '市ID' BIGINT
            ,t1.city_name -- '市名称' STRING
            ,t1.county_id -- '区ID' BIGINT
            ,t1.county_name -- '区名称' STRING

            ,t1.sku_id -- '商品id' BIGINT
            ,t1.category_level4_id -- '后台四级类目ID' BIGINT
            ,t1.category_level4_name -- '后台类目名称' STRING
            ,t1.category_level1_id -- '一级类目ID' BIGINT
            ,t1.category_level1_name -- '一级类目名称' STRING
            ,t1.map_category_level4_id -- '映射后台四级类目ID' BIGINT
            ,t1.map_category_level4_name -- '映射后台四级类目名称' STRING
            ,t1.merchant_id -- '商家ID' BIGINT
            ,t1.merchant_name -- '商家名称' STRING
        ) AS grouping_id -- '分组聚合等级ID' BIGINT
        -- 订单信息
        ,CONCAT("[", WM_CONCAT(", ", t1.ordered_detail), "]") AS ordered_detail -- '下单详情JSON嵌套字典(SSP价格级别统计)' STRING
        ,CONCAT(
            "[", ARRAY_JOIN(ARRAY_DISTINCT(SPLIT(WM_CONCAT(", ", t1.ordered_customer_id), ", ")), ", "), "]"
        ) AS ordered_customer_id -- '下单用户ID 数组' STRING
        ,CONCAT(
            "[", ARRAY_JOIN(ARRAY_DISTINCT(SPLIT(WM_CONCAT(", ", t1.ordered_order_id), ", ")), ", "), "]"
        ) AS ordered_order_id -- '下单订单ID 数组' STRING
        ,CONCAT(
            "[", ARRAY_JOIN(ARRAY_DISTINCT(SPLIT(WM_CONCAT(", ", t1.ordered_submit_order_id), ", ")), ", "), "]"
        ) AS ordered_submit_order_id -- '下单提交订单记录ID 数组' STRING
        -- TODO: 目前还缺少费项信息
        ,WM_CONCAT(", ", t1.fee_detail) AS fee_detail -- '费项详情信息JSON数组' STRING

        ,MIN(t1.ordered_catty_price) AS ordered_catty_price_min -- '最小下单斤单价' DECIMAL(10,2)
        ,MAX(t1.ordered_catty_price) AS ordered_catty_price_max -- '最大下单斤单价' DECIMAL(10,2)
        ,MIN(t1.ordered_stander_price) AS ordered_stander_price_min -- '最小下单件单价' DECIMAL(10,2)
        ,MAX(t1.ordered_stander_price) AS ordered_stander_price_max -- '最大下单件单价' DECIMAL(10,2)
        ,MIN(t1.ordered_time_min) AS ordered_time_min -- '最早订单创建时间' DATETIME
        ,MAX(t1.ordered_time_max) AS ordered_time_max -- '最早订单创建时间' DATETIME
        ,SUM(t1.ordered_goods_num) AS ordered_goods_num -- '下单数量' BIGINT
        ,SUM(t1.ordered_goods_amt) AS ordered_goods_amt -- '下单金额' DECIMAL(10,2)
        ,SUM(t1.ordered_goods_wgt) AS ordered_goods_wgt -- '下单重量' DECIMAL(10,2)
        ,SUM(t1.delivered_goods_num) AS delivered_goods_num -- '送货数量' BIGINT
        ,SUM(t1.delivered_goods_amt) AS delivered_goods_amt -- '送货金额' DECIMAL(10,2)
        ,SUM(t1.delivered_gross_wgt) AS delivered_gross_wgt -- '送货重量' DECIMAL(10,2)
        ,SUM(t1.platform_commission_amt) AS platform_commission_amt -- '平台抽佣金额' DECIMAL(10,2)
        ,SUM(t1.logistics_oos_goods_num) AS logistics_oos_goods_num -- '物流原因缺货数量' INT
        ,SUM(t1.mct_oos_goods_num) AS mct_oos_goods_num -- '商家原因缺货数量' INT
        ,SUM(t1.ordered_item_ticket_num) AS ordered_item_ticket_num -- '明细订单数量' BIGINT
        ,SIZE(ARRAY_DISTINCT(SPLIT(WM_CONCAT(", ", t1.ordered_order_id), ", "))) AS ordered_ticket_num -- '订单数量' BIGINT
        ,SIZE(ARRAY_DISTINCT(SPLIT(WM_CONCAT(", ", t1.ordered_submit_order_id), ", "))) AS ordered_submit_ticket_num -- '订单提交记录数量' BIGINT

        -- 售后信息
        ,MIN(t1.after_sale_applied_time_min) AS after_sale_applied_time_min -- '售后申请最早时间' DATETIME
        ,MAX(t1.after_sale_applied_time_max) AS after_sale_applied_time_max -- '售后申请最晚时间' DATETIME
        ,MIN(t1.after_sale_handled_time_min) AS after_sale_handled_time_min -- '售后处理最早时间' DATETIME
        ,MAX(t1.after_sale_handled_time_max) AS after_sale_handled_time_max -- '售后处理最晚时间' DATETIME

        ,SUM(t1.after_sale_applied_ticket_num) AS after_sale_applied_ticket_num -- '售后单提交数量' BIGINT
        ,SUM(t1.after_sale_applied_num) AS after_sale_applied_num -- '售后申请数量' DECIMAL(20,2)
        ,SUM(t1.after_sale_refund_num) AS after_sale_refund_num -- '售后赔付数量' BIGINT
        ,SUM(t1.after_sale_refund_amt) AS after_sale_refund_amt -- '售后赔付金额' DECIMAL(10,2)
        -- TODO: 售后单处理数量数据可能不准确，待验证是否应该使用处理时间来判断
        ,SUM(t1.after_sale_handled_tick_num) AS after_sale_handled_tick_num -- '售后单处理数量' BIGINT
        ,SUM(t1.after_sale_liability_amt) AS after_sale_liability_amt -- '总赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
        ,SUM(t1.after_sale_mct_liability_amt) AS after_sale_mct_liability_amt -- '商家承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
        ,SUM(t1.after_sale_plat_liability_amt) AS after_sale_plat_liability_amt -- '平台承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
        ,SUM(t1.after_sale_logist_liability_amt) AS after_sale_logist_liability_amt -- '物流承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
        ,SUM(t1.after_sale_oos_refund_amt) AS after_sale_oos_refund_amt -- '缺货赔付金额' DECIMAL(20,2)
        ,SUM(t1.after_sale_mct_oos_refund_amt) AS after_sale_mct_oos_refund_amt -- '商家承担缺货赔付金额' DECIMAL(20,2)
        ,SUM(t1.after_sale_logist_oos_refund_amt) AS after_sale_logist_oos_refund_amt -- '物流承担缺货赔付金额' DECIMAL(20,2)

        ,WM_CONCAT(", ", t1.after_sale_apply_problem) AS after_sale_apply_problem -- '售后申请问题' STRING
        ,WM_CONCAT(", ", t1.after_sale_handle_remark) AS after_sale_handle_remark -- '售后处理意见' STRING
        ,WM_CONCAT(", ", t1.after_sale_handle_problem) AS after_sale_handle_problem -- '售后处理判定问题' STRING

        -- 交叉统计
        ,COUNT(DISTINCT IF(t1.ordered_goods_num>0, t1.customer_store_id, NULL)) AS ordered_store_num -- '下单店铺数' BIGINT
        ,COUNT(DISTINCT IF(t1.ordered_goods_num>0, t1.sku_id, NULL)) AS ordered_sku_num -- '下单商品数' BIGINT
        ,COUNT(DISTINCT IF(t1.ordered_goods_num>0, t1.category_level4_id, NULL)) AS ordered_cat4_num -- '下单四级类目数' BIGINT
        ,COUNT(DISTINCT IF(t1.ordered_goods_num>0, t1.category_level1_id, NULL)) AS ordered_cat1_num -- '下单一级类目数' BIGINT
        ,COUNT(DISTINCT IF(t1.ordered_goods_num>0, CONCAT("s:", t1.customer_store_id, "g:", t1.sku_id), NULL)) AS ordered_uscp_num -- '门店商品下单量' BIGINT
        
    FROM temp t1


    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -7, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")


    GROUP BY t1.dt
        ,t1.mall_id -- '商城id' BIGINT 
        ,t1.mall -- '商城' STRING

    ,GROUPING SETS (
        -- 中心仓/商城 DONE
        ()
        -- 门店+SKU维度 DONE
        ,(
            t1.customer_store_id -- '店铺id' BIGINT
            ,t1.customer_store_name -- '店铺名称' STRING
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
        -- 区县SKU DONE
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

        -- 区县四级类目
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
    t1.mall_id -- '商城id' BIGINT
    ,t1.mall -- '商城' STRING

    ,t1.customer_store_id -- '店铺id' BIGINT
    ,t1.customer_store_name -- '店铺名称' STRING
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

    -- 订单信息
    ,t1.ordered_detail -- '下单详情JSON嵌套字典(SSP价格级别统计)' STRING
    ,t1.ordered_customer_id -- '下单用户ID 数组' STRING
    ,t1.ordered_order_id -- '下单订单ID 数组' STRING
    ,t1.ordered_submit_order_id -- '下单提交订单记录ID 数组' STRING
    -- TODO: 目前还缺少费项信息
    ,t1.fee_detail -- '费项详情信息JSON数组' STRING

    ,t1.ordered_catty_price_min -- '最小下单斤单价' DECIMAL(10,2)
    ,t1.ordered_catty_price_max -- '最大下单斤单价' DECIMAL(10,2)
    ,t1.ordered_stander_price_min -- '最小下单件单价' DECIMAL(10,2)
    ,t1.ordered_stander_price_max -- '最大下单件单价' DECIMAL(10,2)
    ,t1.ordered_time_min -- '最早订单创建时间' DATETIME
    ,t1.ordered_time_max -- '最早订单创建时间' DATETIME
    ,t1.ordered_goods_num -- '下单数量' BIGINT
    ,t1.ordered_goods_amt -- '下单金额' DECIMAL(10,2)
    ,t1.ordered_goods_wgt -- '下单重量' DECIMAL(10,2)
    ,t1.delivered_goods_num -- '送货数量' BIGINT
    ,t1.delivered_goods_amt -- '送货金额' DECIMAL(10,2)
    ,t1.delivered_gross_wgt -- '送货重量' DECIMAL(10,2)
    ,t1.platform_commission_amt -- '平台抽佣金额' DECIMAL(10,2)
    ,t1.logistics_oos_goods_num -- '物流原因缺货数量' INT
    ,t1.mct_oos_goods_num -- '商家原因缺货数量' INT
    ,t1.ordered_item_ticket_num -- '明细订单数量' BIGINT
    ,t1.ordered_ticket_num -- '订单数量' BIGINT
    ,t1.ordered_submit_ticket_num -- '订单提交记录数量' BIGINT

    -- 售后信息
    ,t1.after_sale_applied_time_min -- '售后申请最早时间' DATETIME
    ,t1.after_sale_applied_time_max -- '售后申请最晚时间' DATETIME
    ,t1.after_sale_handled_time_min -- '售后处理最早时间' DATETIME
    ,t1.after_sale_handled_time_max -- '售后处理最晚时间' DATETIME

    ,t1.after_sale_applied_ticket_num -- '售后单提交数量' BIGINT
    ,t1.after_sale_applied_num -- '售后申请数量' DECIMAL(20,2)
    ,t1.after_sale_refund_num -- '售后赔付数量' BIGINT
    ,t1.after_sale_refund_amt -- '售后赔付金额' DECIMAL(10,2)
    -- TODO: 售后单处理数量数据可能不准确，待验证是否应该使用处理时间来判断
    ,t1.after_sale_handled_tick_num -- '售后单处理数量' BIGINT
    ,t1.after_sale_liability_amt -- '总赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
    ,t1.after_sale_mct_liability_amt -- '商家承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
    ,t1.after_sale_plat_liability_amt -- '平台承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
    ,t1.after_sale_logist_liability_amt -- '物流承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
    ,t1.after_sale_oos_refund_amt -- '缺货赔付金额' DECIMAL(20,2)
    ,t1.after_sale_mct_oos_refund_amt -- '商家承担缺货赔付金额' DECIMAL(20,2)
    ,t1.after_sale_logist_oos_refund_amt -- '物流承担缺货赔付金额' DECIMAL(20,2)

    ,t1.after_sale_apply_problem -- '售后申请问题' STRING
    ,t1.after_sale_handle_remark -- '售后处理意见' STRING
    ,t1.after_sale_handle_problem -- '售后处理判定问题' STRING

    -- 交叉统计
    ,t1.ordered_store_num -- '下单店铺数' BIGINT
    ,t1.ordered_sku_num -- '下单商品数' BIGINT
    ,t1.ordered_cat4_num -- '下单四级类目数' BIGINT
    ,t1.ordered_cat1_num -- '下单一级类目数' BIGINT
    ,t1.ordered_uscp_num -- '门店商品下单量' BIGINT
    ,t1.grouping_id -- '聚合分组层级ID' BIGINT
    ,t1.dt -- '日期' STRING
    ,CASE
        WHEN 
            ALL_MATCH(
                ARRAY(
                    t1.customer_store_name, t1.ssp_activity_tag, t1.province_name, t1.city_name
                    , t1.county_name, t1.sku_name, t1.category_level4_name, t1.category_level1_name
                    ,t1.map_category_level4_name, t1.merchant_name
                )
                , item -> item = "合计"
            )
        THEN
            "商城"
        WHEN 
            ALL_MATCH(
                ARRAY(
                    t1.customer_store_name, t1.ssp_activity_tag, t1.province_name, t1.city_name
                    , t1.county_name, t1.sku_name, t1.category_level4_name, t1.category_level1_name
                    ,t1.map_category_level4_name, t1.merchant_name
                )
                , item -> item != "合计"
            )
        THEN
            "商城+门店+SKU"
        WHEN 
            ALL_MATCH(
                ARRAY(
                    t1.customer_store_name, t1.ssp_activity_tag
                )
                , item -> item = "合计"
            ) AND
            ALL_MATCH(
                ARRAY(
                    t1.province_name, t1.city_name
                    , t1.county_name, t1.sku_name, t1.category_level4_name, t1.category_level1_name
                    ,t1.map_category_level4_name, t1.merchant_name
                )
                , item -> item != "合计"
            )
        THEN
            "商城+区县+SKU"
        WHEN 
            ALL_MATCH(
                ARRAY(
                    t1.customer_store_name, t1.ssp_activity_tag,  t1.sku_name
                    ,t1.map_category_level4_name, t1.merchant_name
                    
                )
                , item -> item = "合计"
            ) AND
            ALL_MATCH(
                ARRAY(
                    t1.province_name, t1.city_name, t1.county_name
                    ,t1.category_level4_name, t1.category_level1_name
                )
                , item -> item != "合计"
            )
        THEN
            "商城+区县+四级类目"
        WHEN 
            ALL_MATCH(
                ARRAY(
                    t1.customer_store_name, t1.ssp_activity_tag, t1.province_name
                    , t1.city_name, t1.county_name
                )
                , item -> item = "合计"
            ) AND
            ALL_MATCH(
                ARRAY(
                    t1.sku_name,t1.category_level4_name, t1.category_level1_name
                     ,t1.map_category_level4_name, t1.merchant_name
                )
                , item -> item != "合计"
            )
        THEN
            "商城+SKU"
        WHEN 
            ALL_MATCH(
                ARRAY(
                     t1.customer_store_name, t1.ssp_activity_tag, t1.province_name
                    , t1.city_name, t1.county_name, t1.sku_name
                    ,t1.map_category_level4_name, t1.merchant_name
                )
                , item -> item = "合计"
            ) AND
            ALL_MATCH(
                ARRAY(
                    t1.category_level4_name, t1.category_level1_name
                )
                , item -> item != "合计"
            )
        THEN
            "商城+四级类目"
        ELSE "其他"
    END AS dimention_type -- '维度类型' STRING
    
FROM result t1

)


INSERT OVERWRITE TABLE changsha_trade_fact_daily_asc PARTITION(dt)
SELECT
    t1.mall_id -- '商城id' BIGINT
    ,t1.mall -- '商城' STRING

    ,t1.customer_store_id -- '店铺id' BIGINT
    ,t1.customer_store_name -- '店铺名称' STRING
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

    -- 订单信息
    ,BASE64(COMPRESS(t1.ordered_detail)) AS ordered_detail -- '下单详情JSON嵌套字典(SSP价格级别统计)' STRING
    ,BASE64(COMPRESS(t1.ordered_customer_id))  AS ordered_customer_id -- '下单用户ID 数组' STRING
    ,BASE64(COMPRESS(t1.ordered_order_id)) AS ordered_order_id -- '下单订单ID 数组' STRING
    ,BASE64(COMPRESS(t1.ordered_submit_order_id)) AS ordered_submit_order_id -- '下单提交订单记录ID 数组' STRING
    -- TODO: 目前还缺少费项信息
    ,t1.fee_detail -- '费项详情信息JSON数组' STRING

    ,t1.ordered_catty_price_min -- '最小下单斤单价' DECIMAL(10,2)
    ,t1.ordered_catty_price_max -- '最大下单斤单价' DECIMAL(10,2)
    ,t1.ordered_stander_price_min -- '最小下单件单价' DECIMAL(10,2)
    ,t1.ordered_stander_price_max -- '最大下单件单价' DECIMAL(10,2)
    ,t1.ordered_time_min -- '最早订单创建时间' DATETIME
    ,t1.ordered_time_max -- '最早订单创建时间' DATETIME
    ,t1.ordered_goods_num -- '下单数量' BIGINT
    ,t1.ordered_goods_amt -- '下单金额' DECIMAL(10,2)
    ,t1.ordered_goods_wgt -- '下单重量' DECIMAL(10,2)
    ,t1.delivered_goods_num -- '送货数量' BIGINT
    ,t1.delivered_goods_amt -- '送货金额' DECIMAL(10,2)
    ,t1.delivered_gross_wgt -- '送货重量' DECIMAL(10,2)
    ,t1.platform_commission_amt -- '平台抽佣金额' DECIMAL(10,2)
    ,t1.logistics_oos_goods_num -- '物流原因缺货数量' INT
    ,t1.mct_oos_goods_num -- '商家原因缺货数量' INT
    ,t1.ordered_item_ticket_num -- '明细订单数量' BIGINT
    ,t1.ordered_ticket_num -- '订单数量' BIGINT
    ,t1.ordered_submit_ticket_num -- '订单提交记录数量' BIGINT

    -- 售后信息
    ,t1.after_sale_applied_time_min -- '售后申请最早时间' DATETIME
    ,t1.after_sale_applied_time_max -- '售后申请最晚时间' DATETIME
    ,t1.after_sale_handled_time_min -- '售后处理最早时间' DATETIME
    ,t1.after_sale_handled_time_max -- '售后处理最晚时间' DATETIME

    ,t1.after_sale_applied_ticket_num -- '售后单提交数量' BIGINT
    ,t1.after_sale_applied_num -- '售后申请数量' DECIMAL(20,2)
    ,t1.after_sale_refund_num -- '售后赔付数量' BIGINT
    ,t1.after_sale_refund_amt -- '售后赔付金额' DECIMAL(10,2)
    -- TODO: 售后单处理数量数据可能不准确，待验证是否应该使用处理时间来判断
    ,t1.after_sale_handled_tick_num -- '售后单处理数量' BIGINT
    ,t1.after_sale_liability_amt -- '总赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
    ,t1.after_sale_mct_liability_amt -- '商家承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
    ,t1.after_sale_plat_liability_amt -- '平台承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
    ,t1.after_sale_logist_liability_amt -- '物流承担赔付金额(包括货值赔付和膨胀赔付)' DECIMAL(20,2)
    ,t1.after_sale_oos_refund_amt -- '缺货赔付金额' DECIMAL(20,2)
    ,t1.after_sale_mct_oos_refund_amt -- '商家承担缺货赔付金额' DECIMAL(20,2)
    ,t1.after_sale_logist_oos_refund_amt -- '物流承担缺货赔付金额' DECIMAL(20,2)

    ,BASE64(COMPRESS(t1.after_sale_apply_problem)) AS after_sale_apply_problem -- '售后申请问题' STRING
    ,BASE64(COMPRESS(t1.after_sale_handle_remark)) AS after_sale_handle_remark -- '售后处理意见' STRING
    ,BASE64(COMPRESS(t1.after_sale_handle_problem)) AS after_sale_handle_problem -- '售后处理判定问题' STRING

    -- 交叉统计
    ,t1.ordered_store_num -- '下单店铺数' BIGINT
    ,t1.ordered_sku_num -- '下单商品数' BIGINT
    ,t1.ordered_cat4_num -- '下单四级类目数' BIGINT
    ,t1.ordered_cat1_num -- '下单一级类目数' BIGINT
    ,t1.ordered_uscp_num -- '门店商品下单量' BIGINT
    ,t1.grouping_id -- '聚合分组层级ID' BIGINT
    ,t1.dimention_type -- '维度类型' STRING
    ,t1.dt -- '日期' STRING
    
FROM report t1
WHERE ISNOTNULL(t1.dimention_type)
    AND t1.dt BETWEEN DATEADD(CURRENT_DATE(), -7, "dd")
            AND DATEADD(CURRENT_DATE(), 0, "dd")

;
"""


