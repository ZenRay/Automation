#coding:utf8
"""
temp_dws_mct_stat_sentence: 商城+区县+商家+四级类目 多维度统计报表
mct_cat4_stat_sentence: 商家视角四级类目统计报表
mct_mall_stat_sentence: 商家视角商城统计报表
mct_mct_cat4_stat_sentence: 商家视角商家+四级类目统计报表
mct_mct_province_stat_sentence: 商城商家视角省级统计报表
mct_sku_stat_sentence: 商家视角SKU统计报表
"""

temp_dws_mct_stat_sentence = """
DROP TABLE IF EXISTS changsha_dws_mct_stat_daily_asc_temp;
CREATE TABLE IF NOT EXISTS  changsha_dws_mct_stat_daily_asc_temp
LIFECYCLE 2 
AS


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

, base AS(
    SELECT
        t1.dt -- '日期'
        ,t1.mall_id -- '商城ID' BIGINT
        ,t1.mall -- '商城' STRING
        ,t1.sku_id -- '商品ID' BIGINT
        ,t1.sku_name -- '商品名称' STRING
        ,t1.category_level1_name -- '一级类目名称' STRING
        ,t1.category_level4_name -- '四级类目名称' STRING
        ,t1.merchant_id -- '商家ID' BIGINT
        ,t1.merchant_name -- '商家名称' STRING
        ,t1.customer_store_id -- '店铺ID' BIGINT
        ,t1.ssp_activity_tag -- '商品门店活跃度' STRING
        ,t1.province_id -- '省ID' BIGINT
        ,t1.province_name -- '省名称' STRING
        ,t1.city_id -- '市ID' BIGINT
        ,t1.city_name -- '市名称' STRING
        ,t1.county_id -- '区县ID' BIGINT
        ,t1.county_name -- '区县名称' STRING
        ,t1.ordered_goods_num -- '下单件数' BIGINT
        ,t1.ordered_goods_amt -- '下单金额' DECIMAL(10,2)
        ,t1.ordered_goods_wgt -- '下单重量' DECIMAL(10,2)
        ,t1.delivered_goods_num -- '送货件数' BIGINT
        ,t1.delivered_goods_amt -- '送货金额' DECIMAL(10,2)
        ,t1.delivered_gross_wgt -- '送货重量' DECIMAL(10,2)
        ,t1.platform_commission_amt -- '平台抽佣金额' DECIMAL(10,2)
        ,t1.ordered_item_ticket_num -- '明细订单量' BIGINT
        ,t1.after_sale_applied_num -- '售后申请件数' BIGINT
        ,t1.after_sale_applied_ticket_num -- '售后申请订单量' BIGINT
        ,t1.after_sale_refund_num -- '售后赔付件数' BIGINT
        ,t1.after_sale_refund_amt -- '售后赔付金额' DECIMAL(10,2)
        ,t1.high_refund_amt_ratio_ticket_num -- '高售后赔付订单量' BIGINT
        ,t1.new_store_market_expense_cost -- '新客客户营销费用' DECIMAL(10,2)
        ,t1.lost_store_market_expense_cost -- '流失客户营销费用' DECIMAL(10,2)
        ,t1.market_expense_cost -- '总营销费用' DECIMAL(10,2)
    FROM(
        SELECT
            t1.dt
            ,t1.mall_id
            ,t1.mall
            ,t1.sku_id
            ,t1.sku_name
            ,t1.category_level1_name
            ,t1.category_level4_name
            ,t1.merchant_id
            ,t1.merchant_name
            ,t1.customer_store_id
            ,t1.ssp_activity_tag
            ,t1.province_id
            ,t1.province_name
            ,t1.city_id
            ,t1.city_name
            ,t1.county_id
            ,t1.county_name
            ,t1.ordered_goods_num
            ,t1.ordered_goods_amt
            ,t1.ordered_goods_wgt
            ,t1.delivered_goods_num
            ,t1.delivered_goods_amt
            ,t1.delivered_gross_wgt
            ,t1.platform_commission_amt
            ,t1.ordered_item_ticket_num


            ,t1.after_sale_applied_num
            ,t1.after_sale_applied_ticket_num

            ,t1.after_sale_refund_num
            ,t1.after_sale_refund_amt
            ,t1.high_refund_amt_ratio_ticket_num
            -- ,ROW_NUMBER() OVER(PARTITION BY t1.dt, t1.mall_id, t1.sku_id ORDER BY t1.customer_store_id DESC) AS sku_rnk
            ,IF(NVL(t1.ssp_activity_tag, "NEW")="NEW", t1.mct_market_fee, 0) AS  new_store_market_expense_cost
            ,IF(NVL(t1.ssp_activity_tag, "NEW")="LOSS", t1.mct_market_fee, 0) AS lost_store_market_expense_cost
            ,t1.mct_market_fee AS market_expense_cost
            
        FROM changsha_trade_fact_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -45, "dd")
                AND CURRENT_DATE()
            AND t1.dimention_type = "商城+门店+SKU"
    ) t1

)


,result AS(
    SELECT
        t1.dt -- '日期'
        ,t1.mall_id -- '商城ID' BIGINT
        ,t1.mall -- '商城' STRING

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
            GROUPING(t1.category_level4_name)==0, t1.category_level4_name, "合计"
        ) AS category_level4_name -- '四级类目名称' STRING
        ,IF(
            GROUPING(t1.category_level1_name)==0, t1.category_level1_name, "合计"
        ) AS category_level1_name -- '一级类目名称' STRING
        ,IF(
            GROUPING(t1.merchant_id)==0, t1.merchant_id, -99999L
        ) AS merchant_id -- '商家ID' BIGINT
        ,IF(
            GROUPING(t1.merchant_name)==0, t1.merchant_name, "合计"
        ) AS merchant_name -- '商家名称' STRING
        ,GROUPING_ID(
            t1.province_id -- '省ID' BIGINT
            ,t1.province_name -- '省名称' STRING
            ,t1.city_id -- '市ID' BIGINT
            ,t1.city_name -- '市名称' STRING
            ,t1.county_id -- '区ID' BIGINT
            ,t1.county_name -- '区名称' STRING

            ,t1.sku_id -- '商品id' BIGINT
            ,t1.sku_name -- '商品名称'
            ,t1.category_level4_name -- '四级类目名称' STRING
            ,t1.category_level1_name -- '一级类目名称' STRING
            ,t1.merchant_id -- '商家ID' BIGINT
            ,t1.merchant_name -- '商家名称' STRING
        ) AS grouping_id -- '分组聚合等级ID' BIGINT

        ,SUM(t1.ordered_goods_num) AS ordered_goods_num -- '下单件数' BIGINT
        ,SUM(t1.ordered_goods_amt) AS ordered_goods_amt -- '下单金额' DECIMAL(10,2)
        ,SUM(t1.ordered_goods_wgt) AS ordered_goods_wgt -- '下单重量' DECIMAL(10,2)
        ,COUNT(DISTINCT IF(
            t1.ordered_goods_num>0, t1.customer_store_id, NULL
        )) AS ordered_store_num -- '下单店铺数' BIGINT
        ,SUM(t1.delivered_goods_num) AS delivered_goods_num -- '送货件数' BIGINT
        ,SUM(t1.delivered_goods_amt) AS delivered_goods_amt -- '送货金额' DECIMAL(10,2)
        ,SUM(t1.delivered_gross_wgt) AS delivered_gross_wgt -- '送货重量' DECIMAL(10,2)
        ,SUM(t1.platform_commission_amt) AS platform_commission_amt -- '平台抽佣金额' DECIMAL(10,2)
        ,SUM(t1.ordered_item_ticket_num) AS ordered_item_ticket_num -- '明细订单量' BIGINT
        ,SUM(t1.after_sale_applied_num) AS after_sale_applied_num -- '售后申请件数' BIGINT
        ,SUM(t1.after_sale_applied_ticket_num) AS after_sale_applied_ticket_num -- '售后申请订单量' BIGINT
        ,SUM(t1.after_sale_refund_num) AS after_sale_refund_num -- '售后赔付件数' BIGINT
        ,SUM(t1.after_sale_refund_amt) AS after_sale_refund_amt -- '售后赔付金额' DECIMAL(10,2)
        ,SUM(t1.high_refund_amt_ratio_ticket_num) AS high_refund_amt_ratio_ticket_num -- '高售后赔付订单量' BIGINT
        ,SUM(t1.new_store_market_expense_cost) AS new_store_market_expense_cost -- '新客客户营销费用' DECIMAL(10,2)
        ,SUM(t1.lost_store_market_expense_cost) AS lost_store_market_expense_cost -- '流失客户营销费用' DECIMAL(10,2)
        ,SUM(t1.market_expense_cost) AS market_expense_cost -- '总营销费用' DECIMAL(10,2)
        ,COUNT(DISTINCT IF(
            t1.after_sale_applied_num > 0, t1.customer_store_id, NULL
        )) AS after_sale_applied_store_num -- '申请售后店铺数' BIGINT
    FROM base t1

    GROUP BY t1.dt
            ,t1.mall_id -- '商城id' BIGINT 
            ,t1.mall -- '商城' STRING
        ,GROUPING SETS (
            -- 区县商家四级类目
            (
                t1.province_id -- '省ID' BIGINT
                ,t1.province_name -- '省名称' STRING
                ,t1.city_id -- '市ID' BIGINT
                ,t1.city_name -- '市名称' STRING
                ,t1.county_id -- '区ID' BIGINT
                ,t1.county_name -- '区名称' STRING

                ,t1.merchant_id -- '商家id' BIGINT
                ,t1.merchant_name -- '商家名称' STRING
                ,t1.category_level4_name -- '四级类目名称' STRING
                ,t1.category_level1_name -- '一级类目名称' STRING
            )
            -- 商家四级类目SKU
            ,(
                t1.sku_id
                ,t1.sku_name
                ,t1.merchant_id -- '商家id' BIGINT
                ,t1.merchant_name -- '商家名称' STRING
                ,t1.category_level4_name -- '四级类目名称' STRING
                ,t1.category_level1_name -- '一级类目名称' STRING
            )
            -- 商家四级类目
            ,(
                t1.merchant_id -- '商家id' BIGINT
                ,t1.merchant_name -- '商家名称' STRING
                ,t1.category_level4_name -- '四级类目名称' STRING
                ,t1.category_level1_name -- '一级类目名称' STRING
            )
            -- 区县四级类目
            ,(
                t1.province_id -- '省ID' BIGINT
                ,t1.province_name -- '省名称' STRING
                ,t1.city_id -- '市ID' BIGINT
                ,t1.city_name -- '市名称' STRING
                ,t1.county_id -- '区ID' BIGINT
                ,t1.county_name -- '区名称' STRING

                ,t1.category_level4_name -- '四级类目名称' STRING
                ,t1.category_level1_name -- '一级类目名称' STRING
            )
            -- 四级类目
            ,(
                ,t1.category_level4_name -- '后台类目名称' STRING
                ,t1.category_level1_name -- '一级类目名称' STRING
            )
            -- 区县
            ,(
                t1.province_id -- '省ID' BIGINT
                ,t1.province_name -- '省名称' STRING
                ,t1.city_id -- '市ID' BIGINT
                ,t1.city_name -- '市名称' STRING
                ,t1.county_id -- '区ID' BIGINT
                ,t1.county_name -- '区名称' STRING
            )
            -- 商家
            ,(
                t1.merchant_id -- '商家id' BIGINT
                ,t1.merchant_name -- '商家名称' STRING
            )
        )
)


,report AS(
    SELECT
        t1.dt -- '日期'
        ,t1.mall_id -- '商城ID' BIGINT
        ,t1.mall -- '商城' STRING

        ,t1.province_id -- '省ID' BIGINT
        ,t1.province_name -- '省名称' STRING
        ,t1.city_id -- '市ID' BIGINT
        ,t1.city_name -- '市名称' STRING
        ,t1.county_id -- '区ID' BIGINT
        ,t1.county_name -- '区名称' STRING

        ,t1.sku_id -- '商品id' BIGINT
        ,t1.sku_name -- '商品名称' STRING
        ,t1.category_level4_name -- '四级类目名称' STRING
        ,t1.category_level1_name -- '一级类目名称' STRING
        ,t1.merchant_id -- '商家ID' BIGINT
        ,t1.merchant_name -- '商家名称' STRING


        ,t1.ordered_goods_num -- '下单件数' BIGINT
        ,t1.ordered_goods_amt -- '下单金额' DECIMAL(10,2)
        ,t1.ordered_goods_wgt -- '下单重量' DECIMAL(10,2)
        ,t1.ordered_store_num -- '下单店铺数' BIGINT
        ,t1.delivered_goods_num -- '送货件数' BIGINT
        ,t1.delivered_goods_amt -- '送货金额' DECIMAL(10,2)
        ,t1.delivered_gross_wgt -- '送货重量' DECIMAL(10,2)
        ,t1.platform_commission_amt -- '平台抽佣金额' DECIMAL(10,2)
        ,t1.ordered_item_ticket_num -- '明细订单量' BIGINT
        ,t1.after_sale_applied_num -- '售后申请件数' BIGINT
        ,t1.after_sale_applied_ticket_num -- '售后申请订单量' BIGINT
        ,t1.after_sale_refund_num -- '售后赔付件数' BIGINT
        ,t1.after_sale_refund_amt -- '售后赔付金额' DECIMAL(10,2)
        ,t1.after_sale_applied_store_num -- '申请售后店铺数' BIGINT
        ,t1.high_refund_amt_ratio_ticket_num -- '高售后赔付订单量' BIGINT
        ,t1.new_store_market_expense_cost -- '新客客户营销费用' DECIMAL(10,2)
        ,t1.lost_store_market_expense_cost -- '流失客户营销费用' DECIMAL(10,2)
        ,t1.market_expense_cost -- '总营销费用' DECIMAL(10,2)

        ,t1.grouping_id -- '聚合分组层级ID' BIGINT
        ,CASE
            WHEN 
                ALL_MATCH(
                    ARRAY(
                        t1.sku_name
                    )
                    , item -> item = "合计"
                )
                AND ALL_MATCH(
                    ARRAY(
                        t1.province_name, t1.city_name, t1.county_name, t1.merchant_name,
                        t1.category_level4_name, t1.category_level1_name
                    )
                    , item -> item != "合计"
                )
            THEN
                "商城+区县+商家+四级类目"
            WHEN 
                ALL_MATCH(
                    ARRAY(
                        t1.province_name, t1.city_name, t1.county_name
                    )
                    , item -> item = "合计"
                )
                AND ALL_MATCH(
                    ARRAY(
                        t1.sku_name, t1.merchant_name,
                        t1.category_level4_name, t1.category_level1_name
                    )
                    , item -> item != "合计"
                )
            THEN
                "商城+SKU"
            WHEN 
                ALL_MATCH(
                    ARRAY(
                        t1.sku_name, t1.province_name, t1.city_name, t1.county_name
                    )
                    , item -> item = "合计"
                )
                AND ALL_MATCH(
                    ARRAY(
                        t1.merchant_name, t1.category_level4_name, t1.category_level1_name
                    )
                    , item -> item != "合计"
                )
            THEN
                "商城+商家+四级类目"
            WHEN 
                ALL_MATCH(
                    ARRAY(
                        t1.sku_name, t1.merchant_name
                    )
                    , item -> item = "合计"
                )
                AND ALL_MATCH(
                    ARRAY(
                        t1.province_name, t1.city_name, t1.county_name, t1.category_level4_name, t1.category_level1_name
                    )
                    , item -> item != "合计"
                )
            THEN
                "商城+区县+四级类目"
            WHEN 
                ALL_MATCH(
                    ARRAY(
                        t1.province_name, t1.city_name, t1.county_name, t1.sku_name, t1.merchant_name
                    )
                    , item -> item = "合计"
                )
                AND ALL_MATCH(
                    ARRAY(
                        t1.category_level4_name, t1.category_level1_name
                    )
                    , item -> item != "合计"
                )
            THEN
                "商城+四级类目"
            WHEN 
                ALL_MATCH(
                    ARRAY(
                        t1.category_level4_name, t1.category_level1_name, t1.sku_name, t1.merchant_name
                    )
                    , item -> item = "合计"
                )
                AND ALL_MATCH(
                    ARRAY(
                        t1.province_name, t1.city_name, t1.county_name
                    )
                    , item -> item != "合计"
                )
            THEN
                "商城+区县"
            WHEN 
                ALL_MATCH(
                    ARRAY(
                        t1.category_level4_name, t1.category_level1_name, t1.sku_name, t1.province_name, t1.city_name, t1.county_name
                    )
                    , item -> item = "合计"
                )
                AND ALL_MATCH(
                    ARRAY(
                        t1.merchant_name
                    )
                    , item -> item != "合计"
                )
            THEN
                "商城+商家"
        END dimension_type -- '维度类型' STRING
    FROM result t1
)


-- TODO: 待修正目前只处理了，当日区县有效店铺数没有处理召回客和老客的店铺数
,stores AS(
    SELECT
        NVL(t1.dt, t3.dt) AS dt
        ,NVL(record.mall_id, t3.mall_id) AS mall_id
        ,NVL(t2.mall, t3.mall) AS mall
        ,NVL(t1.customer_store_id, t3.customer_store_id) AS customer_store_id
        ,NVL(t1.province_id, t3.province_id) AS province_id
        ,NVL(t1.province_name, t3.province_name) AS province_name
        ,NVL(t1.city_id, t3.city_id) AS city_id
        ,NVL(t1.city_name, t3.city_name) AS city_name
        ,NVL(t1.county_id, t3.county_id) AS county_id
        ,NVL(t1.county_name, t3.county_name) AS county_name
        
        ,t3.ordered_goods_num
        ,IF(ISNOTNULL(t1.customer_store_id) OR t3.ordered_goods_num>0, 1, 0) AS is_valid
    FROM changsha_dim_store_daily_full t1
    LATERAL VIEW EXPLODE(t1.mall_id) record AS mall_id
    LEFT JOIN mall t2
        ON t2.mall_id = record.mall_id

    FULL JOIN base t3
        ON t1.dt = t3.dt
        AND t3.mall_id = record.mall_id
        AND t3.customer_store_id = t1.customer_store_id
    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -45, "dd")
            AND CURRENT_DATE()
        AND t1.use_status != "INVALID"
        AND GET_JSON_OBJECT(t1.customer_json, "$.[0].customer_auth_status") = "CERTIFIED"
        AND ISNOTNULL(NVL(t2.mall, t3.mall))
)


,stores_stat AS(

    SELECT
        t1.dt
        ,t1.mall_id
        ,t1.mall
        ,t1.province_id
        ,t1.province_name
        ,t1.city_id
        ,t1.city_name
        ,t1.county_id
        ,t1.county_name
        ,COUNT(DISTINCT IF(t1.is_valid >0, t1.customer_store_id, NULL)) AS validated_store_num
        ,"商城+区县" AS dimension_type
    FROM stores t1
    GROUP BY t1.dt
        ,t1.mall_id
        ,t1.mall
        ,t1.province_id
        ,t1.province_name
        ,t1.city_id
        ,t1.city_name
        ,t1.county_id
        ,t1.county_name
)

SELECT
    t1.mall_id -- '商城ID' BIGINT
	,t1.mall -- '商城' STRING

	,t1.province_id -- '省ID' BIGINT
	,t1.province_name -- '省名称' STRING
	,t1.city_id -- '市ID' BIGINT
	,t1.city_name -- '市名称' STRING
	,t1.county_id -- '区ID' BIGINT
	,t1.county_name -- '区名称' STRING

	,t1.sku_id -- '商品id' BIGINT
	,t1.sku_name -- '商品名称' STRING
	,t1.category_level4_name -- '四级类目名称' STRING
	,t1.category_level1_name -- '一级类目名称' STRING
	,t1.merchant_id -- '商家ID' BIGINT
	,t1.merchant_name -- '商家名称' STRING


	,t1.ordered_goods_num -- '下单件数' BIGINT
	,t1.ordered_goods_amt -- '下单金额' DECIMAL(10,2)
	,t1.ordered_goods_wgt -- '下单重量' DECIMAL(10,2)
    ,t1.ordered_store_num -- '下单店铺数' BIGINT
	,t1.delivered_goods_num -- '送货件数' BIGINT
	,t1.delivered_goods_amt -- '送货金额' DECIMAL(10,2)
	,t1.delivered_gross_wgt -- '送货重量' DECIMAL(10,2)
	,t1.platform_commission_amt -- '平台抽佣金额' DECIMAL(10,2)
	,t1.ordered_item_ticket_num -- '明细订单量' BIGINT
	,t1.after_sale_applied_num -- '售后申请件数' BIGINT
	,t1.after_sale_applied_ticket_num -- '售后申请订单量' BIGINT
	,t1.after_sale_refund_num -- '售后赔付件数' BIGINT
	,t1.after_sale_refund_amt -- '售后赔付金额' DECIMAL(10,2)
	,t1.after_sale_applied_store_num -- '申请售后店铺数' BIGINT
	,t1.high_refund_amt_ratio_ticket_num -- '高售后赔付订单量' BIGINT
	,t1.new_store_market_expense_cost -- '新客客户营销费用' DECIMAL(10,2)
	,t1.lost_store_market_expense_cost -- '流失客户营销费用' DECIMAL(10,2)
	,t1.market_expense_cost -- '总营销费用' DECIMAL(10,2)
    ,t2.validated_store_num -- '有效店铺数' BIGINT
	,t1.grouping_id -- '聚合分组层级ID' BIGINT
    ,t1.dimension_type -- '维度类型' STRING

    ,t1.dt
FROM report t1
LEFT JOIN stores_stat t2
    ON t2.dt = t1.dt
    AND t2.mall_id = t1.mall_id
    AND t2.province_id = t1.province_id
    AND t2.city_id = t1.city_id
    AND t2.county_id = t1.county_id
    AND t2.dimension_type = t1.dimension_type
;
"""

mct_cat4_stat_sentence = """
WITH base AS(
	SELECT
		t1.dt -- '日期' DATE
		,t1.mall_id -- '商城ID' BIGINT
		,t1.mall -- '商城' STRING
		,t1.category_level4_name -- '四级类目名称' STRING
		,t1.category_level1_name -- '一级类目名称' STRING

		-- ,t1.province_id -- '省ID' BIGINT
		-- ,t1.province_name -- '省名称' STRING
		-- ,t1.city_id -- '市ID' BIGINT
		-- ,t1.city_name -- '市名称' STRING
		-- ,t1.county_id -- '区ID' BIGINT
		-- ,t1.county_name -- '区名称' STRING
		-- ,t1.sku_id -- '商品id' BIGINT
		-- ,t1.sku_name -- '商品名称' STRING
		-- ,t1.merchant_id -- '商家ID' BIGINT
		-- ,t1.merchant_name -- '商家名称' STRING

		,t1.ordered_goods_num -- '下单件数' BIGINT
		,t1.ordered_goods_amt -- '下单金额' DECIMAL(10,2)
		,t1.ordered_goods_wgt -- '下单重量' DECIMAL(10,2)
		,t1.ordered_store_num -- '下单店铺数' BIGINT
		,t2.cat1_fruit_ordered_store_num -- '水果下单店铺数' BIGINT
		,MAX(CAST(t1.ordered_store_num / t2.cat1_fruit_ordered_store_num AS DECIMAL(10,4))) OVER(
			PARTITION BY t1.dt, t1.category_level1_name, t1.category_level4_name
		) AS top1_store_ratio -- 'Top1水果日活覆盖率' DECIMAL(10,4)
        ,MAX(CAST(t1.ordered_goods_num / t1.ordered_store_num AS DECIMAL(10,1))) OVER(
            PARTITION BY t1.dt, t1.category_level1_name, t1.category_level4_name
        ) AS top1_goods_num_ps -- 'top1客均件数' DECIMAL(10,1)
		,t1.delivered_goods_num -- '送货件数' BIGINT
		,t1.delivered_goods_amt -- '送货金额' DECIMAL(10,2)
		,t1.delivered_gross_wgt -- '送货重量' DECIMAL(10,2)
		,t1.platform_commission_amt -- '平台抽佣金额' DECIMAL(10,2)
		,t1.ordered_item_ticket_num -- '明细订单量' BIGINT
		,t1.after_sale_applied_num -- '售后申请件数' BIGINT
		,t1.after_sale_applied_ticket_num -- '售后申请订单量' BIGINT
		,t1.after_sale_refund_num -- '售后赔付件数' BIGINT
		,t1.after_sale_refund_amt -- '售后赔付金额' DECIMAL(10,2)
		,t1.after_sale_applied_store_num -- '申请售后店铺数' BIGINT
		,t1.high_refund_amt_ratio_ticket_num -- '高售后赔付订单量' BIGINT
	FROM changsha_dws_mct_stat_daily_asc_temp t1
	LEFT JOIN changsha_trade_fact_daily_asc t2
		ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
					AND DATEADD(CURRENT_DATE(), -1, "dd")
		AND t2.dt = t1.dt
		AND t2.dimention_type = "商城"
		AND t2.mall_id = t1.mall_id
	WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
					AND DATEADD(CURRENT_DATE(), -1, "dd")
		AND t1.dimension_type = "商城+四级类目"
		AND t1.category_level4_name IN (
            SELECT
                t1.category_level4_name
            FROM changsha_t_cate4_type_conf t1
        )

)



,scat4_tag_base AS(
        SELECT
            t1.dt -- `日期`
            ,t1.mall_id -- `商城id`
            ,t1.customer_store_id -- `店铺id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level4_name -- `四级类目名称`

            ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -31, 23), "1")>0, 1, 0) AS is_ordered_m30dtm23d -- `是否t-30到t-23下单`
            ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -8, 7), "1")>0, 1, 0) AS is_ordered_m7dtm1d -- `是否t-7到t-1下单`
            
            ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -7, 7), "1")>0, 1, 0) AS is_ordered_m6dtcd -- `是否近七日下单`
            ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -30, 30), "1")>0, 1, 0) AS is_ordered_m29dtcd -- `是否近三十日下单`
            ,REGEXP_COUNT(SUBSTR(t1.exposed_m79t0d, -7, 7), "1") AS exposed_m7dtcd_cnt -- `近七日曝光次数`
            ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_cdta15d, 1, 7), "1")>0, 1, 0) AS is_ordered_cdta6d -- `是否后七日下单`
            ,REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -31, 30), "1") AS ordered_m30dtm1d -- `t-30到t-1下单次数`
            ,REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -31, 23), "1") AS ordered_m30dtm8d -- `t-30到t-8下单次数`
            ,REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -8, 7), "1") AS ordered_m7dtm1d -- `t-7到t-1下单次数`
            ,t1.is_ordered -- `当日是否下单`
            ,t1.is_exposed -- `当日是否曝光`
            ,t1.ordered_goods_num
            ,t1.ordered_goods_amt
            ,NULL AS is_after_sale_applied
            ,t1.dimension_type
        FROM changsha_dws_store_tag_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
					AND DATEADD(CURRENT_DATE(), -1, "dd")
            AND t1.dimension_type ="商城+门店+四级类目"
            AND t1.category_level4_name IN (
                SELECT
                    t1.category_level4_name
                FROM changsha_t_cate4_type_conf t1
            )
)



,scat4_tag AS(
    SELECT
        t1.dt
        ,t1.mall_id
        ,t1.category_level1_name
        ,t1.category_level4_name
        
        ,COUNT(DISTINCT IF(t1.is_ordered_m6dtcd>0, t1.customer_store_id, NULL)) AS ordered_store_num_m6dtcd -- `近七日下单店铺数`
        ,COUNT(DISTINCT IF(t1.is_ordered_m29dtcd>0, t1.customer_store_id, NULL)) AS ordered_store_num_m29dtcd -- `近三日日下单店铺数`

    FROM scat4_tag_base t1
    GROUP BY t1.dt
            ,t1.mall_id
            ,t1.category_level1_name
            ,t1.category_level4_name
)

SELECT
	t1.dt AS `日期`
	,t1.mall_id AS `商城ID`
	,t1.mall AS `商城`
	,t1.category_level4_name AS `四级类目名称`
	,t1.category_level1_name AS `一级类目名称`

	,t1.ordered_goods_num AS `下单件数`
	,t1.ordered_goods_amt AS `下单金额`
	,t1.ordered_goods_wgt AS `下单重量`
	,t1.ordered_store_num AS `下单店铺数`
	,t1.cat1_fruit_ordered_store_num AS `水果下单店铺数`
	,t1.top1_store_ratio AS `Top1水果日活覆盖率`
	,t1.delivered_goods_num AS `送货件数`
	,t1.delivered_goods_amt AS `送货金额`
	,t1.delivered_gross_wgt AS `送货重量`
	,NVL(t1.platform_commission_amt,0) AS `平台抽佣金额`
	,NVL(t1.ordered_item_ticket_num,0) AS `明细订单量`
	,NVL(t1.after_sale_applied_num,0) AS `售后申请件数`
	,NVL(t1.after_sale_applied_ticket_num,0) AS `售后申请订单量`
	,NVL(t1.after_sale_refund_num,0) AS `售后赔付件数`
	,NVL(t1.after_sale_refund_amt,0) AS `售后赔付金额`
	,NVL(t1.after_sale_applied_store_num,0) AS `申请售后店铺数`
	,NVL(t1.high_refund_amt_ratio_ticket_num,0) AS `高售后赔付订单量`

    ,NVL(t2.ordered_store_num_m6dtcd,0) AS `近七日下单店铺数`
    ,NVL(t2.ordered_store_num_m29dtcd,0) AS `近三日日下单店铺数`
    ,NVL(t1.top1_goods_num_ps,0) AS `top1客均件数`
FROM base t1
LEFT JOIN scat4_tag t2
    ON t2.dt = t1.dt
    AND t2.mall_id = t1.mall_id
    AND t2.category_level4_name = t1.category_level4_name
    AND t2.category_level1_name = t1.category_level1_name
WHERE t1.mall_id = 871
;
"""

mct_mall_stat_sentence = """
SELECT
	t1.dt AS `日期`
	,t1.mall_id AS `商城id`
	,t1.mall AS `商城`
	,t1.cat1_fruit_ordered_store_num AS `下单水果店铺数`
	,t1.ordered_store_num AS `下单店铺数`
FROM changsha_trade_fact_daily_asc t1
WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
			AND DATEADD(CURRENT_DATE(), -1, "dd")
		AND t1.dimention_type = "商城"
	AND t1.mall_id = 871
LIMIT 2000
;

"""

mct_mct_cat4_stat_sentence = """
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

,base AS(
	SELECT
		t1.dt -- '日期' DATE
		,t1.mall_id -- '商城ID' BIGINT
		,t1.mall -- '商城' STRING
		,t1.category_level4_name -- '四级类目名称' STRING
		,t1.category_level1_name -- '一级类目名称' STRING
		,t1.merchant_id -- '商家ID' BIGINT
		,t1.merchant_name -- '商家名称' STRING

		,t1.ordered_goods_num -- '下单件数' BIGINT
		,t1.ordered_goods_amt -- '下单金额' DECIMAL(10,2)
		,t1.ordered_goods_wgt -- '下单重量' DECIMAL(10,2)
		,t1.ordered_store_num -- '下单店铺数' BIGINT
		,t1.delivered_goods_num -- '送货件数' BIGINT
		,t1.delivered_goods_amt -- '送货金额' DECIMAL(10,2)
		,t1.delivered_gross_wgt -- '送货重量' DECIMAL(10,2)
		,t1.platform_commission_amt -- '平台抽佣金额' DECIMAL(10,2)
		,t1.ordered_item_ticket_num -- '明细订单量' BIGINT
		,t1.after_sale_applied_num -- '售后申请件数' BIGINT
		,t1.after_sale_applied_ticket_num -- '售后申请订单量' BIGINT
		,t1.after_sale_refund_num -- '售后赔付件数' BIGINT
		,t1.after_sale_refund_amt -- '售后赔付金额' DECIMAL(10,2)
		,t1.after_sale_applied_store_num -- '申请售后店铺数' BIGINT
		,t1.high_refund_amt_ratio_ticket_num -- '高售后赔付订单量' BIGINT
	FROM changsha_dws_mct_stat_daily_asc_temp t1
	WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
					AND DATEADD(CURRENT_DATE(), -1, "dd")
		AND t1.dimension_type = "商城+商家+四级类目"
		AND t1.category_level4_name IN (
            SELECT
                t1.category_level4_name
            FROM changsha_t_cate4_type_conf t1
        )


)


-- 商品维度
,dim_sku AS(
    SELECT
        t1.dt
        ,t1.mall_id
        ,t1.is_up_today
        ,t1.sku_id
        ,t1.sku_name
        ,t1.merchant_id
        ,CASE
            WHEN INSTR(t1.category_level1_name, "水果")>0 THEN "水果"
            ELSE t1.category_level1_name
        END category_level1_name
        ,CASE
            WHEN INSTR(t1.map_back_category_name, "软籽石榴") > 0 THEN "软籽石榴"
            WHEN INSTR(t1.map_back_category_name, "冬枣") > 0 THEN "冬枣"
            WHEN INSTR(t1.map_back_category_name, "秋月梨") > 0 THEN "秋月梨"
            WHEN INSTR(t1.map_back_category_name, "椰青") > 0 THEN "椰青"
            WHEN INSTR(t1.map_back_category_name, "蜜桔") > 0 THEN "蜜桔"
            WHEN INSTR(t1.map_back_category_name, "凯特芒") > 0 THEN "凯特芒"
            WHEN
                INSTR(t1.map_back_category_name, "法兰西西梅")
                + INSTR(t1.map_back_category_name, "进口西梅")> 0 
            THEN "进口西梅"
            WHEN
                INSTR(t1.map_back_category_name, "普通千禧番茄")
                + INSTR(t1.map_back_category_name, "千禧樱桃小番茄")> 0 
            THEN "千禧樱桃小番茄"
            WHEN
                INSTR(t1.map_back_category_name, "红心猕猴桃")
                + INSTR(t1.map_back_category_name, "红心猕猴桃/奇异果")> 0 
            THEN "红心猕猴桃"
            WHEN
                INSTR(t1.map_back_category_name, "皇帝蕉")
                + INSTR(t1.map_back_category_name, "皇帝蕉生蕉")> 0 
            THEN "皇帝蕉"
            WHEN
                INSTR(t1.map_back_category_name, "黄心奇异果")
                + INSTR(t1.map_back_category_name, "进口黄心奇异果")> 0 
            THEN "黄心奇异果"
            WHEN
                INSTR(t1.map_back_category_name, "绿心猕猴桃")
                + INSTR(t1.map_back_category_name, "绿心猕猴桃/奇异果")
                + INSTR(t1.map_back_category_name, "米良1号绿心猕猴桃")
                + INSTR(t1.map_back_category_name, "海沃德绿心猕猴桃")> 0 
            THEN "绿心猕猴桃"
            WHEN
                INSTR(t1.map_back_category_name, "红富士苹果")
                + INSTR(t1.map_back_category_name, "片红红富士苹果")> 0
            THEN "红富士苹果"
            WHEN
                INSTR(t1.map_back_category_name, "沙田柚（冰糖柚）")
                + INSTR(t1.map_back_category_name, "沙田柚")> 0
            THEN "沙田柚"
            WHEN
                INSTR(t1.map_back_category_name, "水果黄瓜")
                + INSTR(t1.map_back_category_name, "水果黄瓜00")> 0
            THEN "水果黄瓜"
            WHEN
                INSTR(t1.map_back_category_name, "无籽红提")
                + INSTR(t1.map_back_category_name, "无籽红提葡萄")> 0
            THEN "无籽红提"
            WHEN
                INSTR(t1.map_back_category_name, "香蕉")
                + INSTR(t1.map_back_category_name, "香蕉生蕉")> 0
            THEN "香蕉"
            WHEN
                INSTR(t1.map_back_category_name, "雪莲果")
                + INSTR(t1.map_back_category_name, "雪莲果00")> 0
            THEN "雪莲果"
            WHEN
                INSTR(t1.map_back_category_name, "25号小蜜瓜")
                + INSTR(t1.map_back_category_name, "25号小蜜瓜")> 0
            THEN "25号小蜜瓜"
            WHEN 
                t1.map_back_category_name IN (
                    "金标龙眼", "红标龙眼", "蓝标龙眼", "绿标龙眼", "龙眼", "进口龙眼"
                )
            THEN "龙眼"
            WHEN 
                t1.map_back_category_name IN (
                    "国产蓝莓", "进口蓝莓", "蓝莓"
                )
            THEN "蓝莓"
            ELSE t1.map_back_category_name
        END AS category_level4_name
        ,CASE
            WHEN 
                INSTR(t1.sku_name, "特A级")>0
            THEN "特A级"
            WHEN 
                INSTR(t1.sku_name, "A级")>0
            THEN "A级"
            WHEN 
                INSTR(t1.sku_name, "B级")>0
            THEN "B级"
            WHEN 
                INSTR(t1.sku_name, "C级")>0
            THEN "C级"
        END AS goods_class
    FROM changsha_dim_sku_daily_asc t1
    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
                    AND DATEADD(CURRENT_DATE(), -1, "dd")
)

-- SKU 标签相关统计
,base_sku_tag AS(

    SELECT
        t1.dt
        ,t1.mall_id
        ,t1.sku_id
        ,t1.merchant_id
        ,t1.category_level1_name
        ,t1.category_level4_name

        ,t1.is_up_today
        ,t1.created_days
        ,t1.onshelved_activity_label
        ,t1.recommend_activity_m25t0d
        ,t1.doudi_activity_m25t0d
        ,t1.disc_rate_base2new_price
        ,t1.disc_rate_base2recall_price

        ,t1.mct_supplied_price_base_m25dtcd
        ,IF(
            CAST(SLICE(SPLIT(t1.mct_supplied_price_base_m25dtcd, ","), -2, 1)[0] AS DECIMAL(10,2))>0,
                CAST(SLICE(SPLIT(t1.mct_supplied_price_base_m25dtcd, ","), -2, 1)[0] AS DECIMAL(10,2)),
                    NULL  
        ) AS mct_supplied_price_base_m1d
        ,IF(
            CAST(SLICE(SPLIT(t1.mct_supplied_price_base_m25dtcd, ","), -1, 1)[0] AS DECIMAL(10,2))>0,
                CAST(SLICE(SPLIT(t1.mct_supplied_price_base_m25dtcd, ","), -1, 1)[0] AS DECIMAL(10,2)),
                    NULL  
        ) AS mct_supplied_price_base

        ,IF(
            CAST(SLICE(SPLIT(t1.mct_supplied_price_new_m25dtcd, ","), -2, 1)[0] AS DECIMAL(10,2))>0,
                CAST(SLICE(SPLIT(t1.mct_supplied_price_new_m25dtcd, ","), -2, 1)[0] AS DECIMAL(10,2)),
                    NULL  
        ) AS mct_supplied_price_new_m1d
        ,IF(
            CAST(SLICE(SPLIT(t1.mct_supplied_price_new_m25dtcd, ","), -1, 1)[0] AS DECIMAL(10,2))>0,
                CAST(SLICE(SPLIT(t1.mct_supplied_price_new_m25dtcd, ","), -1, 1)[0] AS DECIMAL(10,2)),
                    NULL  
        ) AS mct_supplied_price_new

        ,IF(
            CAST(SLICE(SPLIT(t1.mct_supplied_price_recall_m25dtcd, ","), -2, 1)[0] AS DECIMAL(10,2))>0,
                CAST(SLICE(SPLIT(t1.mct_supplied_price_recall_m25dtcd, ","), -2, 1)[0] AS DECIMAL(10,2)),
                    NULL  
        ) AS mct_supplied_price_recall_m1d
        ,IF(
            CAST(SLICE(SPLIT(t1.mct_supplied_price_recall_m25dtcd, ","), -1, 1)[0] AS DECIMAL(10,2))>0,
                CAST(SLICE(SPLIT(t1.mct_supplied_price_recall_m25dtcd, ","), -1, 1)[0] AS DECIMAL(10,2)),
                    NULL  
        ) AS mct_supplied_price_recall
        ,ARRAY_REDUCE(
            SLICE(SPLIT(t1.mct_supplied_price_base_m25dtcd, ","), -6, 3), 0.0, 
                (buff, item) -> IF(CAST(item AS DECIMAL(10,2))>0, buff+CAST(item AS DECIMAL(10,2)), buff), buff -> buff
        )/ ARRAY_REDUCE(
            SLICE(SPLIT(t1.mct_supplied_price_base_m25dtcd, ","), -6, 3), 0, 
                (buff, item) -> IF(CAST(item AS DECIMAL(10,2))>0, buff+1, buff), buff -> buff
        ) AS avg_mct_supplied_price_base_m5dtm3d
        ,ARRAY_REDUCE(
            SLICE(SPLIT(t1.mct_supplied_price_base_m25dtcd, ","), -3, 3), 0.0, 
                (buff, item) -> IF(CAST(item AS DECIMAL(10,2))>0, buff+CAST(item AS DECIMAL(10,2)), buff), buff -> buff
        )/ ARRAY_REDUCE(
            SLICE(SPLIT(t1.mct_supplied_price_base_m25dtcd, ","), -3, 3), 0, 
                (buff, item) -> IF(CAST(item AS DECIMAL(10,2))>0, buff+1, buff), buff -> buff
        ) AS avg_mct_supplied_price_base_m2dtcd

        ,ARRAY_REDUCE(
            SLICE(SPLIT(t1.mct_supplied_price_new_m25dtcd, ","), -6, 3), 0.0, 
                (buff, item) -> IF(CAST(item AS DECIMAL(10,2))>0, buff+CAST(item AS DECIMAL(10,2)), buff), buff -> buff
        )/ ARRAY_REDUCE(
            SLICE(SPLIT(t1.mct_supplied_price_new_m25dtcd, ","), -6, 3), 0, 
                (buff, item) -> IF(CAST(item AS DECIMAL(10,2))>0, buff+1, buff), buff -> buff
        ) AS avg_mct_supplied_price_new_m5dtm3d
        ,ARRAY_REDUCE(
            SLICE(SPLIT(t1.mct_supplied_price_new_m25dtcd, ","), -3, 3), 0.0, 
                (buff, item) -> IF(CAST(item AS DECIMAL(10,2))>0, buff+CAST(item AS DECIMAL(10,2)), buff), buff -> buff
        )/ ARRAY_REDUCE(
            SLICE(SPLIT(t1.mct_supplied_price_new_m25dtcd, ","), -3, 3), 0, 
                (buff, item) -> IF(CAST(item AS DECIMAL(10,2))>0, buff+1, buff), buff -> buff
        ) AS avg_mct_supplied_price_new_m2dtcd

        ,ARRAY_REDUCE(
            SLICE(SPLIT(t1.mct_supplied_price_recall_m25dtcd, ","), -6, 3), 0.0, 
                (buff, item) -> IF(CAST(item AS DECIMAL(10,2))>0, buff+CAST(item AS DECIMAL(10,2)), buff), buff -> buff
        )/ ARRAY_REDUCE(
            SLICE(SPLIT(t1.mct_supplied_price_recall_m25dtcd, ","), -6, 3), 0, 
                (buff, item) -> IF(CAST(item AS DECIMAL(10,2))>0, buff+1, buff), buff -> buff
        ) AS avg_mct_supplied_price_recall_m5dtm3d
        ,ARRAY_REDUCE(
            SLICE(SPLIT(t1.mct_supplied_price_recall_m25dtcd, ","), -3, 3), 0.0, 
                (buff, item) -> IF(CAST(item AS DECIMAL(10,2))>0, buff+CAST(item AS DECIMAL(10,2)), buff), buff -> buff
        )/ ARRAY_REDUCE(
            SLICE(SPLIT(t1.mct_supplied_price_recall_m25dtcd, ","), -3, 3), 0, 
                (buff, item) -> IF(CAST(item AS DECIMAL(10,2))>0, buff+1, buff), buff -> buff
        ) AS avg_mct_supplied_price_recall_m2dtcd
    FROM changsha_dws_sku_operate_tag_daily_asc t1
    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
					AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t1.dimension_type = "商城+SKU"
        AND t1.category_level4_name REGEXP "秋月梨|红香酥梨|皇冠梨|香梨|凯特芒|脆红李|进口龙眼|冬枣|青枣|25号小蜜瓜|麒麟西瓜|冰糖橙|赣南脐橙|金桔|金秋砂糖桔|蜜桔|小叶桔|葡萄柚|三红蜜柚|沙田柚|大菠萝|红肉菠萝蜜|国产红心火龙果|进口红心火龙果|香蕉|金枕榴莲|红心猕猴桃|黄心奇异果|巨峰葡萄|阳光玫瑰葡萄|软籽石榴|脆柿|大红提|无籽红提|千禧樱桃小番茄|蓝莓|黑皮甘蔗|水果黄瓜|雪莲果|红富士苹果|丑桔"


)




,sku_tags AS(
	SELECT
		t1.dt
		,t1.mall_id
		,t1.merchant_id
		,t1.category_level1_name
		,t1.category_level4_name
		,COUNT(DISTINCT IF(
			t1.is_up_today>0, t1.sku_id, NULL 
		)) AS onshelved_sku_num -- `上架sku数`
		,COUNT(DISTINCT IF(
			t1.is_up_today>0 AND INSTR(t1.onshelved_activity_label, "推荐好货")>0, t1.sku_id, NULL 
		)) AS onshelved_recommend_sku_num -- `上架推荐好货活动sku数`
		,COUNT(DISTINCT IF(
			t1.is_up_today>0 AND INSTR(t1.onshelved_activity_label, "标果兜底")>0, t1.sku_id, NULL 
		)) AS onshelved_doudi_sku_num -- `上架标果兜底活动sku数`
		,MAX(ARRAY_REDUCE(
			SPLIT(SUBSTR(t1.recommend_activity_m25t0d, -7, 7), "0"), 0, (buf, item) -> IF(LENGTH(item) > buf, LENGTH(item), buf), buf->buf
		)) AS recommend_activity_m7dtcd_max -- `7日内连续上架推荐好货最大天数`
		,MAX(ARRAY_REDUCE(
			SPLIT(SUBSTR(t1.doudi_activity_m25t0d, -7, 7), "0"), 0, (buf, item) -> IF(LENGTH(item) > buf, LENGTH(item), buf), buf->buf
		)) AS doudi_activity_m7dtcd_max -- `7日内连续上架标果兜底最大天数`

		,COUNT(DISTINCT IF(
			t1.is_up_today>0 AND t1.disc_rate_base2new_price<0, t1.sku_id, NULL
		)) AS disc_rate_base2new_price_sku_num -- `新客价折扣sku数`
		,COUNT(DISTINCT IF(
			t1.is_up_today>0 AND t1.disc_rate_base2new_price< -0.07, t1.sku_id, NULL
		)) AS validate_disc_rate_base2new_price_sku_num -- `新客价有效折扣sku数`
		,COUNT(DISTINCT IF(
			t1.is_up_today>0 AND t1.disc_rate_base2recall_price<0, t1.sku_id, NULL
		)) AS disc_rate_base2recall_price_sku_num -- `召回价折扣sku数`
		,COUNT(DISTINCT IF(
			t1.is_up_today>0 AND t1.disc_rate_base2recall_price< -0.07, t1.sku_id, NULL
		)) AS validate_disc_rate_base2recall_price_sku_num -- `召回价有效折扣sku数`
		
		,COUNT(DISTINCT IF(
			t1.is_up_today>0 AND (
                (t1.created_days>3 AND t1.avg_mct_supplied_price_base_m5dtm3d < t1.avg_mct_supplied_price_base_m2dtcd)
                OR (t1.created_days BETWEEN 2 AND 3 AND t1.mct_supplied_price_base_m1d < t1.mct_supplied_price_base)
            ), t1.sku_id, NULL
		)) AS incr_price_base_sku_num -- `基准价涨价sku数`
		,COUNT(DISTINCT IF(
			t1.is_up_today>0 AND (
                (t1.created_days>3 AND t1.avg_mct_supplied_price_base_m5dtm3d = t1.avg_mct_supplied_price_base_m2dtcd)
                    OR (t1.created_days BETWEEN 2 AND 3 AND t1.mct_supplied_price_base_m1d = t1.mct_supplied_price_base)
                    OR (t1.created_days <2)
            ),t1.sku_id, NULL
		)) AS even_price_base_sku_num -- `基准价持平sku数`
		,COUNT(DISTINCT IF(
			t1.is_up_today>0 AND (
                (t1.created_days>3 AND t1.avg_mct_supplied_price_base_m5dtm3d > t1.avg_mct_supplied_price_base_m2dtcd)
                OR (t1.created_days BETWEEN 2 AND 3 AND t1.mct_supplied_price_base_m1d > t1.mct_supplied_price_base)
            ), t1.sku_id, NULL
		)) AS desc_price_base_sku_num -- `基准价降价sku数`

		,COUNT(DISTINCT IF(
			t1.is_up_today>0 AND (
                (t1.created_days>3 AND t1.avg_mct_supplied_price_new_m5dtm3d < t1.avg_mct_supplied_price_new_m2dtcd)
                OR (t1.created_days BETWEEN 2 AND 3 AND t1.mct_supplied_price_new_m1d < t1.mct_supplied_price_new)
            ), t1.sku_id, NULL
		)) AS incr_price_new_sku_num -- `新客价涨价sku数`
		,COUNT(DISTINCT IF(
			t1.is_up_today>0 AND (
                (t1.created_days>3 AND t1.avg_mct_supplied_price_new_m5dtm3d = t1.avg_mct_supplied_price_new_m2dtcd)
                    OR (t1.created_days BETWEEN 2 AND 3 AND t1.mct_supplied_price_new_m1d = t1.mct_supplied_price_new)
                    OR (t1.created_days <2)
            ),t1.sku_id, NULL
		)) AS even_price_new_sku_num -- `新客价持平sku数`
		,COUNT(DISTINCT IF(
			t1.is_up_today>0 AND (
                (t1.created_days>3 AND t1.avg_mct_supplied_price_new_m5dtm3d > t1.avg_mct_supplied_price_new_m2dtcd)
                OR (t1.created_days BETWEEN 2 AND 3 AND t1.mct_supplied_price_new_m1d > t1.mct_supplied_price_new)
            ), t1.sku_id, NULL
		)) AS desc_price_new_sku_num -- `新客价降价sku数`

		,COUNT(DISTINCT IF(
			t1.is_up_today>0 AND (
                (t1.created_days>3 AND t1.avg_mct_supplied_price_recall_m5dtm3d < t1.avg_mct_supplied_price_recall_m2dtcd)
                OR (t1.created_days BETWEEN 2 AND 3 AND t1.mct_supplied_price_recall_m1d < t1.mct_supplied_price_recall)
            ), t1.sku_id, NULL
		)) AS incr_price_recall_sku_num -- `召回价涨价sku数`
		,COUNT(DISTINCT IF(
			t1.is_up_today>0 AND (
                (t1.created_days>3 AND t1.avg_mct_supplied_price_recall_m5dtm3d = t1.avg_mct_supplied_price_recall_m2dtcd)
                    OR (t1.created_days BETWEEN 2 AND 3 AND t1.mct_supplied_price_recall_m1d = t1.mct_supplied_price_recall)
                    OR (t1.created_days <2)
            ),t1.sku_id, NULL
		)) AS even_price_recall_sku_num -- `召回价持平sku数`
		,COUNT(DISTINCT IF(
			t1.is_up_today>0 AND (
                (t1.created_days>3 AND t1.avg_mct_supplied_price_recall_m5dtm3d > t1.avg_mct_supplied_price_recall_m2dtcd)
                OR (t1.created_days BETWEEN 2 AND 3 AND t1.mct_supplied_price_recall_m1d > t1.mct_supplied_price_recall)
            ), t1.sku_id, NULL
		)) AS desc_price_recall_sku_num -- `召回价降价sku数`
	FROM base_sku_tag t1

	GROUP BY t1.dt
		,t1.mall_id
		,t1.merchant_id
		,t1.category_level1_name
		,t1.category_level4_name

)



-- 门店相关标签
,store_tag_base AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.customer_store_id -- `店铺id`
        ,t1.merchant_id -- `商家id`
        ,t2.merchant_name -- `商家名称`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_name -- `四级类目名称`
        ,MAX(t1.is_ordered_m30dtm23d) AS is_ordered_m30dtm23d -- `是否t-30到t-23下单`
        ,MAX(t1.is_ordered_m7dtm1d) AS is_ordered_m7dtm1d -- `是否t-7到t-1下单`
        ,MAX(t1.is_ordered_m6dtcd) AS is_ordered_m6dtcd -- `是否近七日下单`
        ,MAX(t1.is_ordered_m29dtcd) AS is_ordered_m29dtcd -- `是否近三十日下单`
        ,MAX(t1.exposed_m7dtcd_cnt) AS exposed_m7dtcd_cnt -- `近七日曝光次数`
        ,MAX(t1.is_ordered_cdta6d) AS is_ordered_cdta6d -- `是否后七日下单`

        ,MAX(t1.ordered_m30dtm1d) AS ordered_m30dtm1d -- `t-30到t-1下单次数`
        ,MAX(t1.ordered_m30dtm8d) AS ordered_m30dtm8d -- `t-30到t-8下单次数`
        ,MAX(t1.ordered_m7dtm1d) AS ordered_m7dtm1d -- `t-7到t-1下单次数`
        ,MAX(t1.is_ordered) AS is_ordered -- `是否当然下单`
        ,MAX(t1.is_exposed) AS is_exposed -- `当日是否曝光`
        ,SUM(t1.ordered_goods_num) AS ordered_goods_num -- `下单件数`
        ,SUM(t1.ordered_goods_amt) AS ordered_goods_amt -- `下单金额`
        ,MAX(t1.is_after_sale_applied) AS is_after_sale_applied -- `是否申请售后`
    FROM(
        SELECT
            t1.dt -- `日期`
            ,t1.mall_id -- `商城id`
            ,t1.customer_store_id -- `店铺id`
            ,t1.merchant_id -- `商家id`
            -- ,t2.merchant_name -- `商家名称`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level4_name -- `四级类目名称`
            ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -31, 23), "1")>0, 1, 0) AS is_ordered_m30dtm23d -- `是否t-30到t-23下单`
            ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -8, 7), "1")>0, 1, 0) AS is_ordered_m7dtm1d -- `是否t-7到t-1下单`
            
            ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -7, 7), "1")>0, 1, 0) AS is_ordered_m6dtcd -- `是否近七日下单`
            ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -30, 30), "1")>0, 1, 0) AS is_ordered_m29dtcd -- `是否近三十日下单`
            ,REGEXP_COUNT(SUBSTR(t1.exposed_m79t0d, -7, 7), "1") AS exposed_m7dtcd_cnt -- `近七日曝光次数`
            ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_cdta15d, 1, 7), "1")>0, 1, 0) AS is_ordered_cdta6d -- `是否后七日下单`
            ,REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -31, 30), "1") AS ordered_m30dtm1d -- `t-30到t-1下单次数`
            ,REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -31, 23), "1") AS ordered_m30dtm8d -- `t-30到t-8下单次数`
            ,REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -8, 7), "1") AS ordered_m7dtm1d -- `t-7到t-1下单次数`
            ,t1.is_ordered -- `当日是否下单`
            ,t1.is_exposed -- `当日是否曝光`
            ,t1.ordered_goods_num
            ,t1.ordered_goods_amt
            ,NULL AS is_after_sale_applied
            ,t1.dimension_type
        FROM changsha_dws_store_tag_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
					AND DATEADD(CURRENT_DATE(), -1, "dd")
            AND t1.dimension_type IN ( "商城+门店+四级类目+商家")
            
        UNION 


        SELECT
            t1.dt -- `日期`
            ,t1.mall_id -- `商城id`
            ,t1.customer_store_id -- `店铺id`
            ,t1.merchant_id -- `商家id`
            ,t2.category_level1_name
            ,t2.category_level4_name

            ,NULL AS is_ordered_m30dtm23d -- `是否t-30到t-23下单`
            ,NULL AS is_ordered_m7dtm1d -- `是否t-7到t-1下单`
            ,NULL AS is_ordered_m6dtcd -- `是否近七日下单`
            ,NULL AS is_ordered_m29dtcd -- `是否近三十日下单`
            ,NULL AS exposed_m7dtcd_cnt -- `近七日曝光次数`
            ,NULL AS is_ordered_cdta6d -- `是否后七日下单`
            ,NULL AS ordered_m30dtm1d -- `t-30到t-1下单次数`
            ,NULL AS ordered_m30dtm8d -- `t-30到t-8下单次数`
            ,NULL AS ordered_m7dtm1d -- `t-7到t-1下单次数`
            ,NULL AS is_ordered
            ,NULL AS is_exposed -- `当日是否曝光`
            ,NULL AS ordered_goods_num
            ,NULL AS ordered_goods_amt
            ,1 AS is_after_sale_applied
            ,"售后" AS dimension_type
        FROM datawarehouse_max.dwt_order_after_sale_daily_asc t1
        LEFT JOIN changsha_dim_sku_daily_asc t2
            ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
					AND DATEADD(CURRENT_DATE(), -1, "dd")
            AND t2.dt = t1.dt
            AND t2.mall_id = t1.mall_id
            AND t2.sku_id = t1.sku_id
        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
					AND DATEADD(CURRENT_DATE(), -1, "dd")
            AND t1.status != "CANCEL"
            AND t1.mall_id = 871
    ) t1
    LEFT JOIN datawarehouse_max.dim_merchant_daily_full t2
        ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
					AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t2.mall_id = t1.mall_id
        AND t2.merchant_id = t1.merchant_id
        AND t2.dt = t1.dt
    
    WHERE t1.category_level4_name IN (
            SELECT
                t1.category_level4_name
            FROM changsha_t_cate4_type_conf t1
        )

    GROUP BY t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.customer_store_id -- `店铺id`
        ,t1.merchant_id -- `商家id`
        ,t2.merchant_name -- `商家名称`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_name -- `四级类目名称`
)

,store_tags AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.merchant_id -- `商家id`
        ,t1.merchant_name -- `商家名称`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_name -- `四级类目名称`

        -- 活跃统计
        ,COUNT(DISTINCT IF(
            t1.is_exposed>0, t1.customer_store_id, NULL
        )) AS exposed_store_num -- `当日曝光店铺数`
        ,COUNT(DISTINCT IF(
            t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS ordered_store_num -- `当日下单店铺数`
        ,COUNT(DISTINCT IF(
            t1.is_ordered_m6dtcd>0, t1.customer_store_id, NULL
        )) AS ordered_store_num_m6dtcd -- `近七日下单店铺数`
        ,COUNT(DISTINCT IF(
            t1.is_ordered_m29dtcd>0, t1.customer_store_id, NULL
        )) AS ordered_store_num_m29dtcd -- `近三十日下单店铺数`
        -- 新老客户
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)>4 AND NVL(t1.ordered_m7dtm1d, 0) >=1, t1.customer_store_id, NULL
        )) AS old_store_pool -- `老客户池`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)>4 AND NVL(t1.ordered_m7dtm1d, 0) >=1 AND t1.is_ordered > 0, t1.customer_store_id, NULL
        )) AS old_store_ordered_store_num -- `老客下单店铺数`
        ,SUM(IF(
            NVL(t1.ordered_m30dtm1d,0)>4 AND NVL(t1.ordered_m7dtm1d, 0) >=1, t1.ordered_goods_num, NULL
        )) AS old_store_ordered_goods_num -- `老客下单件数`
        ,SUM(IF(
            NVL(t1.ordered_m30dtm1d,0)>4 AND NVL(t1.ordered_m7dtm1d, 0) >=1, t1.ordered_goods_amt, NULL
        )) AS old_store_ordered_goods_amt -- `老客下单金额`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)>4 AND NVL(t1.ordered_m7dtm1d, 0) >=1 AND t1.is_exposed > 0, t1.customer_store_id, NULL
        )) AS old_store_exposed_store_num -- `老客曝光店铺数`


        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm8d,0)>=4 AND NVL(t1.ordered_m7dtm1d, 0) =0, t1.customer_store_id, NULL
        )) AS loss_store_pool -- `流失客户池`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm8d,0)>=4 AND NVL(t1.ordered_m7dtm1d, 0) =0 AND t1.is_ordered > 0, t1.customer_store_id, NULL
        )) AS loss_store_ordered_store_num -- `流失客户下单店铺数`
        ,SUM(IF(
            NVL(t1.ordered_m30dtm8d,0)>=4 AND NVL(t1.ordered_m7dtm1d, 0) =0, t1.ordered_goods_num, NULL
        )) AS loss_store_ordered_goods_num -- `流失客户下单件数`
        ,SUM(IF(
            NVL(t1.ordered_m30dtm8d,0)>=4 AND NVL(t1.ordered_m7dtm1d, 0) =0, t1.ordered_goods_amt, NULL
        )) AS loss_store_ordered_goods_amt -- `流失客户下单金额`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm8d,0)>=4 AND NVL(t1.ordered_m7dtm1d, 0) =0 AND t1.is_exposed> 0, t1.customer_store_id, NULL
        )) AS loss_store_exposed_store_num -- `流失客户曝光店铺数`
        
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm8d,0)=3 AND NVL(t1.ordered_m7dtm1d, 0)=0, t1.customer_store_id, NULL
        )) AS price_loss_store_pool -- `老客价流失客户池`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm8d,0)=3 AND NVL(t1.ordered_m7dtm1d, 0)=0 AND t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS price_loss_store_ordered_sore_num -- `老客价流失客户下单店铺数`
        ,SUM(IF(
            NVL(t1.ordered_m30dtm8d,0)=3 AND NVL(t1.ordered_m7dtm1d, 0)=0, t1.ordered_goods_num, NULL
        )) AS price_loss_store_ordered_goods_num -- `老客价流失流失客户下单件数`
        ,SUM(IF(
            NVL(t1.ordered_m30dtm8d,0)=3 AND NVL(t1.ordered_m7dtm1d, 0)=0, t1.ordered_goods_amt, NULL
        )) AS price_loss_store_ordered_goods_amt -- `老客价流失流失客户下单金额`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm8d,0)=3 AND NVL(t1.ordered_m7dtm1d, 0)=0 AND t1.is_exposed>0, t1.customer_store_id, NULL
        )) AS price_loss_store_exposed_sore_num -- `老客价流失客户曝光店铺数`

        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm8d,0)=2 AND NVL(t1.ordered_m7dtm1d, 0)=1, t1.customer_store_id, NULL
        )) AS new_store_ordered3cnt_pool -- `下单三次新客户池`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm8d,0)=2 AND NVL(t1.ordered_m7dtm1d, 0)=1 AND t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS new_store_ordered3cnt_ordered_sore_num -- `下单三次新客户下单店铺数`
        ,SUM(IF(
            NVL(t1.ordered_m30dtm8d,0)=2 AND NVL(t1.ordered_m7dtm1d, 0)=1, t1.ordered_goods_num, NULL
        )) AS new_store_ordered3cnt_ordered_goods_num -- `下单三次新客户下单件数`
        ,SUM(IF(
            NVL(t1.ordered_m30dtm8d,0)=2 AND NVL(t1.ordered_m7dtm1d, 0)=1, t1.ordered_goods_amt, NULL
        )) AS new_store_ordered3cnt_ordered_goods_amt -- `下单三次新客户下单金额`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm8d,0)=2 AND NVL(t1.ordered_m7dtm1d, 0)=1 AND t1.is_exposed>0, t1.customer_store_id, NULL
        )) AS new_store_ordered3cnt_exposed_sore_num -- `下单三次新客户曝光店铺数`

        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)=2, t1.customer_store_id, NULL
        )) AS new_store_ordered2cnt_pool -- `下单两次新客户池`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)=2 AND t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS new_store_ordered2cnt_ordered_sore_num -- `下单两次新客户下单店铺数`
        ,SUM(IF(
            NVL(t1.ordered_m30dtm1d,0)=2, t1.ordered_goods_num, NULL
        )) AS new_store_ordered2cnt_ordered_goods_num -- `下单两次新客户下单件数`
        ,SUM(IF(
            NVL(t1.ordered_m30dtm1d,0)=2, t1.ordered_goods_amt, NULL
        )) AS new_store_ordered2cnt_ordered_goods_amt -- `下单两次新客户下单金额`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)=2 AND t1.is_exposed>0, t1.customer_store_id, NULL
        )) AS new_store_ordered2cnt_exposed_sore_num -- `下单两次新客户曝光店铺数`

        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)=1, t1.customer_store_id, NULL
        )) AS new_store_ordered1cnt_pool -- `下单一次新客户池`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)=1 AND t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS new_store_ordered1cnt_ordered_store_num -- `下单一次新客户下单店铺数`
        ,SUM(IF(
            NVL(t1.ordered_m30dtm1d,0)=1, t1.ordered_goods_num, NULL
        )) AS new_store_ordered1cnt_ordered_goods_num -- `下单一次新客户下单件数`
        ,SUM(IF(
            NVL(t1.ordered_m30dtm1d,0)=1, t1.ordered_goods_amt, NULL
        )) AS new_store_ordered1cnt_ordered_goods_amt -- `下单一次新客户下单金额`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)=1 AND t1.is_exposed>0, t1.customer_store_id, NULL
        )) AS new_store_ordered1cnt_exposed_store_num -- `下单一次新客户曝光店铺数`

        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)=0, t1.customer_store_id, NULL
        )) AS new_store_ordered0cnt_pool -- `下单零次新客户池`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)=0 AND t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS new_store_ordered0cnt_ordered_store_num -- `下单零次新客户下单店铺数`
        ,SUM(IF(
            NVL(t1.ordered_m30dtm1d,0)=0, t1.ordered_goods_num, NULL
        )) AS new_store_ordered0cnt_ordered_goods_num -- `下单零次新客户下单件数`
        ,SUM(IF(
            NVL(t1.ordered_m30dtm1d,0)=0, t1.ordered_goods_amt, NULL
        )) AS new_store_ordered0cnt_ordered_goods_amt -- `下单零次新客户下单金额`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)=0 AND t1.is_exposed>0, t1.customer_store_id, NULL
        )) AS new_store_ordered0cnt_exposed_store_num -- `下单零次新客户曝光店铺数`


        ,COUNT(DISTINCT IF(
            t1.exposed_m7dtcd_cnt=1, t1.customer_store_id, NULL 
        )) AS exposed_1cnt_store_num_m7dtcd -- `近七日曝光一次店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_m7dtcd_cnt=1 AND NVL(t1.is_ordered_m6dtcd,0)=0, t1.customer_store_id, NULL 
        )) AS exposed_1cnt_non_ordered_store_num_m7dtcd -- `近七日曝光一次未下单店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_m7dtcd_cnt=2, t1.customer_store_id, NULL 
        )) AS exposed_2cnt_store_num_m7dtcd -- `近七日曝光两次店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_m7dtcd_cnt=2 AND NVL(t1.is_ordered_m6dtcd,0)=0, t1.customer_store_id, NULL 
        )) AS exposed_2cnt_non_ordered_store_num_m7dtcd -- `近七日曝光两次未下单店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_m7dtcd_cnt=3, t1.customer_store_id, NULL 
        )) AS exposed_3cnt_store_num_m7dtcd -- `近七日曝光三次店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_m7dtcd_cnt=3 AND NVL(t1.is_ordered_m6dtcd,0)=0, t1.customer_store_id, NULL 
        )) AS exposed_3cnt_non_ordered_store_num_m7dtcd -- `近七日曝光三次未下单店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_m7dtcd_cnt=4, t1.customer_store_id, NULL 
        )) AS exposed_4cnt_store_num_m7dtcd -- `近七日曝光四次店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_m7dtcd_cnt=4 AND NVL(t1.is_ordered_m6dtcd,0)=0, t1.customer_store_id, NULL 
        )) AS exposed_4cnt_non_ordered_store_num_m7dtcd -- `近七日曝光四次未下单店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_m7dtcd_cnt=5, t1.customer_store_id, NULL 
        )) AS exposed_5cnt_store_num_m7dtcd -- `近七日曝光五次店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_m7dtcd_cnt=5 AND NVL(t1.is_ordered_m6dtcd,0)=0, t1.customer_store_id, NULL 
        )) AS exposed_5cnt_non_ordered_store_num_m7dtcd -- `近七日曝光五次未下单店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_m7dtcd_cnt=6, t1.customer_store_id, NULL 
        )) AS exposed_6cnt_store_num_m7dtcd -- `近七日曝光六次店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_m7dtcd_cnt=6 AND NVL(t1.is_ordered_m6dtcd,0)=0, t1.customer_store_id, NULL 
        )) AS exposed_6cnt_non_ordered_store_num_m7dtcd -- `近七日曝光六次未下单店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_m7dtcd_cnt=7, t1.customer_store_id, NULL 
        )) AS exposed_7cnt_store_num_m7dtcd -- `近七日曝光七次店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_m7dtcd_cnt=7 AND NVL(t1.is_ordered_m6dtcd,0)=0, t1.customer_store_id, NULL 
        )) AS exposed_7cnt_non_ordered_store_num_m7dtcd -- `近七日曝光七次未下单店铺数`

        ,COUNT(DISTINCT IF(
            t1.is_after_sale_applied>0, t1.customer_store_id, NULL
        )) AS after_sale_applied_store_num -- `申请售后店铺数`
        ,COUNT(DISTINCT IF(
            t1.is_after_sale_applied>0 AND NVL(t1.is_ordered_cdta6d,0)>0, t1.customer_store_id, NULL
        )) AS after_sale_applied2ordered_store_num -- `申请售后7日复购店铺数`
        ,COUNT(DISTINCT IF(
            t1.is_after_sale_applied>0 AND NVL(t1.is_ordered_cdta6d,0)=0, t1.customer_store_id, NULL
        )) AS after_sale_applied2non_ordered_store_num -- `申请售后7日流失店铺数`
    FROM store_tag_base t1

    GROUP BY t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.merchant_id -- `商家id`
        ,t1.merchant_name -- `商家名称`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_name -- `四级类目名称`

)



-- SKU 等级标签统计

,sku_class_base AS(
    SELECT
        t1.dt
        ,t1.mall_id
        ,t0.mall
        ,t1.sku_id
        ,t1.sku_name
        ,t1.goods_class
        ,t1.is_up_today
        ,t1.merchant_id
        ,t1.category_level1_name
        ,t1.category_level4_name
        ,t2.customer_store_id
        ,t2.ordered_goods_num
    FROM dim_sku t1
    LEFT JOIN changsha_trade_fact_daily_asc t2
        ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
                AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t2.dt = t1.dt
        AND t2.mall_id = t1.mall_id
        AND t2.sku_id = t1.sku_id
        AND t2.dimention_type = "商城+门店+SKU"
    
    JOIN mall t0
        ON t0.mall_id = t1.mall_id
    WHERE INSTR(t1.category_level1_name, "水果") > 0
)


,sku_class_stat AS(
    SELECT
        t1.dt
        ,t1.mall_id
        ,t1.merchant_id
        ,t1.category_level1_name
        ,t1.category_level4_name
        ,t1.goods_class
        ,t1.sku_num
        ,t1.coverage_ratio
        ,FIRST_VALUE(IF(t1.coverage_ratio>0, t1.goods_class, NULL))OVER(
            PARTITION BY t1.dt, t1.mall_id, t1.category_level1_name, t1.category_level4_name ORDER BY t1.coverage_ratio DESC
        ) AS top1_cov_ratio_goods_class -- `top1商家品类等级日活覆盖率等级`
        ,MAX(t1.coverage_ratio)OVER(
            PARTITION BY t1.dt, t1.mall_id, t1.category_level1_name, t1.category_level4_name
        ) AS top1_cov_ratio -- `top1商家品类等级日活覆盖率`

        ,FIRST_VALUE(IF(t1.coverage_ratio>0, t1.goods_class, NULL))OVER(
            PARTITION BY t1.dt, t1.category_level1_name, t1.category_level4_name ORDER BY t1.coverage_ratio DESC
        ) AS total_top1_cov_ratio_goods_class -- `整体top1商家品类等级日活覆盖率等级`
        ,FIRST_VALUE(IF(t1.coverage_ratio>0, t1.mall, NULL))OVER(
            PARTITION BY t1.dt, t1.category_level1_name, t1.category_level4_name ORDER BY t1.coverage_ratio DESC
        ) AS total_top1_cov_ratio_mall -- `整体top1商家品类等级日活覆盖率商城`
        ,MAX(t1.coverage_ratio)OVER(
            PARTITION BY t1.dt, t1.category_level1_name, t1.category_level4_name
        ) AS total_top1_cov_ratio -- `整体top1商家品类等级日活覆盖率`
    FROM(
        SELECT
            t1.dt
            ,t1.mall_id
            ,t1.mall
            ,t1.merchant_id
            ,t1.category_level1_name
            ,t1.category_level4_name
            ,t1.goods_class
            ,COUNT(DISTINCT IF(t1.is_up_today>0, t1.sku_id, NULL)) AS sku_num
            ,COUNT(DISTINCT IF(t1.ordered_goods_num>0, t1.customer_store_id, NULL)) AS ordered_store_num
            ,CAST(
                COUNT(DISTINCT IF(t1.ordered_goods_num>0, t1.customer_store_id, NULL)) / 
                    MAX(t2.ordered_store_num) AS DECIMAL(10,5)
            ) AS coverage_ratio
        FROM sku_class_base t1
        LEFT JOIN (
            SELECT
                t1.dt
                ,t1.mall_id
                ,t1.category_level1_name
                ,t1.category_level4_name
                ,COUNT(DISTINCT IF(t1.ordered_goods_num>0, t1.customer_store_id, NULL)) AS ordered_store_num

            FROM sku_class_base t1
            GROUP BY t1.dt
                ,t1.mall_id
                ,t1.category_level1_name
                ,t1.category_level4_name
        ) t2
            ON t2.dt = t1.dt
            AND t2.mall_id = t1.mall_id
            AND t2.category_level1_name = t1.category_level1_name
            AND t2.category_level4_name = t1.category_level4_name
        GROUP BY t1.dt
            ,t1.mall_id
            ,t1.mall
            ,t1.merchant_id
            ,t1.category_level1_name
            ,t1.category_level4_name
            ,t1.goods_class
    ) t1
)


,sku_class_tag AS(
    SELECT
        t1.dt
        ,t1.mall_id
        ,t1.merchant_id
        ,t1.category_level1_name
        ,t1.category_level4_name
        ,SUM(IF(t1.goods_class="特A级", t1.sku_num, 0)) AS sku_num_sa_class -- `特a级sku数量`
        ,SUM(IF(t1.goods_class="A级", t1.sku_num, 0)) AS sku_num_a_class -- `a级sku数量`
        ,SUM(IF(t1.goods_class="B级", t1.sku_num, 0)) AS sku_num_b_class -- `b级sku数量`
        ,SUM(IF(t1.goods_class="C级", t1.sku_num, 0)) AS sku_num_c_class -- `c级sku数量`
        ,MAX(t1.top1_cov_ratio_goods_class) AS top1_cov_ratio_goods_class -- `top1商家品类等级日活覆盖率等级`
        ,MAX(t1.top1_cov_ratio) AS top1_cov_ratio -- `top1商家品类等级日活覆盖率`
        ,MAX(t1.total_top1_cov_ratio_goods_class) AS total_top1_cov_ratio_goods_class -- `整体top1商家品类等级日活覆盖率等级`
        ,MAX(t1.total_top1_cov_ratio_mall) AS total_top1_cov_ratio_mall -- `整体top1商家品类等级日活覆盖率商城`
        ,MAX(t1.total_top1_cov_ratio) AS total_top1_cov_ratio -- `整体top1商家品类等级日活覆盖率`
    FROM sku_class_stat t1
    WHERE t1.mall_id = 871
    GROUP BY t1.dt
        ,t1.mall_id
        ,t1.merchant_id
        ,t1.category_level1_name
        ,t1.category_level4_name
)


-- 门店商品标签

,ssp_act_tag_base AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.customer_store_id -- `店铺id`
        ,t1.merchant_id -- `商家id`
        ,t1.sku_id
        ,NVL(t2.is_up_today,0) AS is_up_today
        ,t2.category_level1_name
        ,t2.category_level4_name
        ,t1.is_ordered -- `当日是否下单`
        ,t1.is_exposed -- `当日是否曝光`

        ,CASE
            WHEN
                NVL(t1.ordered_m30dtm1d,0)>4 AND NVL(t1.ordered_m7dtm1d, 0) >=1
            THEN "OLD"
            WHEN
                NVL(t1.ordered_m30dtm8d,0)>=4 AND NVL(t1.ordered_m7dtm1d, 0) =0
            THEN "LOSS"
            WHEN
                NVL(t1.ordered_m30dtm8d,0)=3 AND NVL(t1.ordered_m7dtm1d, 0)=0
            THEN "LOSS_PRICE"
            WHEN
                NVL(t1.ordered_m30dtm8d,0)=2 AND NVL(t1.ordered_m7dtm1d, 0)=1
            THEN "NEW3"
            WHEN
                NVL(t1.ordered_m30dtm1d,0)=2
            THEN "NEW2"
            WHEN
                NVL(t1.ordered_m30dtm1d,0)=1
            THEN "NEW1"
            ELSE "NEW0"
        END AS ssp_activity_tag

    FROM(

        SELECT
            t1.dt -- `日期`
            ,t1.mall_id -- `商城id`
            ,t1.customer_store_id -- `店铺id`
            ,t1.merchant_id -- `商家id`
            ,t1.sku_id
            ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -31, 23), "1")>0, 1, 0) AS is_ordered_m30dtm23d -- `是否t-30到t-23下单`
            ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -8, 7), "1")>0, 1, 0) AS is_ordered_m7dtm1d -- `是否t-7到t-1下单`
            
            ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -7, 7), "1")>0, 1, 0) AS is_ordered_m6dtcd -- `是否近七日下单`
            ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -30, 30), "1")>0, 1, 0) AS is_ordered_m29dtcd -- `是否近三十日下单`
            ,REGEXP_COUNT(SUBSTR(t1.exposed_m79t0d, -7, 7), "1") AS exposed_m7dtcd_cnt -- `近七日曝光次数`
            ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_cdta15d, 1, 7), "1")>0, 1, 0) AS is_ordered_cdta6d -- `是否后七日下单`
            ,REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -31, 30), "1") AS ordered_m30dtm1d -- `t-30到t-1下单次数`
            ,REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -31, 23), "1") AS ordered_m30dtm8d -- `t-30到t-8下单次数`
            ,REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -8, 7), "1") AS ordered_m7dtm1d -- `t-7到t-1下单次数`
            ,t1.is_ordered -- `当日是否下单`
            ,t1.is_exposed -- `当日是否曝光`

        FROM changsha_dws_store_tag_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
                    AND DATEADD(CURRENT_DATE(), -1, "dd")
            AND t1.dimension_type = "商城+门店+SKU"
    ) t1
    LEFT JOIN dim_sku t2
        ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
                    AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t2.dt = t1.dt
        AND t2.mall_id = t1.mall_id
        AND t2.sku_id = t1.sku_id
)



,ssp_stat AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.merchant_id -- `商家id`
        ,t1.sku_id
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_name -- `四级类目名称`

        ,COUNT(DISTINCT IF(
            t1.ssp_activity_tag IN ("OLD") AND t1.is_exposed>0, t1.customer_store_id, NULL
        )) AS exposed_old_store_num -- `曝光老客户门店数`
        ,COUNT(DISTINCT IF(
            t1.ssp_activity_tag IN ("OLD") AND t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS ordered_old_store_num -- `下单老客户门店数`
        ,COUNT(DISTINCT IF(
            t1.ssp_activity_tag IN ("LOSS") AND t1.is_exposed>0, t1.customer_store_id, NULL
        )) AS exposed_loss_store_num -- `曝光流失客户门店数`
        ,COUNT(DISTINCT IF(
            t1.ssp_activity_tag IN ("LOSS") AND t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS ordered_loss_store_num -- `下单流失客户门店数`

        ,COUNT(DISTINCT IF(
            t1.ssp_activity_tag IN ("LOSS_PRICE") AND t1.is_exposed>0, t1.customer_store_id, NULL
        )) AS exposed_loss_price_store_num -- `曝光价格流失客户门店数`
        ,COUNT(DISTINCT IF(
            t1.ssp_activity_tag IN ("LOSS_PRICE") AND t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS ordered_loss_price_store_num -- `下单价格流失客户门店数`

        ,COUNT(DISTINCT IF(
            INSTR(NVL(t1.ssp_activity_tag, "NEW"), "NEW")>0 AND t1.is_exposed>0, t1.customer_store_id, NULL
        )) AS exposed_new_store_num -- `曝光新客户门店数`
        ,COUNT(DISTINCT IF(
            INSTR(NVL(t1.ssp_activity_tag, "NEW"), "NEW")>0  AND t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS ordered_new_store_num -- `下单新客户门店数`
    FROM ssp_act_tag_base t1
    WHERE ISNOTNULL(t1.category_level4_name)
        AND t1.is_up_today > 0
    GROUP BY t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.merchant_id -- `商家id`
        ,t1.sku_id
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_name -- `四级类目名称`
)



,ssp_tags AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.merchant_id -- `商家id`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_name -- `四级类目名称`

        ,COUNT(DISTINCT IF(
            t1.ordered_old_store_num / t1.exposed_old_store_num > 0.3, t1.sku_id, NULL
        )) AS old_store_exposed2ordered_ratio_o30_sku_num -- `老客转化率大于30的sku数`
        ,COUNT(DISTINCT IF(
            t1.ordered_loss_store_num / t1.exposed_loss_store_num > 0.07, t1.sku_id, NULL
        )) AS loss_store_exposed2ordered_ratio_o30_sku_num -- `流失客转化率大于7的sku数`
        ,COUNT(DISTINCT IF(
            t1.ordered_loss_price_store_num / t1.exposed_loss_price_store_num > 0.07, t1.sku_id, NULL
        )) AS loss_price_store_exposed2ordered_ratio_o30_sku_num -- `价格流失客转化率大于7的sku数`
        ,COUNT(DISTINCT IF(
            t1.ordered_new_store_num / t1.exposed_new_store_num > 0.07, t1.sku_id, NULL
        )) AS new_store_exposed2ordered_ratio_o30_sku_num -- `新客转化率大于7的sku数`
    FROM ssp_stat t1
    GROUP BY t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.merchant_id -- `商家id`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_name -- `四级类目名称`
)


-- 营销费用
,fees AS(
    -- FIXED: 2025-04-21 营销费用计算 7 个归属日内的费用
    SELECT
        DATE(t1.statement_date) AS dt
        ,t1.goods_id AS sku_id
        ,871 AS mall_id
        
        ,CAST(SUM(t1.new_market_fee) AS DECIMAL(10,2)) AS new_mct_market_expense_amt -- `新客户商家营销费用`
        ,CAST(SUM(t1.recall_market_fee) AS DECIMAL(10,2)) AS recall_mct_market_expense_amt -- `召回客商家营销费用`
        ,CAST(SUM(NVL(t1.new_market_fee, 0) + NVL(t1.recall_market_fee, 0)) AS DECIMAL(10,2)) AS total_mct_market_expense_amt -- `商家营销费用金额`
    FROM datawarehouse_max.ods_seller_service_demeter_seller_market_fee_count_asc t1
    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -21, "dd")
                        AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND DATE(t1.statement_date) BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
                        AND DATEADD(CURRENT_DATE(), -1, "dd")
        
    GROUP BY DATE(t1.statement_date)
        ,t1.goods_id
)


-- 商城周活和月活
,mall_stat AS(
    SELECT
        t1.dt
        ,t1.mall_id
        ,COUNT(DISTINCT IF(
            t1.ordered_m6dtcd>0, t1.customer_store_id, NULL
        )) AS ordered_store_num_m6dtcd -- `近七日下单店铺数`
        ,COUNT(DISTINCT IF(
            t1.ordered_m29dtcd>0, t1.customer_store_id, NULL
        )) AS ordered_store_num_m29dtcd -- `近三十日下单店铺数`
    FROM(
        SELECT
            t1.dt -- `日期`
            ,t1.mall_id -- `商城id`
            ,t1.customer_store_id -- `店铺id`
            ,REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -7, 7), "1") AS ordered_m6dtcd -- `近七日下单次数`
            ,REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -30, 30), "1") AS ordered_m29dtcd -- `近三十日下单次数`
        FROM changsha_dws_store_tag_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
                    AND DATEADD(CURRENT_DATE(), -1, "dd")
            AND t1.dimension_type IN ( "商城+门店")
    ) t1

    GROUP BY t1.dt
        ,t1.mall_id
)

SELECT
	t1.dt AS `日期`
	,t1.mall_id AS `商城ID`
	,t1.mall AS `商城`
	,t1.merchant_id AS `商家ID`
	,t1.merchant_name AS `商家名称`
	,t1.category_level4_name AS `四级类目名称`
	,t1.category_level1_name AS `一级类目名称`

	,t1.ordered_goods_num AS `下单件数`
	,t1.ordered_goods_amt AS `下单金额`
	,t1.ordered_goods_wgt AS `下单重量`
	,t1.ordered_store_num AS `下单店铺数`
	,t1.delivered_goods_num AS `送货件数`
	,t1.delivered_goods_amt AS `送货金额`
	,t1.delivered_gross_wgt AS `送货重量`
	,NVL(t1.platform_commission_amt,0) AS `平台抽佣金额`
	,NVL(t1.ordered_item_ticket_num,0) AS `明细订单量`
	,NVL(t1.after_sale_applied_num,0) AS `售后申请件数`
	,NVL(t1.after_sale_applied_ticket_num,0) AS `售后申请订单量`
	,NVL(t1.after_sale_refund_num,0) AS `售后赔付件数`
	,NVL(t1.after_sale_refund_amt,0) AS `售后赔付金额`
	,NVL(t1.after_sale_applied_store_num,0) AS `申请售后店铺数`
	,NVL(t1.high_refund_amt_ratio_ticket_num,0) AS `高售后赔付订单量`
    ,NVL(t6.total_mct_market_expense_amt, 0) AS `商家营销费用金额`
	,NVL(t2.onshelved_sku_num,0) AS `上架sku数`
	,NVL(t2.onshelved_recommend_sku_num,0) AS `上架推荐好货活动sku数`
	,NVL(t2.onshelved_doudi_sku_num,0) AS `上架标果兜底活动sku数`
	,NVL(t2.recommend_activity_m7dtcd_max,0) AS `7日内连续上架推荐好货最大天数`
	,NVL(t2.doudi_activity_m7dtcd_max,0) AS `7日内连续上架标果兜底最大天数`

	,CAST(NVL(t2.disc_rate_base2new_price_sku_num,0) AS INT) AS `新客价折扣sku数`
	,CAST(NVL(t2.validate_disc_rate_base2new_price_sku_num,0) AS INT) AS `新客价有效折扣sku数`
	,CAST(NVL(t2.disc_rate_base2recall_price_sku_num,0) AS INT) AS `召回价折扣sku数`
	,CAST(NVL(t2.validate_disc_rate_base2recall_price_sku_num,0) AS INT) AS `召回价有效折扣sku数`

	,CAST(NVL(t2.incr_price_base_sku_num,0) AS INT) AS `基准价涨价sku数`
	,CAST(NVL(t2.even_price_base_sku_num,0) AS INT) AS `基准价持平sku数`
	,CAST(NVL(t2.desc_price_base_sku_num,0) AS INT) AS `基准价降价sku数`

	,CAST(NVL(t2.incr_price_new_sku_num,0) AS INT) AS `新客价涨价sku数`
	,CAST(NVL(t2.even_price_new_sku_num,0) AS INT) AS `新客价持平sku数`
	,CAST(NVL(t2.desc_price_new_sku_num,0) AS INT) AS `新客价降价sku数`

	,CAST(NVL(t2.incr_price_recall_sku_num,0) AS INT) AS `召回价涨价sku数`
	,CAST(NVL(t2.even_price_recall_sku_num,0) AS INT) AS `召回价持平sku数`
	,CAST(NVL(t2.desc_price_recall_sku_num,0) AS INT) AS `召回价降价sku数`

	-- 活跃统计
	,CAST(NVL(t3.exposed_store_num,0) AS INT) AS `当日曝光店铺数`
	,CAST(NVL(t3.ordered_store_num,0) AS INT) AS `当日下单店铺数`
	,CAST(NVL(t3.ordered_store_num_m6dtcd,0) AS INT) AS `近七日下单店铺数`
	,CAST(NVL(t3.ordered_store_num_m29dtcd,0) AS INT) AS `近三十日下单店铺数`
	-- 新老客户
	,CAST(NVL(t3.old_store_pool,0) AS INT) AS `老客户池`
	,CAST(NVL(t3.old_store_ordered_store_num,0) AS INT) AS `老客下单店铺数`
	,NVL(t3.old_store_ordered_goods_num,0) AS `老客下单件数`

	,CAST(NVL(t3.loss_store_pool,0) AS INT) AS `流失客户池`
	,CAST(NVL(t3.loss_store_ordered_store_num,0) AS INT) AS `流失客户下单店铺数`
	,NVL(t3.loss_store_ordered_goods_num,0) AS `流失客户下单件数`
        
	,CAST(NVL(t3.price_loss_store_pool,0) AS INT) AS `老客价流失客户池`
	,CAST(NVL(t3.price_loss_store_ordered_sore_num,0) AS INT) AS `老客价流失客户下单店铺数`
	,NVL(t3.price_loss_store_ordered_goods_num,0) AS `老客价流失流失客户下单件数`

	,CAST(NVL(t3.new_store_ordered3cnt_pool,0) AS INT) AS `下单三次新客户池`
	,CAST(NVL(t3.new_store_ordered3cnt_ordered_sore_num,0) AS INT) AS `下单三次新客户下单店铺数`
	,NVL(t3.new_store_ordered3cnt_ordered_goods_num,0) AS `下单三次新客户下单件数`

	,CAST(NVL(t3.new_store_ordered2cnt_pool,0) AS INT) AS `下单两次新客户池`
	,CAST(NVL(t3.new_store_ordered2cnt_ordered_sore_num,0) AS INT) AS `下单两次新客户下单店铺数`
	,NVL(t3.new_store_ordered2cnt_ordered_goods_num,0) AS `下单两次新客户下单件数`


	,CAST(NVL(t3.new_store_ordered1cnt_pool,0) AS INT) AS `下单一次新客户池`
	,CAST(NVL(t3.new_store_ordered1cnt_ordered_store_num,0) AS INT) AS `下单一次新客户下单店铺数`
	,NVL(t3.new_store_ordered1cnt_ordered_goods_num,0) AS `下单一次新客户下单件数`

	,CAST(NVL(t3.new_store_ordered0cnt_pool,0) AS INT) AS `下单零次新客户池`
	,CAST(NVL(t3.new_store_ordered0cnt_ordered_store_num,0) AS INT) AS `下单零次新客户下单店铺数`
	,CAST(NVL(t3.new_store_ordered0cnt_ordered_goods_num,0) AS INT) AS `下单零次新客户下单件数`
    ,NVL(t3.old_store_ordered_goods_amt,0) AS `老客下单金额`
    ,NVL(t3.loss_store_ordered_goods_amt,0) AS `流失客户下单金额`
    ,NVL(t3.price_loss_store_ordered_goods_amt,0) AS `老客价流失流失客户下单金额`
    ,NVL(t3.new_store_ordered3cnt_ordered_goods_amt,0) AS `下单三次新客户下单金额`
    ,NVL(t3.new_store_ordered2cnt_ordered_goods_amt,0) AS `下单两次新客户下单金额`
    ,NVL(t3.new_store_ordered1cnt_ordered_goods_amt,0) AS `下单一次新客户下单金额`
    ,NVL(t3.new_store_ordered0cnt_ordered_goods_amt,0) AS `下单零次新客户下单金额`

	,NVL(t3.exposed_1cnt_store_num_m7dtcd,0) AS `近七日曝光一次店铺数`
	,NVL(t3.exposed_1cnt_non_ordered_store_num_m7dtcd,0) AS `近七日曝光一次未下单店铺数`
	,NVL(t3.exposed_2cnt_store_num_m7dtcd,0) AS `近七日曝光两次店铺数`
	,NVL(t3.exposed_2cnt_non_ordered_store_num_m7dtcd,0) AS `近七日曝光两次未下单店铺数`
	,NVL(t3.exposed_3cnt_store_num_m7dtcd,0) AS `近七日曝光三次店铺数`
	,NVL(t3.exposed_3cnt_non_ordered_store_num_m7dtcd,0) AS `近七日曝光三次未下单店铺数`
	,NVL(t3.exposed_4cnt_store_num_m7dtcd,0) AS `近七日曝光四次店铺数`
	,NVL(t3.exposed_4cnt_non_ordered_store_num_m7dtcd,0) AS `近七日曝光四次未下单店铺数`
	,NVL(t3.exposed_5cnt_store_num_m7dtcd,0) AS `近七日曝光五次店铺数`
	,NVL(t3.exposed_5cnt_non_ordered_store_num_m7dtcd,0) AS `近七日曝光五次未下单店铺数`
	,NVL(t3.exposed_6cnt_store_num_m7dtcd,0) AS `近七日曝光六次店铺数`
	,NVL(t3.exposed_6cnt_non_ordered_store_num_m7dtcd,0) AS `近七日曝光六次未下单店铺数`
	,NVL(t3.exposed_7cnt_store_num_m7dtcd,0) AS `近七日曝光七次店铺数`
	,NVL(t3.exposed_7cnt_non_ordered_store_num_m7dtcd,0) AS `近七日曝光七次未下单店铺数`

	,NVL(t3.after_sale_applied_store_num,0) AS `申请售后店铺数_提交售后`
	,NVL(t3.after_sale_applied2ordered_store_num,0) AS `申请售后7日复购店铺数`
	,NVL(t3.after_sale_applied2non_ordered_store_num,0) AS `申请售后7日流失店铺数`


    -- 门店商品标签
    ,NVL(t5.old_store_exposed2ordered_ratio_o30_sku_num,0) AS `老客转化率大于30的sku数`
    ,NVL(t5.loss_store_exposed2ordered_ratio_o30_sku_num,0) AS `流失客转化率大于7的sku数`
    ,NVL(t5.loss_price_store_exposed2ordered_ratio_o30_sku_num,0) AS `价格流失客转化率大于7的sku数`
    ,NVL(t5.new_store_exposed2ordered_ratio_o30_sku_num,0) AS `新客转化率大于7的sku数`

    -- 商品标签
	,NVL(t4.sku_num_sa_class,0) AS `特a级sku数量`
	,NVL(t4.sku_num_a_class,0) AS `a级sku数量`
	,NVL(t4.sku_num_b_class,0) AS `b级sku数量`
	,NVL(t4.sku_num_c_class,0) AS `c级sku数量`
	,NVL(t4.top1_cov_ratio_goods_class,0) AS `top1商家品类等级日活覆盖率等级`
	,NVL(t4.top1_cov_ratio,0) AS `top1商家品类等级日活覆盖率`
	,NVL(t4.total_top1_cov_ratio_goods_class,0) AS `整体top1商家品类等级日活覆盖率等级`
	,NVL(t4.total_top1_cov_ratio_mall,0) AS `整体top1商家品类等级日活覆盖率商城`
	,NVL(t4.total_top1_cov_ratio,0) AS `整体top1商家品类等级日活覆盖率`


    ,NVL(t3.old_store_exposed_store_num,0) AS `老客曝光店铺数`
    ,NVL(t3.loss_store_exposed_store_num,0) AS `流失客户曝光店铺数`
    ,NVL(t3.price_loss_store_exposed_sore_num,0) AS `老客价流失客户曝光店铺数`
    ,NVL(t3.new_store_ordered3cnt_exposed_sore_num,0) AS `下单三次新客户曝光店铺数`
    ,NVL(t3.new_store_ordered2cnt_exposed_sore_num,0) AS `下单两次新客户曝光店铺数`
    ,NVL(t3.new_store_ordered1cnt_exposed_store_num,0) AS `下单一次新客户曝光店铺数`
    ,NVL(t3.new_store_ordered0cnt_exposed_store_num,0) AS `下单零次新客户曝光店铺数`

    ,NVL(t7.ordered_store_num_m6dtcd,0) AS `商城近七日下单店铺数`
    ,NVL(t7.ordered_store_num_m29dtcd,0) AS `商城近三十日下单店铺数`
FROM base t1
LEFT JOIN sku_tags t2
	ON t2.dt = t1.dt
	AND t2.mall_id = t1.mall_id
	AND t2.merchant_id = t1.merchant_id
	AND t2.category_level1_name = t1.category_level1_name
	AND t2.category_level4_name = t1.category_level4_name
LEFT JOIN store_tags t3
	ON t3.dt = t1.dt
	AND t3.mall_id = t1.mall_id
	AND t3.merchant_id = t1.merchant_id
	AND t3.category_level1_name = t1.category_level1_name
	AND t3.category_level4_name = t1.category_level4_name
LEFT JOIN sku_class_tag t4
    ON t4.dt = t1.dt
    AND t4.mall_id = t1.mall_id
	AND t4.merchant_id = t1.merchant_id
	AND t4.category_level1_name = t1.category_level1_name
	AND t4.category_level4_name = t1.category_level4_name
LEFT JOIN ssp_tags t5
    ON t5.dt = t1.dt
    AND t5.mall_id = t1.mall_id
	AND t5.merchant_id = t1.merchant_id
	AND t5.category_level1_name = t1.category_level1_name
	AND t5.category_level4_name = t1.category_level4_name

LEFT JOIN (
    SELECT 
        t1.dt
        ,t1.mall_id
        ,t1.merchant_id
        ,t1.category_level1_name
        ,t1.category_level4_name
        ,SUM(t2.total_mct_market_expense_amt) AS total_mct_market_expense_amt -- `商家营销费用金额`
        
    FROM dim_sku t1
    JOIN fees t2
        ON t2.dt = t1.dt
        AND t2.sku_id = t1.sku_id
        AND t2.mall_id = t1.mall_id

    GROUP BY t1.dt
        ,t1.mall_id
        ,t1.merchant_id
        ,t1.category_level1_name
        ,t1.category_level4_name
) t6
	ON t6.dt = t1.dt
	AND t6.mall_id = t1.mall_id
	AND t6.merchant_id = t1.merchant_id
	AND t6.category_level1_name = t1.category_level1_name
	AND t6.category_level4_name = t1.category_level4_name

LEFT JOIN mall_stat t7
    ON t7.dt = t1.dt
    AND t7.mall_id = t1.mall_id
WHERE t1.mall_id = 871
    AND t1.category_level4_name IN (
            SELECT
                t1.category_level4_name
            FROM changsha_t_cate4_type_conf t1
        )
    AND t1.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
                    AND DATEADD(CURRENT_DATE(), -1, "dd")
;
"""

mct_mct_province_stat_sentence = """
WITH cat4_pcc_store AS(
    
    SELECT
        t1.dt
        ,t1.mall_id
        ,t1.province_id
        ,t1.province_name
        ,t1.city_id
        ,t1.city_name
        ,t1.county_id
        ,t1.county_name
        ,t1.category_level1_name
        ,t1.category_level4_name

        -- ,COUNT(DISTINCT IF(
        --     t1.is_validate>0, t1.customer_store_id, NULL
        -- )) AS validate_store_num
        ,COUNT(DISTINCT IF(
            t1.is_validate >0 AND t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS ordered_store_num
    FROM changsha_dws_store_tag_daily_asc t1
    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -7, "dd")
                        AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t1.mall_id = 871
        AND t1.dimension_type = "商城+门店+四级类目"
        AND t1.category_level4_name REGEXP "秋月梨|红香酥梨|皇冠梨|香梨|凯特芒|脆红李|进口龙眼|冬枣|青枣|25号小蜜瓜|麒麟西瓜|冰糖橙|赣南脐橙|金桔|金秋砂糖桔|蜜桔|小叶桔|葡萄柚|三红蜜柚|沙田柚|大菠萝|红肉菠萝蜜|国产红心火龙果|进口红心火龙果|香蕉|金枕榴莲|红心猕猴桃|黄心奇异果|巨峰葡萄|阳光玫瑰葡萄|软籽石榴|脆柿|大红提|无籽红提|千禧樱桃小番茄|蓝莓|黑皮甘蔗|水果黄瓜|雪莲果|红富士苹果|丑桔"
        AND t1.is_ordered > 0

    GROUP BY t1.dt
        ,t1.mall_id
        ,t1.province_id
        ,t1.province_name
        ,t1.city_id
        ,t1.city_name
        ,t1.county_id
        ,t1.county_name
        ,t1.category_level1_name
        ,t1.category_level4_name
)

-- 区县有效店铺数
,pcc_store AS(
    SELECT
        t1.dt
        ,record.mall_id
        ,t1.province_id
        ,t1.province_name
        ,t1.city_id
        ,t1.city_name
        ,t1.county_id
        ,t1.county_name
        ,COUNT(DISTINCT t1.customer_store_id) AS validdate_store_num
    FROM changsha_dim_store_daily_full t1
    LATERAL VIEW EXPLODE(t1.mall_id) record AS mall_id
    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -7, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND record.mall_id = 871
        AND t1.use_status NOT IN ("INVALID")
        AND t1.can_delivery_region > 0

    GROUP BY t1.dt
        ,record.mall_id
        ,t1.province_id
        ,t1.province_name
        ,t1.city_id
        ,t1.city_name
        ,t1.county_id
        ,t1.county_name
)


-- 商家类目区县统计
,mct_cat4_base AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.province_id -- `省id`
        ,t1.province_name -- `省名称`
        ,t1.city_id -- `市id`
        ,t1.city_name -- `市名称`
        ,t1.county_id -- `区县id`
        ,t1.county_name -- `区县名称`
        ,t1.customer_store_id -- `店铺id`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_name -- `四级类目名称`
        ,t1.merchant_id -- `商家id`
        ,t2.merchant_name -- `商家名称`
        
        ,t1.is_validate


        ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -31, 23), "1")>0, 1, 0) AS is_ordered_m30dtm23d -- `是否t-30到t-23下单`
        ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -8, 7), "1")>0, 1, 0) AS is_ordered_m7dtm1d -- `是否t-7到t-1下单`
        
        ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -7, 7), "1")>0, 1, 0) AS is_ordered_m6dtcd -- `是否近七日下单`
        ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -30, 30), "1")>0, 1, 0) AS is_ordered_m29dtcd -- `是否近三十日下单`
        ,REGEXP_COUNT(SUBSTR(t1.exposed_m79t0d, -7, 7), "1") AS exposed_m7dtcd_cnt -- `近七日曝光次数`
        ,IF(REGEXP_COUNT(SUBSTR(t1.ordered_cdta15d, 1, 7), "1")>0, 1, 0) AS is_ordered_cdta6d -- `是否后七日下单`
        ,REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -31, 30), "1") AS ordered_m30dtm1d -- `t-30到t-1下单次数`
        ,REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -31, 23), "1") AS ordered_m30dtm8d -- `t-30到t-8下单次数`
        ,REGEXP_COUNT(SUBSTR(t1.ordered_m79t0d, -8, 7), "1") AS ordered_m7dtm1d -- `t-7到t-1下单次数`
        ,t1.is_ordered -- `当日是否下单`
        ,t1.is_exposed -- `当日是否曝光`
        ,t1.ordered_goods_num
        ,t1.ordered_goods_amt
        ,NULL AS is_after_sale_applied
        ,t1.dimension_type
    FROM changsha_dws_store_tag_daily_asc t1
    LEFT JOIN datawarehouse_max.dim_merchant_daily_full t2
        ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -7, "dd")
                AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t2.dt = t1.dt
        AND t2.merchant_id = t1.merchant_id
        AND t2.mall_id = t1.mall_id
    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -7, "dd")
                AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t1.dimension_type ="商城+门店+四级类目+商家"
        AND t1.category_level4_name REGEXP "秋月梨|红香酥梨|皇冠梨|香梨|凯特芒|脆红李|进口龙眼|冬枣|青枣|25号小蜜瓜|麒麟西瓜|冰糖橙|赣南脐橙|金桔|金秋砂糖桔|蜜桔|小叶桔|葡萄柚|三红蜜柚|沙田柚|大菠萝|红肉菠萝蜜|国产红心火龙果|进口红心火龙果|香蕉|金枕榴莲|红心猕猴桃|黄心奇异果|巨峰葡萄|阳光玫瑰葡萄|软籽石榴|脆柿|大红提|无籽红提|千禧樱桃小番茄|蓝莓|黑皮甘蔗|水果黄瓜|雪莲果|红富士苹果|丑桔"
        AND t1.is_validate > 0
        AND t1.mall_id = 871
)



,result AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.province_id -- `省id`
        ,t1.province_name -- `省名称`
        ,t1.city_id -- `市id`
        ,t1.city_name -- `市名称`
        ,t1.county_id -- `区县id`
        ,t1.county_name -- `区县名称`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_name -- `四级类目名称`
        ,t1.merchant_id -- `商家id`
        ,t1.merchant_name -- `商家名称`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)>4 AND NVL(t1.ordered_m7dtm1d, 0) >=1 AND t1.is_validate>0, 
                t1.customer_store_id, NULL
        )) AS store_pool_old -- `老客户池`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)>4 AND NVL(t1.ordered_m7dtm1d, 0) >=1 AND t1.is_validate>0
                AND t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS ordered_store_num_old -- `老客户下单店铺数`

        ,COUNT(DISTINCT IF(
                NVL(t1.ordered_m30dtm8d,0)>=4 AND NVL(t1.ordered_m7dtm1d, 0) =0 AND t1.is_validate>0, 
                    t1.customer_store_id, NULL
        )) AS store_pool_loss -- `流失客户池`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm8d,0)>=4 AND NVL(t1.ordered_m7dtm1d, 0) =0 AND t1.is_validate>0
                AND t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS ordered_store_num_loss -- `流失客户下单店铺数`

        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm8d,0)=3 AND NVL(t1.ordered_m7dtm1d, 0)=0 AND t1.is_validate>0,
                t1.customer_store_id, NULL
        )) AS price_loss_store_pool -- `老客价流失客户池`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm8d,0)=3 AND NVL(t1.ordered_m7dtm1d, 0)=0 AND t1.is_validate>0
                AND t1.is_ordered>0 , t1.customer_store_id, NULL
        )) AS ordered_store_num_price_loss -- `老客价流失下单店铺数`

        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm8d,0)=2 AND NVL(t1.ordered_m7dtm1d, 0)=1 AND t1.is_validate>0,
                t1.customer_store_id, NULL
        )) AS new_store_ordered3cnt_pool -- `下单三次新客户池`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm8d,0)=2 AND NVL(t1.ordered_m7dtm1d, 0)=1 AND t1.is_validate>0
                AND t1.is_ordered >0, t1.customer_store_id, NULL
        )) AS ordered_store_num_new_ordered3cnt -- `下单三次新客下单店铺数`

        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)=2 AND t1.is_validate>0, t1.customer_store_id, NULL
        )) AS new_store_ordered2cnt_pool -- `下单两次新客户池`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)=2 AND t1.is_validate>0 AND t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS ordered_store_num_new_ordered2cnt -- `下单两次新客下单店铺数`


        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)=1 AND t1.is_validate>0, t1.customer_store_id, NULL
        )) AS new_store_ordered1cnt_pool -- `下单一次新客户池`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)=1 AND t1.is_validate>0 AND t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS ordered_store_num_new_ordered1cnt -- `下单一次新客下单店铺数`


        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)=0 AND t1.is_validate>0, t1.customer_store_id, NULL
        )) AS new_store_ordered0cnt_pool -- `下单零次新客户池`
        ,COUNT(DISTINCT IF(
            NVL(t1.ordered_m30dtm1d,0)=0 AND t1.is_validate>0 AND t1.is_ordered>0, t1.customer_store_id, NULL
        )) AS ordered_store_num_new_ordered0cnt -- `下单零次新客下单店铺数`
        ,COUNT(DISTINCT IF(t1.is_validate>0 AND t1.is_ordered>0, t1.customer_store_id, NULL)) AS ordered_store_num -- `下单店铺数`
    FROM mct_cat4_base t1

    GROUP BY t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.province_id -- `省id`
        ,t1.province_name -- `省名称`
        ,t1.city_id -- `市id`
        ,t1.city_name -- `市名称`
        ,t1.county_id -- `区县id`
        ,t1.county_name -- `区县名称`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_name -- `四级类目名称`
        ,t1.merchant_id -- `商家id`
        ,t1.merchant_name -- `商家名称`
)



,report AS(
    SELECT
        t1.dt AS `日期`
        ,t1.mall_id AS `商城id`
        ,t1.province_id AS `省id`
        ,t1.province_name AS `省名称`
        ,t1.city_id AS `市id`
        ,t1.city_name AS `市名称`
        ,t1.county_id AS `区县id`
        ,t1.county_name AS `区县名称`
        ,t1.category_level1_name AS `一级类目名称`
        ,t1.category_level4_name AS `四级类目名称`
        ,t1.merchant_id AS `商家id`
        ,t1.merchant_name AS `商家名称`
        ,t1.store_pool_old AS `老客户池`
        ,t1.ordered_store_num_old AS `老客户下单店铺数`

        ,t1.store_pool_loss AS `流失客户池`
        ,t1.ordered_store_num_loss AS `流失客户下单店铺数`

        ,t1.price_loss_store_pool AS `老客价流失客户池`
        ,t1.ordered_store_num_price_loss AS `老客价流失下单店铺数`

        ,t1.new_store_ordered3cnt_pool AS `下单三次新客户池`
        ,t1.ordered_store_num_new_ordered3cnt AS `下单三次新客下单店铺数`

        ,t1.new_store_ordered2cnt_pool AS `下单两次新客户池`
        ,t1.ordered_store_num_new_ordered2cnt AS `下单两次新客下单店铺数`

        ,t1.new_store_ordered1cnt_pool AS `下单一次新客户池`
        ,t1.ordered_store_num_new_ordered1cnt AS `下单一次新客下单店铺数`
        ,t1.new_store_ordered0cnt_pool AS `下单零次新客户池`
        ,t1.ordered_store_num_new_ordered0cnt AS `下单零次新客下单店铺数`
        
        ,t1.ordered_store_num AS `下单店铺数`
        ,NVL(t3.ordered_store_num,0) AS `区县品类下单店铺数`
        ,NVL(t2.validdate_store_num,0) AS `区县有效店铺数`
        ,NVL(t1.new_store_ordered3cnt_pool, 0) + NVL(t1.new_store_ordered2cnt_pool,0) + 
            NVL(t1.new_store_ordered1cnt_pool,0) + NVL(t1.new_store_ordered0cnt_pool, 0) AS `新客户池`
        ,NVL(t1.ordered_store_num_new_ordered3cnt, 0) + NVL(t1.ordered_store_num_new_ordered2cnt,0) + 
            NVL(t1.ordered_store_num_new_ordered1cnt,0) + NVL(t1.ordered_store_num_new_ordered0cnt, 0) AS `新客户下单店铺数`
    FROM  result t1
    LEFT JOIN pcc_store t2
        ON t2.dt = t1.dt
        AND t2.mall_id = t1.mall_id
        AND t2.province_id = t1.province_id
        AND t2.city_id = t1.city_id
        AND t2.county_id = t1.county_id
    LEFT JOIN cat4_pcc_store t3
        ON t3.dt = t1.dt
        AND t3.mall_id = t1.mall_id
        AND t3.province_id = t1.province_id
        AND t3.city_id = t1.city_id
        AND t3.county_id = t1.county_id
        AND t3.category_level1_name = t1.category_level1_name
        AND t3.category_level4_name = t1.category_level4_name

    WHERE NVL(t2.validdate_store_num,0) + NVL(t1.ordered_store_num,0) +
        NVL(t1.store_pool_old,0) + NVL(t1.store_pool_loss, 0)+
        NVL(t1.price_loss_store_pool,0) + NVL(t1.new_store_ordered3cnt_pool,0)+
        NVL(t1.new_store_ordered2cnt_pool,0) + NVL(t1.new_store_ordered1cnt_pool,0) > 0

        AND t3.ordered_store_num > 0
        AND t1.ordered_store_num > 0
        AND t1.dt BETWEEN DATEADD(CURRENT_DATE(), -14, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
)


SELECT
    t1.*
FROM report t1
WHERE t1.`四级类目名称` IN (
                "秋月梨", "红香酥梨", "香梨", "凯特芒", "脆红李", "进口龙眼", "冬枣", 
                "青枣", "25号小蜜瓜", "麒麟西瓜", "赣南脐橙", "金桔", "金秋砂糖桔", 
                "蜜桔", "沙田柚", "大菠萝", "红肉菠萝蜜", "国产红心火龙果"
                ,"进口红心火龙果", "香蕉", "金枕榴莲"
                -- , "巨峰葡萄", "阳光玫瑰葡萄"
                --, "软籽石榴", "脆柿", "大红提", "无籽红提", "千禧樱桃小番茄", "蓝莓", "黑皮甘蔗", "水果黄瓜", "雪莲果", "红富士苹果", "丑桔"
                -- , "三红蜜柚", "葡萄柚", "黄心奇异果", "皇冠梨", "红心猕猴桃", "冰糖橙", "小叶桔"
        )
    
;
"""

mct_sku_stat_sentence = """
WITH mall_stat AS(

    SELECT /*+ MAPJOIN(t0) */
        t0.dates AS dt
        ,t1.mall_id
        
        ,COLLECT_LIST(t1.ordered_store_num)WITHIN GROUP (ORDER BY t1.dt) AS ordered_store_num_m2dtcd
        ,COLLECT_LIST(DATEDIFF(t0.dates, DATE(t1.dt), "dd"))WITHIN GROUP (ORDER BY t1.dt) AS gaps
        ,MAP_FROM_ARRAYS(
            COLLECT_LIST(DATEDIFF(t0.dates, DATE(t1.dt), "dd"))WITHIN GROUP (ORDER BY t1.dt),
            COLLECT_LIST(t1.ordered_store_num)WITHIN GROUP (ORDER BY t1.dt)
        ) AS ordered_store_num_m2dtcd_map
    FROM changsha_trade_fact_daily_asc t1
    JOIN datawarehouse_max.dim_dates_attr t0
        ON t0.dates BETWEEN DATEADD(CURRENT_DATE(), -25, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t1.dt BETWEEN DATEADD(DATE(t0.dates), -2, "dd")
            AND DATEADD(DATE(t0.dates), 0, "dd")
    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -25-2, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t1.dimention_type = "商城"

    GROUP BY t0.dates
        ,t1.mall_id
)

-- 1. 基础信息： 商城维度信息 和门店商品标签、前台类目信息
,mall AS(
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


,base AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t2.mall -- `商城`
        ,t1.sku_id -- `商品id`
        ,t2.sku_name -- `商品名称`
        ,t2.merchant_id -- `商家id`
        ,t2.merchant_name -- `商家名称`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_name -- `四级类目名称`
        ,t2.settlement_type -- `结算类型`
        ,t2.settlement_type_desc
        ,t2.gross_weight -- `毛重`

        ,REGEXP_COUNT(SUBSTR(t1.upshelve_m25t0d, -3, 3), "1") AS onshelve_cnt_m2tcd -- `最近三日上架次数`
        
        ,ARRAY_REDUCE(
            SLICE(SPLIT(t1.ordered_goods_num_m25dtcd, ","), -3, 3), 0L, 
                (buff, item) -> buff+CAST(item AS BIGINT), buff -> buff
        ) / ARRAY_REDUCE(
            SLICE(SPLIT(t1.ordered_goods_amt_m25dtcd, ","), -3, 3), 0.0, 
                (buff, item) -> IF(BIGINT(item)>0, buff+1, buff), buff -> buff
        ) AS ordered_goods_num_m2dtcd -- `近三日日均下单件数`

        ,ARRAY_REDUCE(
            SLICE(SPLIT(t1.ordered_goods_amt_m25dtcd, ","), -3, 3), 0.0, 
                (buff, item) -> buff+CAST(item AS DECIMAL(10,2)), buff -> buff
        ) / ARRAY_REDUCE(
            SLICE(SPLIT(t1.ordered_goods_amt_m25dtcd, ","), -3, 3), 0.0, 
                (buff, item) -> IF(BIGINT(item)>0, buff+1, buff), buff -> buff
        ) AS ordered_goods_amt_m2dtcd -- `近三日日均下单金额`
        ,ARRAY_REDUCE(
            SLICE(SPLIT(t1.delivered_goods_amt_m25dtcd, ","), -3, 3), 0.0, 
                (buff, item) -> buff+CAST(item AS DECIMAL(10,2)), buff -> buff
        ) / ARRAY_REDUCE(
            SLICE(SPLIT(t1.ordered_goods_amt_m25dtcd, ","), -3, 3), 0.0, 
                (buff, item) -> IF(BIGINT(item)>0, buff+1, buff), buff -> buff
        ) AS delivered_goods_amt_m2dtcd -- `近三日日均送货金额`
        ,ARRAY_REDUCE(
            SLICE(SPLIT(t1.mct_supplied_price_base_m25dtcd, ","), -3, 3), 0.0, 
                (buff, item) -> IF(CAST(item AS DECIMAL(10,2))>0, buff+CAST(item AS DECIMAL(10,2)), buff), buff -> buff
        ) / ARRAY_REDUCE(
            SLICE(SPLIT(t1.mct_supplied_price_base_m25dtcd, ","), -3, 3), 0.0, 
                (buff, item) -> IF(CAST(item AS DECIMAL(10,2))>0, buff+1, buff), buff -> buff
        ) AS mct_supplied_price_base_m2dtcd -- `近三日日均供货价格`

        ,ARRAY_REDUCE(
            SLICE(SPLIT(t1.ordered_store_num_m25dtcd, ","), -3, 3), 0L, 
                (buff, item) -> buff+CAST(item AS BIGINT), buff -> buff
        ) / ARRAY_REDUCE(
            MAP_KEYS(
                MAP_FILTER(
                    MAP_FROM_ARRAYS(
                        t3.gaps, TRANSFORM(
                            SLICE(SPLIT(t1.ordered_goods_amt_m25dtcd, ","), -3, 3), item -> BIGINT(item)
                        )
                    ), (k, v) -> v > 0
                )
            ), 0L, (buff, item) -> t3.ordered_store_num_m2dtcd_map[item] + buff, buff -> buff
        ) AS ordered_store_num_m2dtcd_ratio -- `近三日日均日活覆盖率`
        
    FROM changsha_dws_sku_operate_tag_daily_asc  t1
    LEFT JOIN changsha_dim_sku_daily_asc t2
        ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -25, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t2.dt = t1.dt
        AND t2.mall_id = t1.mall_id
        AND t2.sku_id = t1.sku_id
    LEFT JOIN mall_stat t3
        ON t3.dt = t1.dt
        AND t3.mall_id = t1.mall_id

    JOIN mall t0
        ON t0.mall_id = t1.mall_id
    WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -25, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t1.category_level4_name IN (
            SELECT
                t1.category_level4_name
            FROM changsha_t_cate4_type_conf t1
        )
        AND INSTR(t1.category_level1_name, "水果") >0
        AND t1.dimension_type = "商城+SKU"
        AND t2.is_sku_valid = 1
)


,result AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.mall -- `商城`
        ,t1.sku_id -- `商品id`
        ,t1.sku_name -- `商品名称`
        ,t1.merchant_id -- `商家id`
        ,t1.merchant_name -- `商家名称`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_name -- `四级类目名称`
        ,t1.settlement_type_desc AS settlement_type -- `结算类型`
        ,t1.gross_weight -- `毛重`
        ,t1.onshelve_cnt_m2tcd -- `近三日上架次数`
        ,CAST(t1.mct_supplied_price_base_m2dtcd AS DECIMAL(10,2)) AS mct_supplied_price_base_m2dtcd -- `近三日日均供货价格`
        ,DECODE(
            t1.settlement_type
            ,"CATTY", CAST(t1.mct_supplied_price_base_m2dtcd AS DECIMAL(10,2))
            ,"STANDER", CAST(t1.mct_supplied_price_base_m2dtcd / t1.gross_weight AS DECIMAL(10,2))
            -- ,"STANDER", CAST(t1.mct_supplied_price_base_m2dtcd / t1.gross_weight AS DECIMAL(10,2))
        ) AS unit_price -- `近三日日均供货斤单价`
        ,ROUND(t1.ordered_goods_num_m2dtcd, 1) AS ordered_goods_num_m2dtcd -- `近三日日均下单件数`
        ,ROUND(t1.ordered_goods_amt_m2dtcd, 2) AS ordered_goods_amt_m2dtcd -- `近三日日均下单金额`
        ,ROUND(t1.delivered_goods_amt_m2dtcd, 2) AS delivered_goods_amt_m2dtcd -- `近三日日均送货金额`
        
        ,ROUND(t1.ordered_store_num_m2dtcd_ratio, 4) AS ordered_store_num_m2dtcd_ratio  -- `近三日日均日活覆盖率`
        -- ,MAX(ROUND(t1.ordered_store_num_m2dtcd_ratio, 4)) OVER(
        --     PARTITION BY t1.dt, t1.category_level1_name, t1.category_level4_name
        -- ) AS top1_ordered_store_num_m2dtcd_ratio -- `近三日日均日活覆盖率top1`
        -- ,FIRST_VALUE(IF(t1.ordered_store_num_m2dtcd_ratio>0, t1.mall, NULL )) OVER(
        --     PARTITION BY t1.dt, t1.category_level1_name, t1.category_level4_name
        --         ORDER BY ROUND(t1.ordered_store_num_m2dtcd_ratio, 4) DESC
        -- ) AS top1_ordered_store_num_m2dtcd_ratio_mall -- `近三日日均日活覆盖率top1商城`
        -- ,FIRST_VALUE(IF(t1.ordered_store_num_m2dtcd_ratio>0, t1.sku_name, NULL )) OVER(
        --     PARTITION BY t1.dt, t1.category_level1_name, t1.category_level4_name
        --         ORDER BY ROUND(t1.ordered_store_num_m2dtcd_ratio, 4) DESC
        -- ) AS top1_ordered_store_num_m2dtcd_ratio_sku_name -- `近三日日均日活覆盖率top1商品名称`
        ,ROW_NUMBER() OVER(
            PARTITION BY t1.dt, t1.category_level1_name, t1.category_level4_name ORDER BY ROUND(t1.ordered_store_num_m2dtcd_ratio, 4) DESC
        ) AS ordered_store_num_m2dtcd_ratio_rnk -- `近三日日均日活覆盖率排名`

    FROM base t1
    WHERE t1.onshelve_cnt_m2tcd > 0

)


,report AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.mall -- `商城`
        ,t1.sku_id -- `商品id`
        ,t1.sku_name -- `商品名称`
        ,t1.merchant_id -- `商家id`
        ,t1.merchant_name -- `商家名称`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_name -- `四级类目名称`
        ,t1.settlement_type -- `结算类型`
        ,t1.gross_weight -- `毛重`
        ,t1.onshelve_cnt_m2tcd -- `近三日上架次数`
        ,t1.mct_supplied_price_base_m2dtcd -- `近三日日均供货价格`
        ,t1.unit_price -- `近三日日均供货斤单价`
        ,NVL(t1.ordered_goods_num_m2dtcd, 0) AS ordered_goods_num_m2dtcd  -- `近三日日均下单件数`
        ,NVL(t1.ordered_goods_amt_m2dtcd, 0) AS ordered_goods_amt_m2dtcd  -- `近三日日均下单金额`
        ,NVL(t1.delivered_goods_amt_m2dtcd, 0) AS delivered_goods_amt_m2dtcd  -- `近三日日均送货金额`
        
        ,NVL(t1.ordered_store_num_m2dtcd_ratio, 0) AS ordered_store_num_m2dtcd_ratio   -- `近三日日均日活覆盖率`
        ,"非排名" AS dimension_type -- `数据类型`
        -- ,t1.top1_ordered_store_num_m2dtcd_ratio AS `近三日日均日活覆盖率top1`
        -- ,t1.top1_ordered_store_num_m2dtcd_ratio_mall AS `近三日日均日活覆盖率top1商城`
        -- ,t1.top1_ordered_store_num_m2dtcd_ratio_sku_name AS `近三日日均日活覆盖率top1商品名称`

        -- COUNT(1)
    FROM result t1
    WHERE t1.mall IN ("标果南昌", "标果长沙")

    UNION ALL

    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.mall -- `商城`
        ,t1.sku_id -- `商品id`
        ,t1.sku_name -- `商品名称`
        ,t1.merchant_id -- `商家id`
        ,t1.merchant_name -- `商家名称`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_name -- `四级类目名称`
        ,t1.settlement_type -- `结算类型`
        ,t1.gross_weight -- `毛重`
        ,t1.onshelve_cnt_m2tcd -- `近三日上架次数`
        ,t1.mct_supplied_price_base_m2dtcd -- `近三日日均供货价格`
        ,t1.unit_price -- `近三日日均供货斤单价`
        ,NVL(t1.ordered_goods_num_m2dtcd, 0) AS ordered_goods_num_m2dtcd  -- `近三日日均下单件数`
        ,NVL(t1.ordered_goods_amt_m2dtcd, 0) AS ordered_goods_amt_m2dtcd  -- `近三日日均下单金额`
        ,NVL(t1.delivered_goods_amt_m2dtcd, 0) AS delivered_goods_amt_m2dtcd  -- `近三日日均送货金额`
        
        ,NVL(t1.ordered_store_num_m2dtcd_ratio, 0) AS ordered_store_num_m2dtcd_ratio   -- `近三日日均日活覆盖率`
        ,"排名" AS dimension_type -- `数据类型`
        -- ,t1.top1_ordered_store_num_m2dtcd_ratio AS `近三日日均日活覆盖率top1`
        -- ,t1.top1_ordered_store_num_m2dtcd_ratio_mall AS `近三日日均日活覆盖率top1商城`
        -- ,t1.top1_ordered_store_num_m2dtcd_ratio_sku_name AS `近三日日均日活覆盖率top1商品名称`

        -- COUNT(1)
    FROM result t1
    WHERE t1.ordered_store_num_m2dtcd_ratio_rnk = 1
)



SELECT
	t1.dt AS `日期`
	,t1.mall_id AS `商城id`
	,t1.mall AS `商城`
	,t1.sku_id AS `商品id`
	,t1.sku_name AS `商品名称`
	,t1.merchant_id AS `商家id`
	,t1.merchant_name AS `商家名称`
	,t1.category_level1_name AS `一级类目名称`
	,t1.category_level4_name AS `四级类目名称`
	,t1.settlement_type AS `结算类型`
	,t1.gross_weight AS `毛重`
    ,t1.onshelve_cnt_m2tcd AS `近三日上架次数`
	,t1.mct_supplied_price_base_m2dtcd AS `近三日日均供货价格`
	,t1.unit_price AS `近三日日均供货斤单价`
	,NVL(t1.ordered_goods_num_m2dtcd, 0)  AS `近三日日均下单件数`
	,NVL(t1.ordered_goods_amt_m2dtcd, 0)  AS `近三日日均下单金额`
	,NVL(t1.delivered_goods_amt_m2dtcd, 0)  AS `近三日日均送货金额`
	
	,NVL(t1.ordered_store_num_m2dtcd_ratio, 0)   AS `近三日日均日活覆盖率`
    ,t1.dimension_type AS `数据类型`
FROM report t1
WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -3, "dd")
                AND DATEADD(CURRENT_DATE(), -1, "dd")

    AND t1.category_level4_name IN (
            SELECT
                t1.category_level4_name
            FROM changsha_t_cate4_type_conf t1
        )
-- LIMIT 2000
;
"""


mct_roi_report_sentence = """
WITH base AS(

    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.mall -- `商城`
        ,t1.customer_store_id
        ,t1.ssp_activity_tag
        ,t1.sku_id
        ,t1.sku_name
        ,t1.merchant_id
        ,t1.merchant_name
        ,t1.category_level1_id -- `一级类目id`
        ,t1.category_level1_name -- `一级类目名称`
        ,t1.category_level4_id -- `四级类目id`
        ,t1.category_level4_name -- `四级类目名称`
        
        ,MAX(t1.ordered_goods_num) AS ordered_goods_num -- `下单商品数量`
        ,MAX(t1.delivered_goods_num) AS delivered_goods_num -- `送货商品数量`
        ,MAX(t1.delivered_goods_amt) AS delivered_goods_amt -- `送货金额`
        ,MAX(t1.after_sale_applied_num) AS after_sale_applied_num -- `售后申请数量`
        ,MAX(t1.after_sale_refund_amt) AS after_sale_refund_amt -- `售后赔付金额`

        ,MAX(t1.exposed_cnt) AS exposed_cnt -- `曝光次数`
        ,MAX(t1.clicked_cnt) AS clicked_cnt -- `点击次数`
        ,MAX(t1.added_cnt) AS added_cnt -- `加购次数`
    FROM(
        SELECT
            t1.dt -- `日期`
            ,t1.mall_id -- `商城id`
            ,t1.mall -- `商城`
            ,t1.customer_store_id
            ,t1.ssp_activity_tag
            ,t1.sku_id
            ,t1.sku_name
            ,t1.merchant_id
            ,t1.merchant_name
            ,t1.category_level1_id -- `一级类目id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`


            ,t1.ordered_goods_num -- `下单商品数量`
            ,t1.delivered_goods_num -- `送货商品数量`
            ,t1.delivered_goods_amt -- `送货金额`
            ,t1.after_sale_applied_num -- `售后申请数量`
            ,t1.after_sale_refund_amt -- `售后赔付金额`

            ,NULL AS exposed_cnt -- `曝光次数`
            ,NULL AS clicked_cnt -- `点击次数`
            ,NULL AS added_cnt -- `加购次数`
        FROM changsha_trade_fact_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -15, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
            AND t1.mall_id = 871
            AND t1.dimention_type = "商城+门店+SKU"


        UNION 
        SELECT
            t1.dt -- `日期`
            ,t1.mall_id -- `商城id`
            ,t1.mall -- `商城`
            ,t1.customer_store_id
            ,t1.ssp_activity_tag
            ,t1.sku_id
            ,t1.sku_name
            ,t1.merchant_id
            ,t1.merchant_name
            ,t1.category_level1_id -- `一级类目id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`

            ,NULL AS ordered_goods_num -- `下单商品数量`
            ,NULL AS delivered_goods_num -- `送货商品数量`
            ,NULL AS delivered_goods_amt -- `送货金额`
            ,NULL AS after_sale_applied_num -- `售后申请数量`
            ,NULL AS after_sale_refund_amt -- `售后赔付金额`

            ,t1.exposed_cnt -- `曝光次数`
            ,t1.clicked_cnt -- `点击次数`
            ,t1.added_cnt -- `加购次数`
        FROM changsha_flow_fact_daily_asc t1
        WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -15, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
            AND t1.mall_id = 871
            AND t1.dimention_type = "商城+门店+SKU"

    ) t1
    GROUP BY t1.dt -- `日期`
            ,t1.mall_id -- `商城id`
            ,t1.mall -- `商城`
            ,t1.customer_store_id
            ,t1.ssp_activity_tag
            ,t1.sku_id
            ,t1.sku_name
            ,t1.merchant_id
            ,t1.merchant_name
            ,t1.category_level1_id -- `一级类目id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`

)


,stats AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.mall -- `商城`


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
            GROUPING(t1.merchant_id)==0, t1.merchant_id, -99999L
        ) AS merchant_id -- '商家ID' BIGINT
        ,IF(
            GROUPING(t1.merchant_name)==0, t1.merchant_name, "合计"
        ) AS merchant_name -- '商家名称' STRING

        ,MAX(NVL(t2.is_up_today,0)) AS is_up_today -- `当日是否上架`
        ,MAX(t2.mct_supplied_price_base) AS mct_supplied_price_base -- `商家基准供货价`
        ,NVL(
            MAX(t2.mct_supplied_price_new), MAX(t2.mct_supplied_price_base)
        ) AS mct_supplied_price_new -- `商家新客供货价`
        ,MAX(t3.old_pond_num) AS os_pool_num -- `老客户客户池`
        ,MAX(t3.new_pond_num) AS ns_pool_num -- `新客户客户池`
        ,SUM(t1.ordered_goods_num) AS ordered_goods_num -- `下单商品数量`
        ,SUM(t1.delivered_goods_num) AS delivered_goods_num -- `送货商品数量`
        
        ,SUM(t1.after_sale_applied_num) AS after_sale_applied_num -- `售后申请数量`
        ,SUM(t1.after_sale_refund_amt) AS after_sale_refund_amt -- `售后赔付金额`
        ,CAST(
            SUM(t1.after_sale_applied_num) / SUM(t1.delivered_goods_num)
                AS DECIMAL(10,4)
        ) AS after_sale_applied_ratio -- `售后提交率`
        ,CAST(
            SUM(t1.after_sale_refund_amt) / SUM(t1.delivered_goods_amt)
                AS DECIMAL(10,4)
        ) AS after_sale_refund_ratio -- `售后赔付率`
        ,COUNT(DISTINCT IF(t1.exposed_cnt>0, t1.customer_store_id, NULL)) AS exposed_store_num -- `曝光店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_cnt>0 AND t1.ssp_activity_tag="NEW", t1.customer_store_id, NULL
        )) AS exposed_new_store_num -- `新客户曝光店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_cnt>0 AND t1.ssp_activity_tag="OLD", t1.customer_store_id, NULL
        )) AS exposed_old_store_num -- `老客户曝光店铺数`
        ,COUNT(DISTINCT IF(
            t1.ordered_goods_num>0, t1.customer_store_id, NULL
        )) AS ordered_store_num -- `下单店铺数`
        ,COUNT(DISTINCT IF(
            t1.ordered_goods_num>0 AND t1.ssp_activity_tag="NEW", t1.customer_store_id, NULL
        )) AS ordered_new_store_num -- `新客户下单店铺数`
        ,COUNT(DISTINCT IF(
            t1.ordered_goods_num>0 AND t1.ssp_activity_tag="OLD", t1.customer_store_id, NULL
        )) AS ordered_old_store_num -- `老客户下单店铺数`
        ,SUM(IF(
            t1.ssp_activity_tag="NEW", t1.ordered_goods_num, NULL 
        )) AS ordered_ns_goods_num -- `新客户下单商品数量`
        ,SUM(IF(
            t1.ssp_activity_tag="OLD", t1.ordered_goods_num, NULL 
        )) AS ordered_os_goods_num -- `老客户下单商品数量`
        ,SUM(IF(
            t1.ssp_activity_tag="LOSS", t1.ordered_goods_num, NULL 
        )) AS ordered_ls_goods_num -- `召回客户下单商品数量`

        ,SUM(t1.delivered_goods_amt) AS delivered_goods_amt -- `送货商品金额`
        ,SUM(IF(
            t1.ssp_activity_tag="NEW", t1.delivered_goods_amt, NULL 
        )) AS delivered_ns_goods_amt -- `新客户送货商品金额`
        ,SUM(IF(
            t1.ssp_activity_tag="OLD", t1.delivered_goods_amt, NULL 
        )) AS delivered_os_goods_amt -- `老客户送货商品金额`
        ,SUM(IF(
            t1.ssp_activity_tag="LOSS", t1.delivered_goods_amt, NULL 
        )) AS delivered_ls_goods_amt -- `召回客户送货商品金额`

        -- ,SUM(t1.exposed_cnt) AS exposed_cnt -- `曝光次数`
        -- ,SUM(t1.clicked_cnt) AS clicked_cnt -- `点击次数`
        -- ,SUM(t1.added_cnt) AS added_cnt -- `加购次数`
    FROM base t1
    LEFT JOIN changsha_dim_sku_daily_asc t2
        ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -15, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t2.dt = t1.dt
        AND t2.mall_id = t1.mall_id
        AND t2.sku_id = t1.sku_id
    
    LEFT JOIN datawarehouse_max.ads_changsha_sku_dashboard_daily_asc t3
        ON t3.dt BETWEEN DATEADD(CURRENT_DATE(), -15, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t3.dt = t1.dt
        AND t1.mall_id = 871
        AND t3.goods_id = t1.sku_id

    GROUP BY t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.mall -- `商城`
    ,GROUPING SETS (
        -- 全商城
        ()
        -- 商品
        ,(
            t1.sku_id -- `商品id`
            ,t1.sku_name -- `商品名称`
            ,t1.merchant_id -- `商家id`
            ,t1.merchant_name -- `商家名称`
            ,t1.category_level1_id -- `一级类目id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`
        )
        -- 商家四级类目
        ,(
            t1.merchant_id -- `商家id`
            ,t1.merchant_name -- `商家名称`
            ,t1.category_level1_id -- `一级类目id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`
        )
        -- 四级类目
        ,(
            t1.category_level1_id -- `一级类目id`
            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`
        )
    )
        
)


,result AS(

    SELECT 
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.mall -- `商城`


        ,t1.sku_id -- '商品ID' BIGINT
        ,t1.sku_name -- '商品名称' STRING
        ,t1.category_level4_id -- '四级类目ID' BIGINT
        ,t1.category_level4_name -- '四级类目名称' STRING
        ,t1.category_level1_id -- '一级类目ID' BIGINT
        ,t1.category_level1_name -- '一级类目名称' STRING

        ,t1.merchant_id -- '商家ID' BIGINT
        ,t1.merchant_name -- '商家名称' STRING

        ,IF(t1.sku_name !="合计", t1.is_up_today, NULL) AS is_up_today -- `当日是否上架`
        ,IF(t1.sku_name !="合计", t1.mct_supplied_price_base, NULL) AS mct_supplied_price_base -- `商家基准供货价`
        ,IF(t1.sku_name !="合计", t1.mct_supplied_price_new, NULL) AS mct_supplied_price_new -- `商家新客供货价`
        ,IF(t1.sku_name !="合计", t1.os_pool_num, NULL) AS os_pool_num -- `老客户客户池`
        ,IF(t1.sku_name !="合计", t1.ns_pool_num, NULL) AS ns_pool_num -- `新客户客户池`
        ,t1.ordered_goods_num -- `下单商品数量`
        ,t1.delivered_goods_num -- `送货商品数量`
        
        ,t1.after_sale_applied_num -- `售后申请数量`
        ,t1.after_sale_refund_amt -- `售后赔付金额`
        ,IF(t1.delivered_goods_num >= 0, NVL(t1.after_sale_applied_ratio,0), NULL) AS after_sale_applied_ratio -- `售后提交率`
        ,IF(t1.delivered_goods_num >= 0, NVL(t1.after_sale_refund_ratio,0), NULL) AS after_sale_refund_ratio -- `售后赔付率`
        ,t1.exposed_store_num -- `曝光店铺数`
        ,t1.exposed_new_store_num -- `新客户曝光店铺数`
        ,t1.exposed_old_store_num -- `老客户曝光店铺数`
        ,t1.ordered_store_num -- `下单店铺数`
        ,t1.ordered_new_store_num -- `新客户下单店铺数`
        ,t1.ordered_old_store_num -- `老客户下单店铺数`
        ,CAST(
            t1.ordered_old_store_num / t1.exposed_old_store_num AS DECIMAL(10,4)
        ) AS exposed2ordered_old_store_ratio -- `老客户曝光转化率`

        ,CAST(
            t1.ordered_new_store_num / t1.exposed_new_store_num AS DECIMAL(10,4)
        ) AS exposed2ordered_new_store_ratio -- `新客户曝光转化率`
        ,t1.ordered_ns_goods_num -- `新客户下单商品数量`
        ,t1.ordered_os_goods_num -- `老客户下单商品数量`
        ,t1.ordered_ls_goods_num -- `召回客户下单商品数量`

        ,t1.delivered_goods_amt -- `送货商品金额`
        ,t1.delivered_ns_goods_amt -- `新客户送货商品金额`
        ,t1.delivered_os_goods_amt -- `老客户送货商品金额`
        ,t1.delivered_ls_goods_amt -- `召回客户送货商品金额`

        ,t2.target_goods_num -- `四级类目目标件数`
        ,CAST(
            IF(t2.target_goods_num>0, t1.ordered_goods_num / t2.target_goods_num, NULL) AS DECIMAL(10,4)
        ) AS achieved_target_goods_ratio -- `四级类目目标件数达成率`
        ,CASE
            WHEN
                ALL_MATCH(
                    ARRAY(t1.sku_name, t1.category_level4_name, t1.category_level1_name, t1.merchant_name), item -> item ="合计"
                )
            THEN "商城"
            WHEN
                ALL_MATCH(
                    ARRAY(t1.sku_name, t1.category_level4_name, t1.category_level1_name, t1.merchant_name), item -> item !="合计"
                )
            THEN "商品"
            WHEN
                ALL_MATCH(
                    ARRAY( t1.category_level4_name, t1.category_level1_name, t1.merchant_name), item -> item !="合计"
                ) AND t1.sku_name = "合计"
            THEN "商家四级类目"
            WHEN
                ALL_MATCH(
                    ARRAY( t1.category_level4_name, t1.category_level1_name), item -> item !="合计"
                ) AND t1.sku_name = "合计" AND t1.merchant_name = "合计"
            THEN "四级类目"
        ELSE "其他"
        END AS dimention_type -- `维度类型`
        ,DATEDIFF(
            DATE(MAX(t1.dt) OVER())
            ,DATE(MAX(t1.dt) FILTER(WHERE t1.is_up_today>0 AND t1.sku_name != "合计")OVER(PARTITION BY t1.mall_id, t1.sku_id))
            ,"dd"
        ) + 1 AS up_gaps -- `最近下单间隔天数`
    FROM stats t1
    LEFT JOIN datawarehouse_max.ads_changsha_cat4_dashboard_daily_asc t2
        ON t2.dt BETWEEN DATEADD(CURRENT_DATE(), -15, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
        AND t2.dt = t1.dt
        AND t1.mall_id = 871
        AND t2.category_level4_id = t1.category_level4_id
        AND t1.sku_name = "合计"
        AND t1.merchant_name = "合计"
)


SELECT 
	t1.dt AS `日期`
	,t1.mall_id AS `商城id`
	,t1.mall AS `商城`


	,t1.sku_id AS `商品ID`
	,t1.sku_name AS `商品名称`
	,t1.category_level4_id AS `四级类目ID`
	,t1.category_level4_name AS `四级类目名称`
	,t1.category_level1_id AS `一级类目ID`
	,t1.category_level1_name AS `一级类目名称`

	,t1.merchant_id AS `商家ID`
	,t1.merchant_name AS `商家名称`

	,t1.is_up_today AS `当日是否上架`
	,t1.mct_supplied_price_base AS `商家基准供货价`
	,t1.mct_supplied_price_new AS `商家新客供货价`
	,t1.os_pool_num AS `老客户客户池`
	,t1.ns_pool_num AS `新客户客户池`
	,t1.ordered_goods_num AS `下单商品数量`
	,t1.delivered_goods_num AS `送货商品数量`
	
	,t1.after_sale_applied_num AS `售后申请数量`
	,t1.after_sale_refund_amt AS `售后赔付金额`
	,t1.after_sale_applied_ratio AS `售后提交率`
	,t1.after_sale_refund_ratio AS `售后赔付率`
	,t1.exposed_store_num AS `曝光店铺数`
	,t1.exposed_new_store_num AS `新客户曝光店铺数`
	,t1.exposed_old_store_num AS `老客户曝光店铺数`
	,t1.ordered_store_num AS `下单店铺数`
	,t1.ordered_new_store_num AS `新客户下单店铺数`
	,t1.ordered_old_store_num AS `老客户下单店铺数`
	,t1.exposed2ordered_old_store_ratio AS `老客户曝光转化率`

	,t1.exposed2ordered_new_store_ratio AS `新客户曝光转化率`
	,t1.ordered_ns_goods_num AS `新客户下单商品数量`
	,t1.ordered_os_goods_num AS `老客户下单商品数量`
	,t1.ordered_ls_goods_num AS `召回客户下单商品数量`

	,t1.delivered_goods_amt AS `送货商品金额`
	,t1.delivered_ns_goods_amt AS `新客户送货商品金额`
	,t1.delivered_os_goods_amt AS `老客户送货商品金额`
	,t1.delivered_ls_goods_amt AS `召回客户送货商品金额`

	,t1.target_goods_num AS `四级类目目标件数`
	,t1.achieved_target_goods_ratio AS `四级类目目标件数达成率`
	,t1.dimention_type AS `维度类型`
    ,t1.up_gaps AS `最近下单间隔天数`
    
FROM result t1
WHERE t1.dt BETWEEN DATEADD(CURRENT_DATE(), -9, "dd")
            AND DATEADD(CURRENT_DATE(), -1, "dd")
    AND (
        -- t1.up_gaps <= 10 OR t1.sku_name != "合计"
        (t1.dimention_type="商品" AND NVL(t1.up_gaps, -2) BETWEEN 0 AND 10 AND t1.category_level1_name LIKE "%水果%")
        OR (t1.dimention_type != "商品" AND t1.category_level1_name LIKE "%水果%")
        OR (t1.dimention_type = "商城")
    )
    
;
"""
