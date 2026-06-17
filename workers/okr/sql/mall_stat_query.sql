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


-- 流量相关
,flow_stat AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.mall_name AS mall -- `商城`
        ,COUNT(DISTINCT IF(t1.exposed_sku_num>0, t1.customer_store_id, NULL)) AS exposed_store_num -- `曝光店铺数`
        ,COUNT(DISTINCT IF(
            t1.exposed_sku_num>0 AND t1.page_name="买过页", t1.customer_store_id, NULL
        )) AS exposed_store_num_page_maiguo -- `买过页面曝光店铺数`
        ,SUM(IF(
            t1.exposed_sku_num>0 AND t1.page_name="买过页", t1.exposed_sku_num, 0
        )) AS exposed_page_view_num_page_maiguo -- `买过页面曝光数量`
    FROM datawarehouse_max.dws_flow_store_page_daily_asc t1
    WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.mall_id = 871
        AND t1.mall_name = "标果长沙"
        AND NVL(t1.exposed_sku_num, 0) + NVL(t1.clicked_sku_num, 0) + NVL(t1.added_sku_num, 0)+NVL(t1.ordered_sku_num,0) > 0

    GROUP BY t1.dt -- `日期`
        ,t1.mall_id
        ,t1.mall_name
)





-- FIXED: 计算商品日活覆盖率达表的SKU数，添加了过滤条件SKU数大于0
, sku_stat AS(
    SELECT
        t1.dt
        ,t1.mall_id
        ,t1.category_level1_id
        ,t1.category_level4_id

        ,COUNT(
            DISTINCT IF(t1.ordered_store_num / t2.ordered_store_num >= 0.1, t1.sku_id, NULL )
        ) AS acheive_target_sku_num -- `达标sku数`
    FROM datawarehouse_max.ads_pub_mall_sku_stata_daily_asc t1
    LEFT JOIN datawarehouse_max.dws_pub_mall_cate1_base_daily_asc t2
        ON t2.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t2.dt = t1.dt
        AND t2.mall_id = t1.mall_id
        AND t2.category_level1_id = t1.category_level1_id
    WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.mall_id = 871
    GROUP BY t1.dt
        ,t1.mall_id
        ,t1.category_level1_id
        ,t1.category_level4_id


    HAVING acheive_target_sku_num > 0
)


-- 品质问题售后
,css_stat AS(

    SELECT
        t1.dt -- `日期`
        ,t1.mall_id
        ,t1.category_level1_id -- `一级类目id`
        ,t1.category_level4_id -- `四级类目id`
        ,t1.category_level4_name -- `四级类目名称`

        ,SUM(t1.final_refund_amt) AS final_refund_amt
        ,SUM(t1.after_sale_num) AS after_sale_num
        ,SUM(t1.after_sale_cnt) AS after_sale_cnt

    FROM datawarehouse_max.dws_qa_cate4_after_sale_type_stat_daily_asc t1
    WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                        AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.mall_id = 871
        -- AND t1.category_level4_name IN ("金枕榴莲","干尧榴莲","甲仑榴莲","托曼尼榴莲","青尼榴莲")
        AND t1.after_sale_type_desc NOT IN ( 
            "成熟度", "腐坏变质", "鲜度问题"
        )
    GROUP BY t1.dt -- `日期`
        ,t1.mall_id
        ,t1.category_level1_id -- `一级类目id`
        ,t1.category_level4_id -- `四级类目id`
        ,t1.category_level4_name -- `四级类目名称`
)


-- 四级类目基础统计
,cat4_stat AS(
    SELECT
        t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.mall -- `商城`
        ,SUM(t1.delivered_goods_amt) AS delivered_goods_amt -- `送达金额`
        ,SUM(t1.is_filtered * t1.delivered_goods_amt) AS delivered_goods_amt_filtered -- `过滤后送达金额`
        ,SUM(t1.final_refund_amt) AS final_refund_amt -- `赔付金额`
        ,SUM(t1.is_filtered * t1.final_refund_amt) AS final_refund_amt_filtered -- `过滤后赔付金额`
        ,SUM(t1.sku_num_onsale) AS sku_num_onsale -- `在售sku数`

        ,SUM(t1.sku_num_sold) AS sku_num_sold -- `动销sku数`
        ,SUM(t1.sku_num_sold * t1.is_fruit) AS sku_num_sold_fruit -- `动销水果sku数`
        ,SUM(t1.is_ka_operate_cat4 * t1.delivered_goods_amt) AS delivered_goods_amt_ka_operate -- `ka运营品类送达金额`
        ,SUM(t1.is_ka_operate_cat4 * t1.final_refund_amt) AS final_refund_amt_ka_operate -- `ka运营品类赔付金额`

        -- 蔬菜
        ,SUM(t1.is_vegitable * t1.sku_num_onsale) AS sku_num_onsale_vegitable -- `蔬菜在售sku数`
        ,SUM(t1.is_vegitable * t1.delivered_goods_amt) AS delivered_goods_amt_vegitable -- `蔬菜送达金额`
        ,SUM(t1.is_vegitable * t1.final_refund_amt) AS final_refund_amt_vegitable -- `蔬菜赔付金额`

        -- 水果
        ,SUM(t1.is_fruit * t1.delivered_goods_amt) AS delivered_goods_amt_fruit -- `水果送达金额`

        ,COUNT(DISTINCT IF(t1.sku_num_onsale>0, t1.category_level4_id, NULL)) AS cat4_num_onsale -- `在售四级类目数`
        ,COUNT(DISTINCT IF(t1.sku_num_sold>0, t1.category_level4_id, NULL)) AS cate4_num_sold -- `动销四级类目数`
        ,COUNT(DISTINCT IF(t1.penetration_rate_cat1 >=0.35, t1.category_level4_id, NULL)) AS cat4_num_penet_rate_cat1_gtp35 -- `一级类目渗透率超过35点的四级类目数`
        ,COUNT(DISTINCT IF(t1.penetration_rate_cat1 BETWEEN 0.25 AND 0.35 AND t1.penetration_rate_cat1 <> 0.35, t1.category_level4_id, NULL)) AS cat4_num_penet_rate_cat1_gep25ltp35 -- `一级类目渗透率【25,35）的四级类目数`
        ,COUNT(DISTINCT IF(t1.penetration_rate_cat1 BETWEEN 0.15 AND 0.25 AND t1.penetration_rate_cat1 <> 0.25, t1.category_level4_id, NULL)) AS cat4_num_penet_rate_cat1_gep15ltp25 -- `一级类目渗透率【15,25）的四级类目数`
        ,COUNT(DISTINCT IF(t1.penetration_rate_cat1 BETWEEN 0.05 AND 0.15 AND t1.penetration_rate_cat1 <> 0.15, t1.category_level4_id, NULL)) AS cat4_num_penet_rate_cat1_gep5ltp15 -- `一级类目渗透率【5,15）的四级类目数`
        ,COUNT(DISTINCT IF(t1.penetration_rate_cat1 < 0.05 AND t1.sku_num_sold>0, t1.category_level4_id, NULL)) AS cat4_num_penet_rate_cat1_ltp5 -- `一级类目渗透率低于5点的四级类目数`

        ,COUNT(DISTINCT IF(t1.penetration_rate_cat1 >=0.15, t1.category_level4_id, NULL)) AS cat4_num_penet_rate_cat1_gtp15 -- `一级类目渗透率超过15点的四级类目数`
        ,COUNT(DISTINCT IF(t1.penetration_rate_cat1 >=0.15 AND t1.penetration_rate_cat1_rnk<=3, t1.category_level4_id, NULL)) AS cat4_num_penet_rate_cat1_gtp35_top3 -- `一级类目渗透率超过15点且排名top3的四级类目数`

        -- 特殊品类运营
        ,SUM(t1.is_special_operate * t1.delivered_goods_amt) AS delivered_goods_amt_special_operate -- `特殊运营品类送达金额`
        ,COUNT(DISTINCT IF(t1.penetration_rate_cat1 >=0.1 AND t1.is_special_operate=1, t1.category_level4_id, NULL)) AS penet_rate_cat1_gtp10_special_operate -- `一级类目渗透率超过10点的特殊运营四级类目数`
        ,SUM(t1.acheive_target_sku_num * t1.is_special_operate) AS acheive_target_sku_num_special_operate -- `特殊运营品类商品一级类目渗透率超10点的商品数`
        ,SUM(t1.is_special_operate * t1.order_cnt) AS order_cnt_special_operate -- `特殊运营品类明细单量`
        ,SUM(t1.after_sale_num_order_time * t1.is_special_operate) AS after_sale_num_order_time_special_operate -- `特殊运营品类售后单数量`
        ,SUM(t1.final_refund_amt_non_quality * t1.is_special_operate) AS final_refund_amt_non_quality -- `非品质问题赔付金额`
        ,SUM(t1.after_sale_num_non_quality * t1.is_special_operate) AS after_sale_num_non_quality -- `非品质问题售后数量`
        ,SUM(t1.after_sale_cnt_non_quality * t1.is_special_operate) AS after_sale_cnt_non_quality -- `非品质问题售后单量`


    FROM(
        SELECT
            t1.dt -- `日期`
            ,t1.mall_id -- `商城id`
            ,t1.mall_name AS mall -- `商城`

            ,t1.category_level1_name -- `一级类目名称`
            ,t1.category_level2_name -- `二级类目名称`
            ,t1.category_level3_name -- `三级类目名称`
            ,t1.category_level4_id -- `四级类目id`
            ,t1.category_level4_name -- `四级类目名称`
            -- 所见即所得剔除榴莲
            ,IF(
                t1.category_level4_name IN ("金枕榴莲","干尧榴莲","甲仑榴莲","托曼尼榴莲","青尼榴莲")
                ,0, 1
            ) AS is_filtered -- `是否需要过滤`

            -- 所见即所得运营
            ,IF(
                t1.category_level4_name IN ("麒麟西瓜", "香蕉"), 1, 0
            ) AS is_ka_operate_cat4 -- `是否ka试点品类`

            -- 特殊品类运营
            ,IF(
                t1.category_level4_name IN ("金枕榴莲","干尧榴莲","甲仑榴莲","托曼尼榴莲","青尼榴莲")
                ,1, 0
            ) AS is_special_operate -- `是否特殊品类运营`
            ,t1.delivered_goods_amt -- `送达金额`

            -- 规模
            ,IF(t1.category_level1_name IN ("蔬菜", "干货"), 1, 0) AS is_vegitable -- `是否蔬菜类目`
            ,IF(t1.category_level1_name ="水果", 1, 0) AS is_fruit -- `是否水果类目`

            ,t1.final_refund_amt -- `赔付金额`
            ,t1.sku_num_onsale -- `在售sku数`
            ,t1.sku_num_sold -- `动销sku数`

            ,t6.final_refund_amt AS final_refund_amt_non_quality -- `非品质问题赔付金额`
            ,t6.after_sale_num AS after_sale_num_non_quality -- `非品质问题售后数量`
            ,t6.after_sale_cnt AS after_sale_cnt_non_quality -- `非品质问题售后单量`

            ,t1.exposed_store_num_old_user -- `老客户曝光店铺数`
            ,t1.ordered_store_num_old_user -- `老客户下单店铺数`
            ,ROUND(NVL(t1.ordered_store_num / t3.ordered_store_num, 0), 5) AS penetration_rate_cat1 -- `一级类目渗透率`
            ,ROUND(NVL(t1.ordered_store_num / t4.ordered_store_num, 0), 5) AS dau_rate -- `日活覆盖率`
            ,t5.acheive_target_sku_num -- `达标sku数`

            ,t1.order_cnt -- `明细订单数`
            ,t1.after_sale_num_order_time -- `售后单数量`

            ,ROW_NUMBER() OVER(
                PARTITION BY t1.category_level1_id, t1.category_level1_name, t1.category_level4_name, t1.category_level4_id, t1.dt
                ORDER BY NVL(t1.delivered_goods_amt, 0) DESC
            ) AS delivered_goods_amt_rnk -- `送达金额排名`
            ,ROW_NUMBER() OVER(
                PARTITION BY t1.category_level1_id, t1.category_level1_name, t1.category_level4_name, t1.category_level4_id, t1.dt
                ORDER BY NVL(t1.ordered_store_num / t3.ordered_store_num, 0) DESC
            ) AS penetration_rate_cat1_rnk -- `一级类目渗透率排名`
            ,ROW_NUMBER() OVER(
                PARTITION BY t1.category_level1_id, t1.category_level1_name, t1.category_level4_name, t1.category_level4_id, t1.dt
                ORDER BY NVL(t1.ordered_store_num / t4.ordered_store_num, 0) DESC
            ) AS dau_rate_rnk -- `日活覆盖率排名`


        FROM datawarehouse_max.dws_pub_mall_category_level4_base_daily_asc t1
        JOIN mall t2
            ON t2.mall_id = t1.mall_id
        LEFT JOIN datawarehouse_max.dws_pub_mall_cate1_base_daily_asc t3
            ON t3.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                    AND DATEADD(${date_param}, ${end_offset}, "dd")
            AND t3.dt = t1.dt
            AND t3.mall_id = t1.mall_id
            AND t3.category_level1_id = t1.category_level1_id


        LEFT JOIN datawarehouse_max.dws_pub_mall_base_daily_asc t4
            ON t4.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                    AND DATEADD(${date_param}, ${end_offset}, "dd")
            AND t4.dt = t1.dt
            AND t4.mall_id = t1.mall_id

        LEFT JOIN sku_stat t5
            ON t5.dt = t1.dt
            AND t5.mall_id = t1.mall_id
            AND t5.category_level1_id = t1.category_level1_id
            AND t5.category_level4_id = t1.category_level4_id

        LEFT JOIN css_stat t6
            ON t6.dt = t1.dt
            AND t6.mall_id = t1.mall_id
            AND t6.category_level1_id = t1.category_level1_id
            AND t6.category_level4_id = t1.category_level4_id

        WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                    AND DATEADD(${date_param}, ${end_offset}, "dd")
            AND ISNOTNULL(t1.category_level4_id)
        QUALIFY t1.mall_id = 871


    ) t1
    GROUP BY t1.dt -- `日期`
        ,t1.mall_id -- `商城id`
        ,t1.mall -- `商城`
)



SELECT
	t1.dt `日期`
	,t1.mall_id `商城id`
	,t1.mall `商城`

    ,t3.ordered_store_num AS `下单店铺数`
    ,t3.ordered_store_num_old_user AS `老客户下单店铺数`
    ,t3.ordered_store_num_new_user AS `新客户下单店铺数`
    ,t3.ordered_store_num_loss_user AS `流失客户下单店铺数`

	,NVL(t1.delivered_goods_amt,0)  `送达金额`
	,NVL(t1.final_refund_amt,0)  `赔付金额`
    ,ROUND(
        NVL(t1.final_refund_amt / t1.delivered_goods_amt, 0), 5
    ) AS `售后赔付率`
    ,NVL(t1.delivered_goods_amt_filtered,0)  `过滤后送达金额`
	,NVL(t1.final_refund_amt_filtered,0)  `过滤后赔付金额`
    ,ROUND(
        NVL(t1.final_refund_amt_filtered / t1.delivered_goods_amt_filtered, 0), 5
    ) AS `过滤后售后赔付率`
	,NVL(t1.sku_num_onsale,0)  `在售sku数`
	,NVL(t1.sku_num_sold,0)  `动销sku数`
    ,NVL(t1.sku_num_sold_fruit,0) AS `动销水果sku数`
	,NVL(t1.delivered_goods_amt_ka_operate,0)  `ka运营品类送达金额`
	,NVL(t1.final_refund_amt_ka_operate,0)  `ka运营品类赔付金额`
    ,NVL(t1.cat4_num_onsale,0) AS `在售四级类目数`
	,NVL(t1.cate4_num_sold,0)  `动销四级类目数`
	,NVL(t1.cat4_num_penet_rate_cat1_gtp35,0)  `一级类目渗透率超过35点的四级类目数`
	,NVL(t1.cat4_num_penet_rate_cat1_gep25ltp35,0)  `一级类目渗透率【25,35）的四级类目数`
	,NVL(t1.cat4_num_penet_rate_cat1_gep15ltp25,0)  `一级类目渗透率【15,25）的四级类目数`
	,NVL(t1.cat4_num_penet_rate_cat1_gep5ltp15,0)  `一级类目渗透率【5,15）的四级类目数`
	,NVL(t1.cat4_num_penet_rate_cat1_ltp5,0)  `一级类目渗透率低于5点的四级类目数`

	,NVL(t1.cat4_num_penet_rate_cat1_gtp15,0)  `一级类目渗透率超过15点的四级类目数`
	,NVL(t1.cat4_num_penet_rate_cat1_gtp35_top3,0)  `一级类目渗透率超过15点且排名top3的四级类目数`


    -- 流量
	,NVL(t2.exposed_store_num,0)  `曝光店铺数`
	,NVL(t2.exposed_store_num_page_maiguo,0)  `买过页面曝光店铺数`
	,NVL(t2.exposed_page_view_num_page_maiguo,0)  `买过页面曝光数量`
    ,ROUND(
        NVL(t2.exposed_store_num_page_maiguo / t2.exposed_store_num, 0), 5
    ) AS `买过页面使用率`
    -- 规模
    ,NVL(t1.sku_num_onsale_vegitable,0) AS `蔬菜在售sku数`
    ,NVL(t1.delivered_goods_amt_vegitable,0) AS `蔬菜送达金额`
    ,NVL(t1.final_refund_amt_vegitable,0) AS `蔬菜赔付金额`
    ,ROUND(
        NVL(t1.final_refund_amt_vegitable / t1.delivered_goods_amt_vegitable,0), 5
    ) AS `蔬菜赔付率`
    ,NVL(t1.delivered_goods_amt_fruit,0) AS `水果送达金额`
    ,ROUND(
        NVL(t1.delivered_goods_amt_fruit / t1.delivered_goods_amt,0), 5
    ) AS `水果送达金额占比`

    -- 特殊品类运营
	,NVL(t1.delivered_goods_amt_special_operate,0)  `特殊运营品类送达金额`
    ,NVL(t1.order_cnt_special_operate,0) AS `特殊运营品类明细单量`
	,NVL(t1.penet_rate_cat1_gtp10_special_operate,0)  `一级类目渗透率超过10点的特殊运营四级类目数`
	,NVL(t1.acheive_target_sku_num_special_operate,0)  `特殊运营品类商品一级类目渗透率超10点的商品数`
    ,NVL(t1.after_sale_num_order_time_special_operate,0) AS `特殊运营品类售后单数量`
    ,NVL(t1.final_refund_amt_non_quality,0) AS `特殊运营品类非品质问题赔付金额`
    ,NVL(t1.after_sale_num_non_quality,0) AS `特殊运营品类非品质问题售后数量`
    ,NVL(t1.after_sale_cnt_non_quality,0) AS `特殊运营品类非品质问题售后单量`
    ,ROUND(
        NVL(t1.final_refund_amt_non_quality / t1.delivered_goods_amt_special_operate,0), 5
    ) AS `特殊运营品类赔付率`
    ,ROUND(
        NVL(t1.after_sale_cnt_non_quality / t1.after_sale_num_order_time_special_operate, 0), 5
    ) AS `特殊运营非品质问题售后单占比`
FROM cat4_stat t1
LEFT JOIN flow_stat t2
    ON t2.dt = t1.dt
    AND t2.mall_id = t1.mall_id

LEFT JOIN datawarehouse_max.dws_pub_mall_base_daily_asc t3
    ON t3.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
    AND t3.dt = t1.dt
    AND t3.mall_id = t1.mall_id

WHERE t1.dt BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
            AND DATEADD(${date_param}, ${end_offset}, "dd")
        AND t1.mall_id = 871
;