WITH cte_cmi_agg AS(
/**
Aggregating the items amount, count, tax at check level for each business date, store
**/

  SELECT check_number
      , business_date
      , store_number
      , menu_item_number
      , menu_item_name
      , major_group_name
      , order_type_name
      , generic_key
      , sum(report_line_total) as item_total
      , sum(sum(report_line_total)) over (partition by check_number, business_date, store_number) as check_total 
      , sum(report_line_count) as item_count
      , sum(report_include_tax_total) as item_tax
      , sum(report_include_tax_total_ext) as item_tax_ext
  from {{ref("stg_simphony_cmi")}}
  group by check_number
    , business_date
    , menu_item_number
    , menu_item_name
    , major_group_name
    , order_type_name
    , generic_key
    , store_number
  order by check_number, business_date, store_number
),

cte_cmi as (
    SELECT  *
        , DIV0(item_total, check_total) as item_weight_per_check
    FROM cte_cmi_agg
) 
,
cte_csvc as (
    SELECT  generic_key
        , sum(report_line_total) as service_charge
    FROM {{ref("stg_simphony_csvc")}}
    GROUP BY generic_key
),
cte_cdsc as (
    SELECT
        discount_name
        , discounted_menu_item_number
        , generic_key
        , sum(report_line_total) as discount_amount
    FROM {{ref("stg_simphony_cdsc")}} 
    GROUP BY generic_key, discounted_menu_item_number, discount_name
     
)
,
cmi_agg as(

 SELECT cmi.*
    ,(svc.service_charge * cmi.item_weight_per_check) as service_charge
    ,COALESCE(cdsc.discount_amount,0) as discount_amount
 FROM cte_cmi cmi 
 LEFT JOIN cte_csvc svc
 ON (cmi.generic_key = svc.generic_key)
 LEFT JOIN cte_cdsc cdsc
 ON (cmi.generic_key = cdsc.generic_key
 AND cmi.menu_item_number = cdsc.discounted_menu_item_number)
 order by store_number , business_date, check_number
 )
 
 select * from cmi_agg