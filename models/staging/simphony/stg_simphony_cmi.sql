  with cmi_raw as(
    select guest_check_number as check_number
        , business_date
        , postransaction_reference
        , menu_item_number
        , menu_item_name_1 as menu_item_name
        , major_group_name
        , line_count
        , line_total
        , line_number
        , include_tax_for_given_total
        , cost
        , report_line_total
        , report_line_count
        , report_include_tax_total
        , report_inclide_tax_total_ext as report_include_tax_total_ext
        , service_round_number
        , order_type_name
        , order_type_number
        , order_type_master_name
        , order_type_master_number
        , combo_meal_number
        , combo_side_number
        , combo_group_number
        , numerator
        , denominator
        , void_type
        , transaction_date_time
        , transaction_date_time_utc
        , transaction_employee_number
        , manager_employee_number
        , check_employee_number
        , meal_employee_number
        , SPLIT_PART(sys_filename,'_',2) as store_number
    from {{source("stage_simphony","cmi")}}
    where
        1=1
        and do_not_show_flag = 0
  )
  
select check_number
    , business_date
    , postransaction_reference
    , menu_item_number
    , menu_item_name
    , major_group_name
    , line_count
    , line_total
    , line_number
    , include_tax_for_given_total
    , cost
    , report_line_total
    , report_line_count
    , report_include_tax_total
    , report_include_tax_total_ext
    , service_round_number
    , order_type_name
    , order_type_number
    , order_type_master_name
    , order_type_master_number
    , combo_meal_number
    , combo_side_number
    , combo_group_number
    , numerator
    , denominator
    , void_type
    , transaction_date_time
    , transaction_date_time_utc
    , transaction_employee_number
    , manager_employee_number
    , check_employee_number
    , meal_employee_number
    , store_number
    , md5(
        concat(
            'fnb_check_items'
            , check_number
            , menu_item_number
            , line_number
            , business_date
            , transaction_date_time
            , store_number
        )
    ) as pk_fnb_check_item
    , md5(
        concat(
            'generic_key'
            , business_date
            , check_number
            , store_number
        )
    ) as generic_key
from cmi_raw