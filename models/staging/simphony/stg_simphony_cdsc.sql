select guest_check_number as check_number
    , business_date
    , discount_name
    , discount_number
    , discount_master_name
    , discount_master_number
    , discounted_menu_item_name_one as discounted_menu_item_name
    , discounted_menue_item_number as discounted_menu_item_number
    , touch_item_discount_instance
    , touch_item_discount_id
    , transaction_employee_number
    , manager_employee_number
    , service_round_number
    , line_number
    , seat_number
    , reference_info
    , line_total
    , line_count
    , include_tax_for_given_total
    , report_line_total
    , report_line_count
    , report_include_tax_total
    , report_inclide_tax_total_ext as report_include_tax_total_ext
    , SPLIT_PART(sys_filename,'_',2) as store_number
    , md5(
        concat(
            'generic_key'
            , business_date
            , check_number
            , store_number
        )
    ) as generic_key
from {{source("stage_simphony", "cdsc")}}
where
    1=1
    AND do_not_show_flag=0