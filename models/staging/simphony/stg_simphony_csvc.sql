select guest_check_number as check_number
    , business_date
    , transaction_date
    , service_charge_name
    , service_charge_number
    , service_charge_master_name
    , service_charge_master_number
    , service_round_number
    , line_number
    , seat_number
    , check_employee_number
    , manager_employee_number
    , line_count
    , line_total
    , report_line_count
    , report_include_tax_total
    , report_inclide_tax_total_ext as report_inlcude_tax_total_ext 
    , report_line_total
    , split_part(sys_filename,'_',2) as store_number
    , md5(
        concat(
            'generic_key'
            , business_date
            , check_number
            , menu_item_number
            , store_number
        )
    )
from sh_dev.stage_simphony.csvc 
where 
    1=1
    and do_not_show_flag = 0