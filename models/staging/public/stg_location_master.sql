select id
    , account_name
    , restaurant_type
    , business_unit
    , region
    , country
    , currency
    , simphony_prod
    , icare_store_name
    , opera_house_id
    , kounta
    , b4t_location_id
    , nexudus
    , no_of_hotel_rooms
    , hotel_size
    , address
    , time_zone
    , time_zone_id
from {{source("dbt_public", "location_master")}}
where 
    1=1
    and lower(account_name) not like 'test'