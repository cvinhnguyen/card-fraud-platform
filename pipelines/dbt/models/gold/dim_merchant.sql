-- One row per merchant (type 1). city is already one of the 34 units; NULL for foreign merchants (country != VN).
select merchant_id, merchant_name, mcc, is_online, city as merchant_unit, country
from {{ source('bronze', 'raw_merchants') }}
