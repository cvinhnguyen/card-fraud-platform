-- One row per calendar day of the transaction period.
with bounds as (
    select min(txn_date) as lo, max(txn_date) as hi from {{ source('silver', 'stg_transactions') }}
)
select
    cast(date_format(d, 'yyyyMMdd') as int) as date_key,
    d as full_date,
    year(d) as year,
    month(d) as month,
    date_format(d, 'yyyy-MM') as year_month,
    dayofweek(d) as day_of_week,          -- 1 = Sunday
    dayofweek(d) in (1, 7) as is_weekend,
    d >= date '2025-07-01' as is_after_reorg   -- 63 -> 34 province reorganization
from bounds
lateral view explode(sequence(lo, hi, interval 1 day)) t as d
