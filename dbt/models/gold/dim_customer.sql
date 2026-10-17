-- SCD2 customer dimension built from the source's version history (one row per customer_id + updated_ts).
-- A new version starts only when a tracked attribute changes. The city is compared after normalization
-- (customer_unit), so the 2025-07-01 relabel of old province names does not look like a move.
with src as (
    select
        *,
        md5(to_json(named_struct(
            'full_name', full_name, 'dob', dob, 'unit', customer_unit,
            'income_band', income_band, 'risk_tier', risk_tier, 'kyc_level', kyc_level
        ))) as attr_hash
    from {{ source('silver', 'stg_customers') }}
),

flagged as (
    select *, lag(attr_hash) over (partition by customer_id order by updated_ts) as prev_hash
    from src
),

versions as (
    select * from flagged
    where prev_hash is null or prev_hash != attr_hash
)

select
    md5(concat(customer_id, '|', cast(updated_ts as string))) as customer_sk,
    customer_id,
    full_name,
    dob,
    customer_unit,
    city_raw,
    income_band,
    risk_tier,
    kyc_level,
    updated_ts as valid_from_ts,
    coalesce(lead(updated_ts) over (partition by customer_id order by updated_ts),
             timestamp_ntz '9999-12-31 00:00:00') as valid_to_ts,   -- exclusive end; open-ended = 9999-12-31
    lead(updated_ts) over (partition by customer_id order by updated_ts) is null as is_current
from versions
