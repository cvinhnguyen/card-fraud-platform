-- Grain: one settled transaction. Reversed transactions are left out (no money moved); fraud labels live in
-- gold.label_transaction because they change after the fact is written.
-- Incremental: only silver export dates newer than the newest in this table, MERGEd on transaction_id.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='transaction_id',
    partition_by=['txn_month'],
) }}

with txn as (
    select * from {{ source('silver', 'stg_transactions') }}
    where status = 'settled'
    {% if is_incremental() %}
      and export_date > (select max(export_date) from {{ this }})
    {% endif %}
)

select
    t.transaction_id,
    cast(date_format(t.txn_date, 'yyyyMMdd') as int) as date_key,
    t.txn_ts,
    t.card_id,
    d.customer_sk,                         -- customer version valid at txn_ts (point-in-time)
    t.merchant_id,
    t.terminal_id,
    t.txn_unit,
    t.txn_city_raw,
    t.channel,
    t.auth_method,
    t.three_ds_version,
    t.amount_vnd,
    t.currency,                            -- USD = cross-border (foreign merchant), amount_vnd already converted
    t.export_date,
    date_format(t.txn_date, 'yyyy-MM') as txn_month
from txn t
join {{ ref('dim_card') }} c on c.card_id = t.card_id
left join {{ ref('dim_customer') }} d
    on d.customer_id = c.customer_id and t.txn_ts >= d.valid_from_ts and t.txn_ts < d.valid_to_ts
