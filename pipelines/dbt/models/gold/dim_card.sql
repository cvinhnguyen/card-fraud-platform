-- One row per card (type 1: the source keeps no card history). Carries the owner so the fact can reach dim_customer.
select
    c.card_id,
    c.account_id,
    a.customer_id,
    c.card_type,
    c.network,
    a.account_type,
    a.credit_limit,
    c.issue_ts,
    c.expiry_month,
    c.status
from {{ source('bronze', 'raw_cards') }} c
join {{ source('bronze', 'raw_accounts') }} a on a.account_id = c.account_id
