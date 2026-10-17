-- Versions of one customer follow each other without gaps or overlaps.
select customer_id, valid_to_ts
from (
    select customer_id, valid_to_ts,
           lead(valid_from_ts) over (partition by customer_id order by valid_from_ts) as next_from
    from {{ ref('dim_customer') }}
) v
where next_from is not null and next_from != valid_to_ts
