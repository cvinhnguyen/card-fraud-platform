-- Every customer has exactly one current version.
select customer_id
from {{ ref('dim_customer') }}
group by customer_id
having sum(case when is_current then 1 else 0 end) != 1
