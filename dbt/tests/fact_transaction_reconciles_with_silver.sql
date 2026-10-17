-- Rows and settled spend in the fact must equal silver: no row lost or duplicated by the dimension joins.
with f as (select count(*) as n, sum(amount_vnd) as spend from {{ ref('fact_transaction') }}),
     s as (select count(*) as n, sum(amount_vnd) as spend from {{ source('silver', 'stg_transactions') }} where status = 'settled')
select * from f cross join s where f.n != s.n or f.spend != s.spend
