select
    cast(ordered_at as date) as order_date,
    coalesce(country, 'unknown') as country,
    count(*) as orders,
    cast(sum(amount) as decimal(18, 2)) as revenue
from {{ ref('fct_orders') }}
group by 1, 2
