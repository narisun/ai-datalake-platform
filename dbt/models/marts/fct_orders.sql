select
    o.order_id,
    o.customer_id,
    c.country,
    o.ordered_at,
    o.amount
from {{ ref('stg_orders') }} as o
left join {{ ref('dim_customer') }} as c
    on o.customer_id = c.customer_id
