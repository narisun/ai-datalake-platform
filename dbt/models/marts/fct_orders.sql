select
    o.order_id,
    o.customer_id,
    c.country,
    o.ordered_at,
    o.amount
from {{ ref('sales_order') }} as o
left join {{ ref('customer') }} as c
    on o.customer_id = c.customer_id
