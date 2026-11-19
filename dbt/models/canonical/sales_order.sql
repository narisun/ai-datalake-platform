select
    order_id,
    customer_id,
    ordered_at,
    amount
from {{ ref('stg_orders') }}
where reject_reason is null
