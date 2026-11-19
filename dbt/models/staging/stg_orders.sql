select
    cast(order_id as varchar) as order_id,
    cast(customer_id as varchar) as customer_id,
    cast(ordered_at as timestamp) as ordered_at,
    cast(amount as decimal(18, 2)) as amount
from {{ source('raw', 'orders') }}
