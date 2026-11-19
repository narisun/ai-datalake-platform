-- Every row that arrived but could not be used, and why. This is the list to send back
-- to whoever supplied the file.
select
    'order' as entity,
    order_id as business_key,
    reject_reason,
    source_file,
    loaded_at
from {{ ref('stg_orders') }}
where reject_reason is not null

union all

select
    'customer' as entity,
    customer_id as business_key,
    reject_reason,
    source_file,
    loaded_at
from {{ ref('stg_customers') }}
where reject_reason is not null
