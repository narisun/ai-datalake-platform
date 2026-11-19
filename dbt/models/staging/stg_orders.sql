-- Raw orders are text, one row per row of every file ever loaded. A corrected order can
-- arrive again in a later file, so keep the most recently loaded row per order.
with ranked as (
    select
        *,
        row_number() over (
            partition by trim(order_id)
            order by _loaded_at desc, _source_file desc, _row_number desc
        ) as recency
    from {{ source('raw', 'orders') }}
    where nullif(trim(order_id), '') is not null
),

typed as (
    select
        trim(order_id) as order_id,
        nullif(trim(customer_id), '') as customer_id,
        -- try_cast gives null instead of failing the whole model on one bad value.
        try_cast(trim(ordered_at) as timestamp(6)) as ordered_at,
        try_cast(trim(amount) as decimal(18, 2)) as amount,
        ordered_at as ordered_at_text,
        amount as amount_text,
        _source_file as source_file,
        _loaded_at as loaded_at
    from ranked
    where recency = 1
)

select
    order_id,
    customer_id,
    ordered_at,
    amount,
    case
        when customer_id is null then 'customer_id is missing'
        when ordered_at is null
            then 'ordered_at is not a date and time: ' || coalesce(ordered_at_text, '(empty)')
        when amount is null
            then 'amount is not a number: ' || coalesce(amount_text, '(empty)')
    end as reject_reason,
    source_file,
    loaded_at
from typed
