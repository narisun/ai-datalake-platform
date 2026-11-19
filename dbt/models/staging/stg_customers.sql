select
    cast(customer_id as varchar) as customer_id,
    cast(full_name as varchar) as full_name,
    lower(cast(email as varchar)) as email,
    cast(country as varchar) as country
from {{ source('raw', 'customers') }}
