select
    customer_id,
    full_name,
    email,
    country
from {{ ref('stg_customers') }}
