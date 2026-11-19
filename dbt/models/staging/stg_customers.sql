-- Raw customers are text, replaced in full by each new customer file. The file is a
-- snapshot, so a customer id appearing twice is a fault in the file: it is not hidden
-- here, and the unique test on this model stops the build.
with typed as (
    select
        trim(customer_id) as customer_id,
        nullif(trim(full_name), '') as full_name,
        lower(nullif(trim(email), '')) as email,
        upper(nullif(trim(country), '')) as country,
        _source_file as source_file,
        _loaded_at as loaded_at
    from {{ source('raw', 'customers') }}
    where nullif(trim(customer_id), '') is not null
)

select
    customer_id,
    full_name,
    email,
    country,
    case
        when full_name is null then 'full_name is missing'
        when email is null then 'email is missing'
        when email not like '%_@_%' then 'email is not an address: ' || email
    end as reject_reason,
    source_file,
    loaded_at
from typed
