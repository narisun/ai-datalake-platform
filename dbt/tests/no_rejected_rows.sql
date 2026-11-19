-- Warns, with the count, whenever any arriving row could not be used. It does not stop
-- the build: the valid rows are still published, and rejected_rows says what to fix.
{{ config(severity='warn') }}

select * from {{ ref('rejected_rows') }}
