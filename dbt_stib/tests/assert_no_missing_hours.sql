select *
from (
    select
        count(*) as n,
        date_diff('hour', min(local_hour), max(local_hour)) + 1 as expected
    from {{ ref('fct_collection_health') }}
)
where n <> expected