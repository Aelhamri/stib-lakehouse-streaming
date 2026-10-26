with polls as (
    select distinct event_ts
    from {{ ref('stg_positions_tracked') }}
),

local_polls as (
    select timezone('Europe/Brussels', event_ts at time zone 'UTC') as local_ts
    from polls
),

hourly as (
    select
        date_trunc('hour', local_ts) as local_hour,
        count(*)                     as polls_received
    from local_polls
    group by 1
),

calendar as (
    select unnest(generate_series(
        (select min(local_hour) from hourly),
        (select max(local_hour) from hourly),
        interval 1 hour
    )) as local_hour
)

select
    c.local_hour,
    cast(c.local_hour as date)                              as local_date,
    coalesce(h.polls_received, 0)                           as polls_received,
    120                                                     as polls_expected,
    round(100.0 * coalesce(h.polls_received, 0) / 120, 1)   as completeness_pct
from calendar c
left join hourly h using (local_hour)
order by 1