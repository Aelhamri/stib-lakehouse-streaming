with arrivals as (
    select
        lineid,
        direction,
        pointId,
        arrive_ts_s,
        timezone('Europe/Brussels', to_timestamp(arrive_ts_s)) as arrive_local_ts
    from {{ ref('int_stop_arrivals') }}
),

gaps as (
    select
        *,
        arrive_ts_s - lag(arrive_ts_s) over (
            partition by lineid, direction, pointId order by arrive_ts_s
        ) as headway_s
    from arrivals
),

valid as (
    select
        *,
        extract(hour from arrive_local_ts) as local_hour_of_day
    from gaps
    where headway_s between 30 and 3600
),

medians as (
    select lineid, direction, local_hour_of_day, median(headway_s) as median_headway_s
    from valid
    group by 1, 2, 3
)

select
    v.lineid,
    v.direction,
    v.local_hour_of_day,
    count(*)                                                                    as n_headways,
    round(m.median_headway_s / 60, 1)                                           as median_headway_min,
    round(stddev_samp(v.headway_s) / avg(v.headway_s), 2)                       as cv,
    round(100.0 * avg(cast(v.headway_s < 0.5 * m.median_headway_s as int)), 1)  as bunching_pct,
    round(100.0 * avg(cast(v.headway_s > 1.5 * m.median_headway_s as int)), 1)  as gap_pct
from valid v
join medians m using (lineid, direction, local_hour_of_day)
group by v.lineid, v.direction, v.local_hour_of_day, m.median_headway_s
having count(*) >= 30