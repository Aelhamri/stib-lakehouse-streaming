with arrivals as (
    select
        tracklet_id,
        lineid,
        direction,
        pointId,
        geo_status,
        min(ts_s)  as arrive_ts_s,
        min(s0_m)  as s0_m
    from {{ ref('stg_positions_tracked') }}
    group by 1, 2, 3, 4, 5
),

ordered as (
    select
        *,
        lag(pointId)     over w as from_point,
        lag(arrive_ts_s) over w as prev_arrive_ts_s,
        lag(s0_m)        over w as prev_s0_m
    from arrivals
    window w as (partition by tracklet_id order by arrive_ts_s)
)

select
    tracklet_id,
    lineid,
    direction,
    geo_status,
    from_point,
    pointId                                   as to_point,
    timezone('Europe/Brussels', to_timestamp(arrive_ts_s)) as arrive_local_ts,
    arrive_ts_s - prev_arrive_ts_s            as travel_s,
    s0_m - prev_s0_m                          as length_m,
    (s0_m - prev_s0_m) / (arrive_ts_s - prev_arrive_ts_s) * 3.6 as speed_kmh
from ordered
where from_point is not null
  and s0_m - prev_s0_m > 0
  and arrive_ts_s - prev_arrive_ts_s between 15 and 600