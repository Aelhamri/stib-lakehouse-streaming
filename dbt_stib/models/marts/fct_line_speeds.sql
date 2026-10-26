select
    lineid,
    direction,
    extract(hour from arrive_local_ts)        as local_hour_of_day,
    count(*)                                  as n_segments,
    round(median(speed_kmh), 1)               as median_speed_kmh,
    round(quantile_cont(speed_kmh, 0.9), 1)   as p90_speed_kmh
from {{ ref('int_stop_segments') }}
where geo_status = 'LOCATABLE'
group by 1, 2, 3
having count(*) >= 30