select
    tracklet_id,
    lineid,
    direction,
    pointId,
    geo_status,
    min(ts_s) as arrive_ts_s,
    min(s0_m) as s0_m
from {{ ref('stg_positions_tracked') }}
group by 1, 2, 3, 4, 5