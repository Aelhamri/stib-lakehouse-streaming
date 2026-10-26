select *
from {{ ref('fct_line_speeds') }}
where median_speed_kmh < 3 or median_speed_kmh > 90