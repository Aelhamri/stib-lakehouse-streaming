select lineid, direction, local_hour_of_day, count(*) as n
from {{ ref('fct_line_speeds') }}
group by 1, 2, 3
having count(*) > 1