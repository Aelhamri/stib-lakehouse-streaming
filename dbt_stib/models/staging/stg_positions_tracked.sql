select * exclude (value)
from {{ source('silver', 'positions_tracked') }}