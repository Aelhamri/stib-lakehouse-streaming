select *
from {{ ref('fct_collection_health') }}
where completeness_pct > 101