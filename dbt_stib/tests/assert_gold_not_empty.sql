select 'fct_collection_health' as table_name from (select count(*) as n from {{ ref('fct_collection_health') }}) where n = 0
union all
select 'fct_headway_regularity' from (select count(*) as n from {{ ref('fct_headway_regularity') }}) where n = 0
union all
select 'fct_line_speeds' from (select count(*) as n from {{ ref('fct_line_speeds') }}) where n = 0