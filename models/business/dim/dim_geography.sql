with nation as (
    select * from {{ ref("trn_nation") }}
),

region as (
     select * from {{ ref("trn_region") }}
)



select 
    nation_key,
    nation_name as nation,
    name as region
  
from nation
join region using (region_key)