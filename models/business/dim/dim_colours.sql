with part as (
    select * from {{ref('trn_part')}}
),

part_colours as(
    SELECT 
       part_key,
        REPLACE(value, '"', '')as colour
    FROM 
        part,
        LATERAL FLATTEN(INPUT => SPLIT(available_colours, ' ')) AS colors
)


select * 
from part_colours