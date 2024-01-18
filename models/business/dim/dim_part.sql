with part as (
    select * from {{ref('trn_part')}}
),

final as(
    select 
        part_key,
        part_manufacturer,
        part_brand,
        size_type,
        finished_type,
        material_type,
        _size,
        container_size,
        container_type,
        retail_price
    from part
)

select * 
from final