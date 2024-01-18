with 
stg_region as (

    select * from {{ ref('stg_tpch_sf1__region') }}

),


trn_region as 
(
   select   
        R_REGIONKEY as region_key,
        R_NAME as name
    from stg_region
)

SELECT
    *
FROM trn_region
