with 
stg_nation as (

    select * from {{ ref('stg_tpch_sf1__nation') }}

),


trn_nation as 
(

   select   N_NATIONKEY as nation_key, 
            N_NAME as nation_name,
            N_REGIONKEY as region_key
    from stg_nation
)

SELECT
    *
FROM trn_nation
