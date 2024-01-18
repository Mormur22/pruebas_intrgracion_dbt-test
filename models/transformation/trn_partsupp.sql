with 
stg_partsupp as (

    select * from {{ ref('stg_tpch_sf1__partsupp') }}

),


trn_partsupp as 
(
   select
        PS_PARTKEY as part_key,
        PS_SUPPKEY as supp_key,
        PS_AVAILQTY as avaibility,
        PS_SUPPLYCOST as supply_cost
    from stg_partsupp
)

SELECT
    *
FROM trn_partsupp
