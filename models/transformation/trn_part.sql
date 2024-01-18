with 
stg_part as (

    select * from {{ ref('stg_tpch_sf1__part') }}

),


trn_part as 
(
   select P_PARTKEY as part_key,
        P_NAME AS AVAILABLE_COLOURS,
        REPLACE(P_MFGR, 'Manufacturer#', '') AS part_manufacturer,
        REPLACE(P_BRAND, 'Brand#', '') AS part_brand,
        TRIM(SPLIT_PART(P_TYPE, ' ', 1)) AS size_type,
        TRIM(SPLIT_PART(P_TYPE, ' ', 2)) AS finished_type,
        TRIM(SPLIT_PART(P_TYPE, ' ', 3)) AS material_type,
        P_SIZE as _size,
        TRIM(SPLIT_PART(P_CONTAINER, ' ', 1)) AS container_size,
        TRIM(SPLIT_PART(P_CONTAINER, ' ', 2)) AS container_type,
        P_RETAILPRICE as retail_price
    from stg_part
)

SELECT
    *
FROM trn_part
