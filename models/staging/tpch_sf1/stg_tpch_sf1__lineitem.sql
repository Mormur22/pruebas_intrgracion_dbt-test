with source as (

    select * from {{ source('tpch_sf1', 'lineitem') }}

),


--Aqui podríamos filtrar por solo los pedidos que están abiertos o por fecha para evitar realizar la carga incremental de los que están ya en el modelo
filtered AS (
    SELECT * 
    FROM source
    WHERE l_linestatus='O' or l_linestatus='P'
),

renamed as (

    select
        l_orderkey,
        l_partkey,
        l_suppkey,
        l_linenumber,
        l_quantity,
        l_extendedprice,
        l_discount,
        l_tax,
        l_returnflag,
        l_linestatus,
        l_shipdate,
        l_commitdate,
        l_receiptdate,
        l_shipinstruct,
        l_shipmode,
        l_comment,
        CURRENT_TIMESTAMP() AS ETL_TIMESTAMP
    FROM filtered
)

select * from renamed
