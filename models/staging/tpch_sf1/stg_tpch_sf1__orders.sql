with 

source as (

    select * from {{ source('tpch_sf1', 'orders') }}

),


--Aqui podríamos filtrar por solo los pedidos que están abiertos o por fecha para evitar realizar la carga incremental de los que están ya en el modelo
filtered as(
    select *
    from source
    where o_orderstatus='O' or o_orderstatus='P'
),

renamed as (

    select
        o_orderkey,
        o_custkey,
        o_orderstatus,
        o_totalprice,
        o_orderdate,
        o_orderpriority,
        o_clerk,
        o_shippriority,
        o_comment,
        CURRENT_TIMESTAMP() AS ETL_TIMESTAMP

    from filtered

)

select * from renamed
