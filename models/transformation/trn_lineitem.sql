with 
stg_lineitem as (

    select * from {{ ref('stg_tpch_sf1__lineitem') }}

),


trn_lineitem as 
(

   select 
        L_ORDERKEY as order_key,
        L_PARTKEY as part_key,
        L_SUPPKEY as supp_key,
        L_LINENUMBER as line_number,
        CAST(L_QUANTITY as INTEGER) as quantity,
        L_EXTENDEDPRICE as extended_price,
        L_DISCOUNT as discount,
        L_TAX as tax,
        L_RETURNFLAG as return_flag,
        L_LINESTATUS as line_status,
        L_SHIPDATE as ship_date,
        L_COMMITDATE as commit_date,
        L_RECEIPTDATE as receipt_date,
        L_SHIPINSTRUCT as ship_instruction,
        L_SHIPMODE as ship_mode,
        ETL_TIMESTAMP
    from stg_lineitem
)

SELECT
    *
FROM trn_lineitem
