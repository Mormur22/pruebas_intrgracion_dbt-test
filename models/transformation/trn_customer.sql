with 
stg_customer as (

    select * from {{ ref('stg_tpch_sf1__customer') }}

),


trn_customer as 
(

    select  
            c_name as name,
            c_custkey as customer_key,
            c_nationkey as nation_key,
            {{ test_phone_number('c_phone') }} AS customer_phone,
            c_acctbal as customer_account_balance,
            c_mktsegment as market_segment
    from
    stg_customer
)

SELECT
    *
FROM trn_customer
