with 
stg_supplier as (

    select * from {{ ref('stg_tpch_sf1__supplier') }}

),


trn_supplier as 
(
   select   
        S_SUPPKEY as supp_key,
        S_NAME as name,
        S_NATIONKEY as nation_key, 
        {{ test_phone_number('s_phone') }} AS supplier_phone,
        S_ACCTBAL as supplier_account_balance
    from stg_supplier
)

SELECT
    *
FROM trn_supplier
