with supplier as (
    select * from {{ ref('trn_supplier')}}
)


select * from supplier