with
    customer as (select * from {{ ref("dim_customer") }}),

    geography as (select * from {{ ref("dim_geography") }}),

    _time as (select * from {{ ref("dim_time") }}),

    suppliers as (select * from {{ ref("dim_suppplier") }}),

    colours as (select * from {{ ref("dim_colours") }}),

    parts as (select * from {{ ref("dim_part") }}),

    part_suppliers as (select * from {{ ref("trn_partsupp") }}),

    line_items as (select * from {{ ref("trn_lineitem") }}),

    orders as (select * from {{ ref("trn_orders") }}),

    -- Suppliers and parts
    part_colours as (select * from {{ ref("dim_colours") }}),

    supplier_geography as (
        select
            s.supp_key,
            s.name,
            s.supplier_phone,
            s.supplier_account_balance,
            g.region as supplier_region,
            g.nation as supplier_nation
        from suppliers s
        join geography g using (nation_key)
    ),

    part_colour as (select p.*, c.colour from parts p join colours c using (part_key)),

    part_supplier as (

        select s.*, ps.part_key, ps.avaibility, ps.supply_cost
        from supplier_geography s
        join part_suppliers ps using (supp_key)
        join parts using (part_key)
    ),

    -- Customers
    customer_geography as (
        select
            c.name,
            c.customer_key,
            c.customer_phone,
            c.customer_account_balance,
            c.market_segment,
            g.region as customer_region,
            g.nation as customer_nation
        from customer c
        join geography g using (nation_key)
    ),

    customer_orders as (
        select
            o.order_key,
            o.customer_key,
            o.order_status,
            o.ok_status,
            o.total_price,
            o.order_date,
            o.order_priority,
            o.clerk,
            o.ship_priority,
            o.etl_timestamp as order_timestamp,
            cg.name as customer_name,
            cg.customer_phone,
            cg.customer_account_balance,
            cg.market_segment,
            cg.customer_region,
            cg.customer_nation
        from orders o
        join customer_geography cg using (customer_key)
    ),

    lineitem_partsupp_orders as (
        select
            line_number,
            quantity,
            extended_price,
            discount,
            tax,
            return_flag,
            line_status,
            ship_date,
            commit_date,
            receipt_date,
            ship_instruction,
            ship_mode,
            etl_timestamp as lineitem_etl,
            ps.*,
            co.*
        from line_items l
        join part_supplier ps on l.part_key = ps.part_key and l.supp_key = ps.supp_key
        join customer_orders co on l.order_key = co.order_key
    )

select *
from lineitem_partsupp_orders
