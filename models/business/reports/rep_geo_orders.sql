with
    fact_orders as (select * from {{ ref("fact_orders") }}),

    number_of_national_order_lineitems as (
        select supplier_nation as nation, count(*) as number_national_order_lineitems
        from fact_orders
        where supplier_nation = customer_nation
        group by supplier_nation
    ),

    kpi_of_international_order_lineitems_origin as (
        select supplier_nation, count(*) as number_international_order_lineitems_origin
        from fact_orders
        where supplier_nation != customer_nation
        group by supplier_nation
    ),

    kpi_of_international_order_lineitems_destiny as (
        select customer_nation, count(*) as number_international_order_lineitems_destiny
        from fact_orders
        where supplier_nation != customer_nation
        group by customer_nation
    ),

    kpi_of_regional_orders_lineitems as (
        select supplier_region as region, count(*) as number_regional_orders_lineitems
        from fact_orders
        where supplier_nation = customer_nation
        group by supplier_region
    ),

    kpi_of_interregional_orders_lineitems_origin as (
        select supplier_region, count(*) as number_interregional_orders_lineitems_origin
        from fact_orders
        where supplier_region != customer_region
        group by supplier_region
    ),

    kpi_of_interregional_orders_lineitems_destiny as (
        select
            customer_region, count(*) as number_interregional_orders_lineitems_destiny
        from fact_orders
        where supplier_region != customer_region
        group by customer_region
    ),

    lineitems_by_geography as (
        select
            supplier_nation as origin_nation,
            supplier_region as origin_region,
            customer_nation as destination_nation,
            customer_region as destination_region,
            count(*) as number_of_lineitems
        from fact_orders
        group by supplier_nation, supplier_region, customer_nation, customer_region
    ),

    most_popular_destinations as (
        select
            origin_nation,
            origin_region,
            destination_nation,
            destination_region,
            number_of_lineitems,
            rank() over (
                partition by origin_nation order by number_of_lineitems desc
            ) as nation_rank,
            rank() over (
                partition by origin_region order by number_of_lineitems desc
            ) as region_rank
        from lineitems_by_geography
    ),

    top_destinations as (
        select
            origin_nation,
            origin_region,
            destination_nation as top_destination_nation,
            destination_region as top_destination_region
        from most_popular_destinations
        where nation_rank = 1 or region_rank = 1
    ),

    {% set total_lineitems = get_total_lineitems() %}

    orders_kpi_by_geography as (
        select
            geo.region as region,
            geo.nation as nation,
            {{ total_lineitems }} as total_orders,
            irold.number_interregional_orders_lineitems_destiny,
            irolo.number_interregional_orders_lineitems_origin,
            rol.number_regional_orders_lineitems,
            inold.number_international_order_lineitems_destiny,
            inolo.number_international_order_lineitems_origin,
            nol.number_national_order_lineitems,
            (
                irold.number_interregional_orders_lineitems_destiny
                / nullif({{ total_lineitems }}, 0)
            )
            * 100 as percent_interregional_destiny,
            (
                irolo.number_interregional_orders_lineitems_origin
                / nullif({{ total_lineitems }}, 0)
            )
            * 100 as percent_interregional_origin,
            (rol.number_regional_orders_lineitems / nullif({{ total_lineitems }}, 0))
            * 100 as percent_regional,
            (
                inold.number_international_order_lineitems_destiny
                / nullif({{ total_lineitems }}, 0)
            )
            * 100 as percent_international_destiny,
            (
                inolo.number_international_order_lineitems_origin
                / nullif({{ total_lineitems }}, 0)
            )
            * 100 as percent_international_origin,
            (nol.number_national_order_lineitems / nullif({{ total_lineitems }}, 0))
            * 100 as percent_national,
            td.top_destination_nation,
            td.top_destination_region
        from {{ ref("dim_geography") }} geo
        left join
            top_destinations td
            on geo.nation = td.origin_nation
            and geo.region = td.origin_region
        join
            kpi_of_interregional_orders_lineitems_destiny irold
            on geo.region = irold.customer_region
        join
            kpi_of_interregional_orders_lineitems_origin irolo
            on geo.region = irolo.supplier_region
        join kpi_of_regional_orders_lineitems rol on geo.region = rol.region
        join
            kpi_of_international_order_lineitems_destiny inold
            on geo.nation = inold.customer_nation
        join
            kpi_of_international_order_lineitems_origin inolo
            on geo.nation = inolo.supplier_nation
        join number_of_national_order_lineitems nol on geo.nation = nol.nation
    )

select *
from orders_kpi_by_geography
order by region, nation
