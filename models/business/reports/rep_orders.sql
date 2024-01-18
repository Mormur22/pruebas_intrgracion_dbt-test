WITH 
fact_orders AS (
    SELECT * FROM {{ref('fact_orders')}}
),

orders_total_kpi AS (
    SELECT 
        order_priority,
        COUNT(*) AS total_number_of_orders,
        AVG(quantity) AS average_number_of_orders
    FROM fact_orders
    GROUP BY order_priority
),


orders_kpi_order_priority AS (
    SELECT 
        order_priority,
        COUNT(*) AS num_order_priority,
        AVG(CAST(order_priority AS DECIMAL)) AS avg_order_priority -- Aquí está el intento de castear una columna de texto a decimal. Necesitamos reconsiderar esto
    FROM fact_orders
    GROUP BY order_priority
),

num_lineorder_by_order AS (
    SELECT
        order_priority,
        order_key AS ok,
        MAX(LINE_NUMBER) AS max_line_number
    FROM fact_orders
    GROUP BY order_priority, order_key
),

avg_num_of_lineitems AS (
    SELECT 
        order_priority,
        AVG(max_line_number) AS avg_lineitems_by_order
    FROM num_lineorder_by_order
    GROUP BY order_priority
)

SELECT 
    ot.order_priority,
    ot.total_number_of_orders,
    ot.average_number_of_orders,
    op.num_order_priority,
    op.avg_order_priority, -- Necesitamos reconsiderar este campo
    an.avg_lineitems_by_order
FROM orders_total_kpi ot
JOIN orders_kpi_order_priority op ON ot.order_priority = op.order_priority
JOIN avg_num_of_lineitems an ON ot.order_priority = an.order_priority
