with
customer_list AS (
    SELECT DISTINCT customer_key
    FROM {{ref('fact_orders')}}
),
cross_join AS (
    SELECT
        cl.customer_key,
        ud.year,
        ud.month
    FROM customer_list cl
    CROSS JOIN {{ref('dim_time')}} ud
),
monthly_spending AS (
    SELECT
        cj.customer_key,
        cj.year,
        cj.month,
        COALESCE(SUM(fo.total_price), 0) AS monthly_spending
    FROM cross_join cj
    LEFT JOIN {{ref('fact_orders')}} fo
        ON cj.customer_key = fo.customer_key
        AND YEAR(fo.order_date) = cj.year
        AND MONTH(fo.order_date) = cj.month
    GROUP BY cj.customer_key, cj.year, cj.month
)

SELECT
    ms.customer_key,
    ms.year,
    ms.month,
    ms.monthly_spending
FROM monthly_spending ms
ORDER BY ms.customer_key, ms.year, ms.month