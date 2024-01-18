WITH 
fac_orders AS (
    SELECT * 
    FROM {{ ref("fact_orders") }}
),

customer_orders AS (
    SELECT
        CUSTOMER_KEY,
        MIN(ORDER_DATE) AS FIRST_ORDER_DATE,
        MAX(ORDER_DATE) AS MOST_RECENT_ORDER_DATE,
        COUNT(DISTINCT ORDER_KEY) AS NUMBER_OF_ORDERS,
        SUM(TOTAL_PRICE) AS LIFETIME_VALUE,
        SUM(
            CASE WHEN DATEDIFF(day, ORDER_DATE,'{{ env_var('DBT_CURRENT_DATE') }}') <= 30
            THEN TOTAL_PRICE ELSE 0 END
        ) AS MONEY_SPENT_LAST_MONTH
    FROM fac_orders
    GROUP BY CUSTOMER_KEY
),


OrderCounts AS (
    SELECT 
        CUSTOMER_KEY,
        SUPP_KEY,
        COUNT(DISTINCT ORDER_KEY) AS OrderCount
    FROM fac_orders
    GROUP BY CUSTOMER_KEY, SUPP_KEY
),

MaxOrderCounts AS (
    SELECT 
        CUSTOMER_KEY,
        SUPP_KEY AS MOST_ORDERED_SUPPLIER,
        MAX(OrderCount) AS MOST_ORDER_COUNT
    FROM OrderCounts
    GROUP BY CUSTOMER_KEY,SUPP_KEY
),

final AS (
    SELECT
        co.CUSTOMER_KEY,
        fo.CUSTOMER_NAME,
        fo.CUSTOMER_PHONE,
        fo.CUSTOMER_ACCOUNT_BALANCE,
        fo.MARKET_SEGMENT,
        fo.CUSTOMER_REGION,
        fo.CUSTOMER_NATION,
        co.FIRST_ORDER_DATE,
        co.MOST_RECENT_ORDER_DATE,
        co.NUMBER_OF_ORDERS,
        co.LIFETIME_VALUE,
        co.MONEY_SPENT_LAST_MONTH,
        moc.MOST_ORDERED_SUPPLIER,
        MAX(fo.SUPPLIER_NATION) AS SUPPLIER_NATION,
        MAX(fo.SUPPLIER_REGION) AS SUPPLIER_REGION,
        moc.MOST_ORDER_COUNT
    FROM customer_orders co
    JOIN fac_orders fo ON co.CUSTOMER_KEY = fo.CUSTOMER_KEY
    LEFT JOIN MaxOrderCounts moc ON co.CUSTOMER_KEY = moc.CUSTOMER_KEY
    GROUP BY 
        co.CUSTOMER_KEY, 
        fo.CUSTOMER_NAME, 
        fo.CUSTOMER_PHONE, 
        fo.CUSTOMER_ACCOUNT_BALANCE, 
        fo.MARKET_SEGMENT, 
        fo.CUSTOMER_REGION, 
        fo.CUSTOMER_NATION, 
        co.FIRST_ORDER_DATE, 
        co.MOST_RECENT_ORDER_DATE, 
        co.NUMBER_OF_ORDERS, 
        co.LIFETIME_VALUE, 
        co.MONEY_SPENT_LAST_MONTH, 
        moc.MOST_ORDERED_SUPPLIER, 
        moc.MOST_ORDER_COUNT
)

SELECT * 
FROM final
