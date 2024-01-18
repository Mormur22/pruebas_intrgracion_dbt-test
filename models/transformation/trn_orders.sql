WITH 
stg_orders AS (
    SELECT * FROM {{ ref('stg_tpch_sf1__orders') }}
),

order_status_ok AS (
    SELECT 
        O_ORDERKEY AS sts_key, 
        COUNT(DISTINCT LINE_STATUS) AS ok_status
    FROM stg_orders
    JOIN {{ ref('trn_lineitem') }} ON ORDER_KEY = O_ORDERKEY
    GROUP BY O_ORDERKEY
),

trn_orders AS (
    SELECT  
        O_ORDERKEY AS order_key,
        O_CUSTKEY AS customer_key,
        O_ORDERSTATUS AS order_status,
        TO_BOOLEAN(stat.ok_status) AS ok_status,
        O_TOTALPRICE AS total_price,
        O_ORDERDATE AS order_date,
        CASE 
            WHEN UPPER(SPLIT_PART(O_ORDERPRIORITY, '-', 2)) = 'LOW' THEN 1
            WHEN UPPER(SPLIT_PART(O_ORDERPRIORITY, '-', 2)) = 'MEDIUM' THEN 2
            WHEN UPPER(SPLIT_PART(O_ORDERPRIORITY, '-', 2)) = 'HIGH' THEN 3
            ELSE NULL
        END AS order_priority,
        REPLACE(O_CLERK, 'Clerk#', '') AS clerk,
        O_SHIPPRIORITY AS ship_priority,
        ETL_TIMESTAMP
    FROM stg_orders AS stg 
    JOIN order_status_ok AS stat ON stg.O_ORDERKEY = stat.sts_key
)

SELECT
    *
FROM trn_orders
