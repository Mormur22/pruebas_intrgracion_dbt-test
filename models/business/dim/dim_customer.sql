WITH 
customer AS (
    SELECT * 
    FROM {{ ref("trn_customer") }}
)


SELECT * 
FROM customer

