WITH customer_age as
(
    SELECT
        customer_id,
        max(customer_age) as customer_age
    FROM {{ ref('orders') }}
    WHERE customer_age is not null
    GROUP BY 1
)

SELECT * FROM customer_age
