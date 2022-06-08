
WITH orders as(
    select * from {{ source('SHOPIFY', 'ORDER') }}
)
, customer_address_initial as (
    SELECT
        customer_id,
        billing_address_province as customer_province,
        count(*) as no_of_orders,
        sum(subtotal_price) as total_spend,
        RANK() OVER (PARTITION BY customer_id order by no_of_orders desc) as orders_rank,
        RANK() OVER (PARTITION BY customer_id order by total_spend desc) as spend_rank
      FROM orders GROUP BY  1,2
)
, customer_address_intermediate as (
    SELECT
        customer_id,
        customer_province,
        no_of_orders,
        total_spend,
        orders_rank
        spend_rank
    FROM customer_address_initial
    WHERE orders_rank = 1
    ORDER BY total_spend desc
)
, customer_address_intermediate_2 as (
    SELECT
        customer_id,
        customer_province,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total_spend DESC) as row_number_by_spend
    FROM
        customer_address_intermediate
)
, customer_address as (
    SELECT
        customer_id,
        customer_province
    FROM
        customer_address_intermediate_2
    WHERE
        row_number_by_spend = 1
)

SELECT * from customer_address