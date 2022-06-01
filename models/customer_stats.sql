
WITH customers as
(
    SELECT * from {{ source('SHOPIFY', 'CUSTOMER') }}
)
, customer_address as (
    SELECT * FROM {{ ref('stg_customer_address') }}
)
, orders as (
    SELECT * from {{ ref('stg_orders_aggregated') }}
)
, customer_age as (
    SELECT * FROM {{ ref('stg_customer_age') }}
)
, customer_stats as (
    SELECT 
        o.customer_id as customer_id,
        c.first_name as first_name,
        c.last_name as last_name,
        c.email as email,
        c.created_at as created_at,
        o.first_order_timestamp,
        o.most_recent_order_timestamp,
        coalesce(o.average_order_value, 0) as average_order_value,
        coalesce(o.lifetime_total_spent, 0) as lifetime_total_spent,
        coalesce(o.lifetime_total_refunded, 0) as lifetime_total_refunded,
        (coalesce(o.lifetime_total_spent, 0) - coalesce(o.lifetime_total_refunded, 0)) as lifetime_total_amount,
        coalesce(o.lifetime_count_orders, 0) as lifetime_count_orders,        
        customer_province,
        customer_age,
        CASE
            WHEN (customer_age < 20 ) then 'Less than 20'
            WHEN (customer_age >= 20 and customer_age < 25 ) then '20 to 25'
            WHEN (customer_age >= 25 and customer_age < 30 ) then '25 to 30'
            WHEN (customer_age >= 30 and customer_age < 35 ) then '30 to 35'
            WHEN (customer_age >= 35 and customer_age < 40 ) then '35 to 40'
            WHEN (customer_age >= 40 and customer_age < 45 ) then '40 to 45'
            WHEN (customer_age >= 45 and customer_age < 50 ) then '45 to 50'
            WHEN (customer_age >= 50 and customer_age < 55 ) then '50 to 55'
            WHEN (customer_age >= 55 and customer_age < 60 ) then '55 to 60'
            WHEN (customer_age >= 55 and customer_age < 60 ) then '55 to 60'
            WHEN (customer_age >= 60 ) then '60 and above'
            ELSE 'Unknown'
        END as customer_age_bucket
    FROM
        orders o 
        left join customers c on c.id = o.customer_id
        left join customer_address ca on c.id = ca.customer_id
        left join customer_age cage on c.id = cage.customer_id
)

SELECT * from customer_stats