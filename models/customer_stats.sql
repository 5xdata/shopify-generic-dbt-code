{{ config(materialized='table') }}

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
        customer_province
    FROM
        orders o 
        left join customers c on c.id = o.customer_id
        left join customer_address ca on c.id = ca.customer_id
)

SELECT * from customer_stats