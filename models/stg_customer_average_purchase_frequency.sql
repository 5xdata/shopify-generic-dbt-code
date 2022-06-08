

with orders as  (
        select * from {{ ref('shopify_analytics') }}
),

--Purchase frequency tells you how often your customers buy from you.

customer_purchase_frequency as (
        select 
                customer_email,total_orders,
                round(datediff('day', first_order_date, recent_order_date)::FLOAT / (365::FLOAT / 12)) customer_lifespan,
                round(total_orders::float/customer_lifespan,1) avg_purchase_frequency
                from 
        orders where total_orders > 1 and customer_lifespan > 0
        order by 4 desc
)

select * from customer_purchase_frequency