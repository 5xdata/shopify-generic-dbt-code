
with orders as  (
        select * from {{ ref('shopify_analytics') }}
),

--Average order value (AOV) gives you a sense of how much customers spend each time they purchase.

customers_aov as (
        select
                customer_email,
                net_sales,
                total_orders,
                round(net_sales/total_orders,2) as average_order_value
        from orders
        where net_sales is not null and total_orders > 0 order by 4 desc
)

select * from customers_aov
