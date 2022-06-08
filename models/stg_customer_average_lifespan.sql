
--Customer lifespan is how long, on average, customers purchase from you before going dormant.
with average_customer_lifespan as (
	select 
		country_code,
		avg(round(datediff('day', first_order_date, recent_order_date)::FLOAT / (365::FLOAT / 12)))
	from {{ ref('shopify_analytics') }}
	where total_orders > 1
	group by 1 
)

select * from average_customer_lifespan
