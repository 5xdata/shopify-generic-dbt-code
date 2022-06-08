
with customer_ltv as (
	select 
		round(sum(net_sales) / sum(total_orders),2) aov,
		round(avg(datediff('day', first_order_date, recent_order_date)::FLOAT / (365::FLOAT / 12)),2) avg_customer_lifespan,
		round(sum(total_orders::FLOAT) / avg_customer_lifespan,2) purchase_frequency,
		round(aov * avg_customer_lifespan * purchase_frequency,2) ltv,
		sum(net_sales) / count(customer_email) easy_ltv
	from 
	{{ ref('shopify_analytics') }}
	where  total_orders > 1 AND datediff('day', first_order_date, recent_order_date)::FLOAT / (365::FLOAT / 12) > 0
	
)

select * from customer_ltv



