
with customer_value as (
	SELECT total_orders,
		count(DISTINCT customer_id) distinct_customers,
		avg(net_sales::FLOAT / total_orders) aov,
		sum(net_sales::FLOAT) / distinct_customers customer_value
	FROM {{ ref('shopify_analytics') }}
	WHERE total_orders > 0 
	GROUP BY 1 having count(DISTINCT customer_id) > 0
	ORDER BY 1 
)


select * from customer_value