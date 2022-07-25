{{ config(materialized='table') }}

with refunds as (
	select 
      order_id,
      count(r.id) as number_of_items_returned,
      sum(subtotal) as refunds,
      sum(total_tax) as tax_returned
    from {{ source('SHOPIFY', 'REFUND') }} r
    left join {{ source('SHOPIFY', 'ORDER_LINE_REFUND') }} olr on olr.refund_id = r.id
    group by 1
),

shipping as (

	select 
        order_id,
        sum(price) as shipping_charge,
        sum(discounted_price) as shipping_discount
     from {{ source('SHOPIFY', 'ORDER_SHIPPING_LINE') }}
     group by 1
)


select 

customer_id,
customer_email,
customer_created_at,
city,
province_code,
country_code,

min(o.created_at) as first_order_date,
max(o.created_at) as recent_order_date,

count(o.order_id) as total_orders,
sum(o.total_line_items_price) as gross_sales,
sum(- o.total_discount) as discounts,
sum(- nvl(r.refunds, 0)) as "returns",
gross_sales + discounts + "returns" AS net_sales,
sum(o.total_tax) - sum(nvl (r.tax_returned, 0)) AS taxes,
sum(nvl (s.shipping_charge, 0)) AS shipping_cost,
gross_sales + discounts + "returns" + taxes + shipping_cost AS total_sales

from {{ ref('orders') }} o 
left join refunds r on r.order_id = o.order_id
left join shipping s on s.order_id = o.order_id
group by 1,2,3,4,5,6



