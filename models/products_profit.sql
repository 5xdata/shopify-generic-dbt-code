select ol.title,
ol.PRODUCT_TYPE, coalesce(product_price, 0) as product_price,
QUANTITY,
ORIGINAL_UNIT_PRICE,
TOTAL_LINE_DISCOUNT,
TOTAL_LINE_TAX,
TOTAL_LINE_AMOUNT,
TOTAL_REFUND_AMOUNT
from
(select title, PRODUCT_TYPE,
sum(coalesce(ORIGINAL_UNIT_PRICE, 0)) as ORIGINAL_UNIT_PRICE,
sum(coalesce(TOTAL_LINE_DISCOUNT, 0)) as TOTAL_LINE_DISCOUNT,
sum(coalesce(TOTAL_LINE_TAX, 0)) as TOTAL_LINE_TAX, 
sum(coalesce(TOTAL_LINE_AMOUNT, 0)) TOTAL_LINE_AMOUNT,
sum(coalesce(TOTAL_REFUND_AMOUNT, 0)) as TOTAL_REFUND_AMOUNT,
sum(coalesce(QUANTITY, 0)) as QUANTITY
from {{ ref('order_lines') }} group by 1,2)ol
left join (
select distinct p.title, p.product_type, pv.Price as product_price
from 
{{ source('SHOPIFY', 'PRODUCT') }} p
inner join {{ source('SHOPIFY', 'PRODUCT_VARIANT') }} pv on pv.product_id = p.id
)p on p.title = ol.title and p.product_type = ol.PRODUCT_TYPE 
order by 1 