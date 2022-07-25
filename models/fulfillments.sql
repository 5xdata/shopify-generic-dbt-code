{{ config(materialized='table') }}

select
osl.price,
o.SHIPPING_ADDRESS_CITY,
o.SHIPPING_ADDRESS_PROVINCE,
o.SHIPPING_ADDRESS_COUNTRY,
f.name,
TRACKING_COMPANY,
f.created_at::date as Fulfilment_Created_Date,
f.updated_at::date as Fulfilment_Last_Updated_Date,
f.order_id as ORDER_ID,
case when shipment_status = 'attempted_delivery' then 'Attempted Delivery'
     when shipment_status = 'label_printed' then 'Label Printed'
     when shipment_status = 'delivered' then 'Delivered'
     when shipment_status = 'out_for_delivery' then 'Out for Delivery'
     when shipment_status = 'confirmed' then 'Confirmed'
     when shipment_status = 'in_transit' then 'In Transit'
     when shipment_status = 'ready_for_pickup' then 'Ready for Pickup'
     when shipment_status = 'failure' then 'Failure'
     when shipment_status is null then 'N/A'
     else shipment_status end AS SHIPMENT_STATUS,
status AS "STATUS",
  o.fulfillment_status,
case when f.updated_at::date - f.created_at::date = 0 and f.updated_at = f.created_at then 'Instant/Manual'
     when f.updated_at::date - f.created_at::date = 0 and f.updated_at != f.created_at then 'Less than a day'
     when f.updated_at::date - f.created_at::date != 0 then 'More than a Day(s)'
     else null end as  Fullfilment_Day_Status,
f.updated_at::date - f.created_at::date as Fulfilment_Days
from {{ source('SHOPIFY', 'FULFILLMENT') }} f
   left join {{ source('SHOPIFY', 'ORDER') }} o on o.id = f.order_id
   left join {{ source('SHOPIFY', 'ORDER_SHIPPING_LINE') }} osl on osl.order_id = o.id
where f.status = 'success' 