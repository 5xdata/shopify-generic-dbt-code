{{ config(materialized='table') }}

with order_tag as (
    select order_id
           ,"VALUE" as order_tag_value
      from {{ source('SHOPIFY','ORDER_TAG') }}
     where "VALUE" in ('Subscription First Order','Subscription Recurring Order')
)
, order_transaction as (
    select order_id
           ,sum(case when kind = 'refund' then amount else 0 end) as total_refunded
           ,sum(case when kind in ('sale','capture') then amount else 0 end) as total_paid
      from {{ source('SHOPIFY','TRANSACTION') }}
     where status = 'success'
     group by 1
)
, order_line_bottom_up as (
    select ol.order_id
           ,ol.order_discount_list
           ,count(distinct sku) as distinct_sku_ordered
           ,sum(quantity) as total_quantity_ordered
           ,sum(original_line_price) as line_total_wo_discount
           ,sum(total_line_tax) as line_total_tax
           ,sum(total_line_discount) as line_total_discount
           ,sum(total_line_amount) as line_total_w_discount 
           ,sum(refund_quantity) as total_refund_quantity
           ,sum(total_refund_amount) as total_refund_amount
      from {{ ref('order_lines') }} ol
     group by 1,2
)
, order_shipping as (
    select osl.order_id
           ,coalesce(dap.description,osl.title) as shipping_description
           ,osl.discounted_price as shipping_charged
      from {{ source('SHOPIFY','ORDER_SHIPPING_LINE') }}  osl
      left join {{ source('SHOPIFY','DISCOUNT_APPLICATION') }}  dap
        on osl.order_id = dap.order_id
       and dap.target_type = 'shipping_line'
),
customer_address as (
    select 
        customer_id, city, province_code, country_code
    from {{ source ('SHOPIFY', 'CUSTOMER_ADDRESS') }} 
    where is_default = true
)

, order_details as (
    select o.id as order_id
           ,o.name as order_num
           ,lower(o.email) as customer_email
           ,o.customer_id
           ,o.created_at
           ,round(o.subtotal_price,2) as subtotal_price
           ,round(o.total_tax,2) as total_tax
           ,os.shipping_charged
           ,round(o.total_price,2) as total_price
           ,round(otrn.total_paid,2) as total_paid
           ,round(otrn.total_refunded,2) as total_refunded
           ,olbu.distinct_sku_ordered
           ,olbu.total_quantity_ordered
           ,round(olbu.line_total_wo_discount,2) as subtotal_wo_discount
           ,round(olbu.line_total_discount,2) as total_discount
           ,round(olbu.line_total_w_discount,2) as subtotal_w_discount
           ,olbu.order_discount_list
           ,olbu.total_refund_quantity
           ,os.shipping_description as shipping_code
           ,o.financial_status
           ,o.fulfillment_status
           ,o.PROCESSING_METHOD
           ,o.source_name
           ,case when ot.order_tag_value is not null
                 then ot.order_tag_value
                 when o.source_name = 'web'
                 then 'one time online purchase'
                 when o.source_name = 'shopify_draft_order'
                 then 'shopify draft orders'
                 when o.source_name in ('2376822','iphone')
                 then 'ordered for influencer marketing'
                 else 'manual processing'
             end as order_type
           ,o.ORDER_STATUS_URL
           ,o.landing_site_ref
           ,o.landing_site_base_url
           ,order_utm.utm_source
           ,order_utm.utm_medium
           ,order_utm.utm_campaign
           ,order_utm.utm_term
           ,order_utm.utm_content
           ,o.note
           ,o.NOTE_ATTRIBUTES
           ,o.SHIPPING_ADDRESS_CITY
           ,o.SHIPPING_ADDRESS_ZIP
           ,o.SHIPPING_ADDRESS_PROVINCE_CODE
           ,o.SHIPPING_ADDRESS_COUNTRY_CODE
           ,cast(o.SHIPPING_ADDRESS_LATITUDE as decimal(11,8)) as SHIPPING_ADDRESS_LATITUDE
           ,cast(o.SHIPPING_ADDRESS_LONGITUDE as decimal(11,8)) as SHIPPING_ADDRESS_LONGITUDE
           ,o.BILLING_ADDRESS_CITY
           ,o.BILLING_ADDRESS_ZIP
           ,o.BILLING_ADDRESS_PROVINCE_CODE
           ,o.BILLING_ADDRESS_COUNTRY_CODE
           ,cast(o.BILLING_ADDRESS_LATITUDE as decimal(11,8)) as BILLING_ADDRESS_LATITUDE
           ,cast(o.BILLING_ADDRESS_LONGITUDE as decimal(11,8)) as BILLING_ADDRESS_LONGITUDE
           ,o.total_line_items_price
           ,c.created_at as customer_created_at
           ,upper(c_add.city) city
           ,c_add.province_code
           ,c_add.country_code
      from {{ source('SHOPIFY','ORDER') }} o
      left join order_tag ot
        on o.id = ot.order_id
      left join order_transaction otrn
        on o.id = otrn.order_id
      left join order_shipping os
        on o.id = os.order_id
      left join order_line_bottom_up olbu
        on o.id = olbu.order_id
      left join {{ ref('stg_order_utm') }} as order_utm
        on o.id = order_utm.order_id
      left join {{ source('SHOPIFY','CUSTOMER') }} c 
        on c.id = o.customer_id
      left join customer_address c_add
        on c_add.customer_id = o.customer_id
)
select * from order_details