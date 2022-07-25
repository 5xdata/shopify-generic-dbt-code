{{ config(materialized='table') }}

with order_line_tax as (
    select order_line_id
           ,sum(price) as total_line_tax
      from {{ source('SHOPIFY', 'TAX_LINE') }}
     group by 1
)
, order_line_discount as (
     select ol.order_id
            ,ol.id order_line_id
            ,sum(dal.amount) over(partition by ol.order_id,ol.index) as total_line_discount
            ,case when dap.type = 'discount_code'
                  then dap.code
                  else dap.title
              end as discount_title
            ,object_construct ('discount_title',discount_title,'discount_amount',dal.amount::decimal(5,2)) as discount_object
            ,arrayagg(discount_object) within group ( order by discount_title) over(partition by ol.order_id,ol.index) as line_discount_details
            ,arrayagg(distinct discount_title) within group ( order by discount_title) over(partition by ol.order_id) as order_discount_list
       from {{ source('SHOPIFY','ORDER_LINE') }} ol
      inner join {{ source('SHOPIFY','DISCOUNT_ALLOCATION') }} dal
         on ol.id = dal.order_line_id
      inner join {{ source('SHOPIFY','DISCOUNT_APPLICATION') }} dap
         on ol.order_id = dap.order_id
        and dal.discount_application_index = dap.index
    qualify row_number() over(partition by ol.id order by discount_title desc) = 1
)
, order_line_refunds as (
     select olr.order_line_id
            ,sum(olr.quantity) as refund_quantity
            ,sum(olr.subtotal) as refund_subtotal_amount
            ,sum(olr.total_tax) as refund_tax_amount
            ,refund_subtotal_amount + refund_tax_amount as refund_total_amount
       from {{ source('SHOPIFY','ORDER_LINE_REFUND') }} olr
      group by 1
)
, order_line_details as (
     select ol.order_id
            ,ol.id as order_line_id
            ,ol.index
            ,ol.product_id
            ,ol.variant_id
            ,ol.sku
            ,ol.name product_variant_name
            ,ol.title
            ,p.PRODUCT_TYPE
            ,ol.price as original_unit_price
            ,ol.quantity
            ,(original_unit_price * ol.quantity) as original_line_price
            ,round(old.total_line_discount,2) as total_line_discount
            ,round(olt.total_line_tax,2) as total_line_tax
            ,round((original_line_price - coalesce(old.total_line_discount,0) + coalesce(olt.total_line_tax,0)),2) as total_line_amount
            ,old.line_discount_details
            ,old.order_discount_list
            ,ol.fulfillable_quantity
            ,ol.fulfillment_status
            ,olr.refund_quantity
            ,olr.refund_subtotal_amount + olr.refund_tax_amount as total_refund_amount
       from {{ source('SHOPIFY','ORDER_LINE') }} ol
       left join {{ source('SHOPIFY','PRODUCT') }} p
         on ol.product_id = p.id
       left join order_line_tax olt
         on ol.id = olt.order_line_id
       left join order_line_discount old
         on ol.id = old.order_line_id
       left join order_line_refunds olr
         on ol.id = olr.order_line_id
)
select * from order_line_details