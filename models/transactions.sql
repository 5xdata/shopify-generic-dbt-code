
select 
    t.id, t.user_id, t.order_id, t.refund_id, amount, t.created_at, device_id, gateway, source_name,
    message, currency, PAYMENT_CREDIT_CARD_COMPANY, kind, status,
    receipt:error_code::text as error_code, note
from {{ source('SHOPIFY', 'TRANSACTION') }} t
left join {{ source('SHOPIFY', 'REFUND') }} r on r.id = t.refund_id