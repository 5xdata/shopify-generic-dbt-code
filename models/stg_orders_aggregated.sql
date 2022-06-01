WITH orders as(
    select * from {{ source('SHOPIFY', 'ORDER') }}
),

transactions as
(
SELECT
    *
FROM
  {{ source('SHOPIFY', 'TRANSACTION') }}
WHERE
    lower(status) = 'success'
),

orders_aggregated as
(
SELECT
    o.customer_id,
    min(o.created_at) as first_order_timestamp,
    max(o.created_at) as most_recent_order_timestamp,
    avg(case when lower(t.kind) in ('sale','capture') then t.amount end) as average_order_value,
    sum(case when lower(t.kind) in ('sale','capture') then t.amount end) as lifetime_total_spent,
    sum(case when lower(t.kind) in ('refund') then t.amount end) as lifetime_total_refunded,
    count(distinct o.id) as lifetime_count_orders
FROM
    orders o left join
    transactions t on o.id = t.order_id
WHERE
    customer_id is not null
GROUP BY
    1
)

select * from orders_aggregated
