/*
  fct_orders
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Grain: one row per order_id

  This is a thin mart-layer promotion of int_orders_enriched.
  It exposes the fully enriched order record for:
    - ad-hoc order-level analysis
    - joining to dim_customers / dim_sellers in BI tools
    - input to aggregate mart models

  No new logic here. If you find yourself adding complex logic to this
  model, it belongs one layer down in int_orders_enriched.
*/

with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

-- bring in customer state for regional analysis
customers as (
    select
        customer_id,
        state       as customer_state,
        city        as customer_city
    from {{ ref('int_customers_enriched') }}
)

select
    o.order_id,
    o.customer_id,
    c.customer_state,
    c.customer_city,

    -- status & lifecycle
    o.order_status,
    o.purchased_at,
    o.approved_at,
    o.shipped_at,
    o.delivered_at,
    o.estimated_delivery_at,

    -- time dimensions for BI slicing
    o.order_month,
    o.order_week,
    o.purchase_day_of_week,
    o.purchase_hour,

    -- items
    o.item_count,
    o.distinct_products,
    o.distinct_sellers,

    -- revenue measures
    o.items_revenue,
    o.freight_revenue,
    o.gross_order_value,
    o.total_payment_value,

    -- payment
    o.primary_payment_type,
    o.max_installments,
    o.payment_methods_used,

    -- delivery performance
    o.days_to_deliver,
    o.is_late_delivery,

    -- satisfaction
    o.review_score,
    o.review_sentiment

from orders o
left join customers c on o.customer_id = c.customer_id
