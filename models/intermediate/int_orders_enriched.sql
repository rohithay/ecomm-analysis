/*
  int_orders_enriched
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  One row per order with all order-level aggregates:
    • item counts and revenue totals from order_items
    • payment totals and primary payment method
    • review score (if available)
*/

with orders as (
    select * from {{ ref('stg_orders') }}
),

items_agg as (
    select
        order_id,
        count(*)                    as item_count,
        count(distinct product_id)  as distinct_products,
        count(distinct seller_id)   as distinct_sellers,
        sum(item_price)             as items_revenue,
        sum(freight_value)          as freight_revenue,
        sum(total_item_value)       as gross_order_value
    from {{ ref('stg_order_items') }}
    group by 1
),

payments_agg as (
    select
        order_id,
        sum(payment_value)          as total_payment_value,
        max(installments)           as max_installments,
        -- Primary payment method = highest value payment type
        first(payment_type order by payment_value desc) as primary_payment_type,
        count(distinct payment_type) as payment_methods_used
    from {{ ref('stg_order_payments') }}
    group by 1
),

reviews as (
    select
        order_id,
        review_score,
        sentiment
    from {{ ref('stg_order_reviews') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.purchased_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_at,
        o.days_to_deliver,
        o.is_late_delivery,

        -- Items
        coalesce(i.item_count, 0)           as item_count,
        coalesce(i.distinct_products, 0)    as distinct_products,
        coalesce(i.distinct_sellers, 0)     as distinct_sellers,
        coalesce(i.items_revenue, 0)        as items_revenue,
        coalesce(i.freight_revenue, 0)      as freight_revenue,
        coalesce(i.gross_order_value, 0)    as gross_order_value,

        -- Payments
        coalesce(p.total_payment_value, 0)  as total_payment_value,
        p.primary_payment_type,
        coalesce(p.max_installments, 1)     as max_installments,
        coalesce(p.payment_methods_used, 0) as payment_methods_used,

        -- Reviews
        r.review_score,
        r.sentiment                         as review_sentiment,

        -- Derived
        date_trunc('month', o.purchased_at) as order_month,
        date_trunc('week',  o.purchased_at) as order_week,
        dayofweek(o.purchased_at)           as purchase_day_of_week,
        hour(o.purchased_at)                as purchase_hour

    from orders o
    left join items_agg    i on o.order_id = i.order_id
    left join payments_agg p on o.order_id = p.order_id
    left join reviews      r on o.order_id = r.order_id
)

select * from final
