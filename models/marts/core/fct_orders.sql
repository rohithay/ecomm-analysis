/*
  fct_orders
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Core fact table. One row per order.
  Grain: order_id

  This is the primary table for revenue and operations analysis.
*/

with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

customers as (
    select
        customer_id,
        customer_unique_id,
        city    as customer_city,
        state   as customer_state
    from {{ ref('stg_customers') }}
),

final as (
    select
        -- Keys
        o.order_id,
        o.customer_id,
        c.customer_unique_id,

        -- Dimensions
        o.order_status,
        o.primary_payment_type,
        o.order_month,
        o.order_week,
        o.purchase_day_of_week,
        o.purchase_hour,
        c.customer_city,
        c.customer_state,

        -- Timestamps
        o.purchased_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_at,

        -- Delivery metrics
        o.days_to_deliver,
        o.is_late_delivery,
        case
            when o.is_late_delivery < 0  then 'early'
            when o.is_late_delivery = 0  then 'on_time'
            when o.is_late_delivery <= 3 then 'slightly_late'
            else 'late'
        end as delivery_timeliness,

        -- Order metrics
        o.item_count,
        o.distinct_products,
        o.distinct_sellers,
        o.items_revenue,
        o.freight_revenue,
        o.gross_order_value,
        o.total_payment_value,
        o.max_installments,
        o.payment_methods_used,

        -- Review
        o.review_score,
        o.review_sentiment,

        -- Boolean flags (useful for pivot/filter)
        (o.order_status = 'delivered')                          as is_delivered,
        (o.order_status = 'canceled')                           as is_canceled,
        (o.review_score is not null)                            as has_review,
        (o.item_count > 1)                                      as is_multi_item,
        (o.distinct_sellers > 1)                                as is_multi_seller,
        (o.is_late_delivery > 0)                       as was_late

    from orders o
    left join customers c on o.customer_id = c.customer_id
)

select * from final
