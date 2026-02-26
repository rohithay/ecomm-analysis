/*
  agg_seller_performance
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Seller scorecard for marketplace health monitoring.
  Grain: one row per seller_id.
*/

with items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

sellers as (
    select * from {{ ref('stg_sellers') }}
),

-- Join items to order metadata
items_with_order as (
    select
        i.seller_id,
        i.order_id,
        i.item_price,
        i.freight_value,
        i.total_item_value,
        o.purchased_at,
        o.days_to_deliver,
        o.delivery_days_vs_estimate,
        o.review_score,
        o.review_sentiment,
        o.is_delivered,
        o.is_canceled,
        o.was_late
    from items i
    join orders o on i.order_id = o.order_id
),

seller_agg as (
    select
        seller_id,
        count(distinct order_id)                        as total_orders,
        count(*)                                        as total_items_sold,
        count(distinct date_trunc('month', purchased_at)) as active_months,
        sum(item_price)                                 as total_revenue,
        avg(item_price)                                 as avg_item_price,
        sum(freight_value)                              as total_freight_collected,

        -- Quality
        avg(review_score)                               as avg_review_score,
        sum(case when review_score >= 4 then 1 else 0 end) as positive_reviews,
        sum(case when review_score <= 2 then 1 else 0 end) as negative_reviews,
        sum(case when was_late        then 1 else 0 end) as late_deliveries,
        sum(case when is_canceled     then 1 else 0 end) as canceled_orders,
        sum(case when is_delivered    then 1 else 0 end) as delivered_orders,

        -- Delivery speed
        avg(days_to_deliver)                            as avg_days_to_deliver,
        avg(delivery_days_vs_estimate)                  as avg_days_vs_estimate,

        -- Time range
        min(purchased_at)                               as first_sale_at,
        max(purchased_at)                               as last_sale_at

    from items_with_order
    group by 1
),

final as (
    select
        a.*,
        s.city          as seller_city,
        s.state         as seller_state,

        -- Derived rates
        round(a.canceled_orders  / nullif(a.total_orders, 0) * 100, 2) as cancellation_rate_pct,
        round(a.late_deliveries  / nullif(a.total_orders, 0) * 100, 2) as late_rate_pct,
        round(a.negative_reviews / nullif(a.total_orders, 0) * 100, 2) as negative_review_rate_pct,

        -- Seller tier
        case
            when a.total_revenue >= 50000 and a.avg_review_score >= 4.0  then 'platinum'
            when a.total_revenue >= 10000 and a.avg_review_score >= 3.5  then 'gold'
            when a.total_revenue >= 1000  and a.avg_review_score >= 3.0  then 'silver'
            else 'bronze'
        end as seller_tier

    from seller_agg a
    join sellers s on a.seller_id = s.seller_id
)

select * from final
order by total_revenue desc
