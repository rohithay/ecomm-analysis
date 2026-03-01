/*
  mart_delivery_performance
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Grain: one row per (customer_state, seller_id)

  Answers:
    - What % of orders are delivered on time vs. late?
    - Which states have the worst delivery performance?
    - Is there a correlation between delivery delay and review score?
    - Which sellers consistently ship late?

  Note: grain is (customer_state, seller_id) so you can slice by either
  state alone (GROUP BY customer_state in BI) or seller alone
  (GROUP BY seller_id). This avoids building two separate aggregate tables.
*/

with orders as (
    select
        o.order_id,
        o.customer_state,
        o.days_to_deliver,
        o.is_late_delivery,
        o.review_score,
        o.review_sentiment,
        o.order_status,
        o.order_month,
        -- how many days late (positive = late, negative = early)
        case
            when o.delivered_at is not null
             and o.estimated_delivery_at is not null
            then datediff('day',
                    o.estimated_delivery_at,
                    o.delivered_at)
            else null
        end                             as days_late
    from {{ ref('fct_orders') }} o
    where o.order_status = 'delivered'
),

-- bring in seller from order items
-- one order can have multiple sellers; we fan out here intentionally
order_sellers as (
    select distinct
        order_id,
        seller_id
    from {{ ref('stg_order_items') }}
),

base as (
    select
        o.customer_state,
        os.seller_id,
        o.order_month,
        o.days_to_deliver,
        o.is_late_delivery,
        o.review_score,
        o.review_sentiment,
        o.days_late
    from orders o
    inner join order_sellers os on o.order_id = os.order_id
),

final as (
    select
        customer_state,
        seller_id,

        -- volume
        count(*)                                        as delivered_orders,

        -- on-time vs late
        sum(case when is_late_delivery then 0 else 1 end) as on_time_orders,
        sum(case when is_late_delivery then 1 else 0 end) as late_orders,
        sum(case when is_late_delivery then 1 else 0 end)::float
            / nullif(count(*), 0)                       as late_delivery_rate,

        -- delay magnitude
        avg(days_late)                                  as avg_days_late,
        avg(case when is_late_delivery
            then days_late end)                         as avg_days_late_when_late,
        avg(days_to_deliver)                            as avg_days_to_deliver,

        -- review correlation with delivery
        avg(review_score)                               as avg_review_score,
        avg(case when is_late_delivery
            then review_score end)                      as avg_review_score_when_late,
        avg(case when not is_late_delivery
            then review_score end)                      as avg_review_score_when_on_time,

        -- sentiment
        sum(case when review_sentiment = 'positive'
            then 1 else 0 end)                          as positive_reviews,
        sum(case when review_sentiment = 'negative'
            then 1 else 0 end)                          as negative_reviews,

        -- monthly breakdown for trending
        count(distinct order_month)                     as active_months

    from base
    group by 1, 2
)

select * from final
order by late_delivery_rate desc
