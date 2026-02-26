/*
  agg_daily_revenue
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Daily revenue snapshot for time-series dashboards.
  Grain: one row per calendar date.
*/

with orders as (
    select *
    from {{ ref('fct_orders') }}
    where is_delivered = true  -- Only count revenue on delivered orders
),

daily as (
    select
        cast(purchased_at as date)          as order_date,
        count(order_id)                     as total_orders,
        count(distinct customer_unique_id)  as unique_customers,
        sum(gross_order_value)              as gross_revenue,
        sum(freight_revenue)                as freight_revenue,
        sum(items_revenue)                  as product_revenue,
        avg(gross_order_value)              as avg_order_value,
        sum(item_count)                     as total_items_sold,
        avg(review_score)                   as avg_review_score,

        -- Delivery quality
        avg(days_to_deliver)                as avg_days_to_deliver,
        sum(case when was_late then 1 else 0 end)   as late_deliveries,
        sum(case when is_canceled then 1 else 0 end) as cancellations

    from orders
    group by 1
),

with_moving_avg as (
    select
        *,
        -- 7-day rolling averages
        avg(gross_revenue) over (
            order by order_date
            rows between 6 preceding and current row
        ) as revenue_7d_ma,
        avg(total_orders) over (
            order by order_date
            rows between 6 preceding and current row
        ) as orders_7d_ma,

        -- Week-over-week revenue change
        lag(gross_revenue, 7) over (order by order_date) as revenue_7d_ago,
        round(
            (gross_revenue - lag(gross_revenue, 7) over (order by order_date))
            / nullif(lag(gross_revenue, 7) over (order by order_date), 0)
            * 100, 2
        ) as wow_revenue_pct_change

    from daily
)

select * from with_moving_avg
order by order_date
