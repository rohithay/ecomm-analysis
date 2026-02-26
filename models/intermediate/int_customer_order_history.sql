/*
  int_customer_order_history
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  One row per customer_unique_id with lifetime metrics.
  Uses customer_unique_id (not customer_id) to correctly
  capture repeat buyers.
*/

with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

-- Join to get unique customer id
order_customer as (
    select
        c.customer_unique_id,
        o.*
    from orders o
    join customers c on o.customer_id = c.customer_id
    where o.order_status not in ('canceled', 'unavailable')
),

agg as (
    select
        customer_unique_id,
        count(distinct order_id)            as total_orders,
        sum(gross_order_value)              as lifetime_value,
        avg(gross_order_value)              as avg_order_value,
        min(purchased_at)                   as first_order_at,
        max(purchased_at)                   as last_order_at,
        avg(review_score)                   as avg_review_score,
        avg(days_to_deliver)                as avg_days_to_deliver,
        sum(item_count)                     as total_items_purchased,

        -- Cohort month (first purchase)
        date_trunc('month', min(purchased_at)) as cohort_month,

        -- Customer type
        case
            when count(distinct order_id) = 1 then 'one_time'
            when count(distinct order_id) between 2 and 3 then 'repeat'
            else 'loyal'
        end as customer_segment

    from order_customer
    group by 1
)

select * from agg
