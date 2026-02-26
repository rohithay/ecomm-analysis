/*
  dim_customers
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Customer dimension for BI/analytics consumption.
  Grain: one row per customer_unique_id.
*/

with customers as (
    select * from {{ ref('stg_customers') }}
),

history as (
    select * from {{ ref('int_customer_order_history') }}
),

-- Most recent customer record for the unique customer
latest_customer as (
    select *,
        row_number() over (
            partition by customer_unique_id
            order by customer_id desc  -- deterministic tie-break
        ) as rn
    from customers
),

final as (
    select
        lc.customer_unique_id                               as customer_id,
        lc.city,
        lc.state,
        lc.zip_code_prefix,

        -- Lifetime metrics
        coalesce(h.total_orders,            0)              as total_orders,
        coalesce(h.total_items_purchased,   0)              as total_items_purchased,
        coalesce(h.lifetime_value,          0.0)            as lifetime_value,
        coalesce(h.avg_order_value,         0.0)            as avg_order_value,
        h.avg_review_score,
        h.avg_days_to_deliver,

        -- Temporal
        h.first_order_at,
        h.last_order_at,
        h.cohort_month,
        coalesce(h.customer_segment, 'no_orders')           as customer_segment,

        -- Days since last order (relative to dataset end 2018-12-31)
        datediff('day', h.last_order_at,
            cast('2018-12-31' as timestamp))                as days_since_last_order

    from latest_customer lc
    left join history h on lc.customer_unique_id = h.customer_unique_id
    where lc.rn = 1
)

select * from final
