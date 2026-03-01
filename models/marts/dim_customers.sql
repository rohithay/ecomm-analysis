/*
  dim_customers
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Grain: one row per customer_id

  Descriptive dimension for filtering and segmentation.
  All metrics already computed in int_customers_enriched.
*/

select
    customer_id,
    customer_unique_id,
    state,
    city,
    customer_lat,
    customer_lng,

    -- segments
    case
        when total_orders = 0 then 'no_orders'
        when total_orders = 1 then 'one_time'
        when total_orders between 2 and 3 then 'occasional'
        else 'loyal'
    end                             as customer_segment,

    is_repeat_buyer,

    -- lifetime value buckets
    case
        when lifetime_gmv = 0      then 'no_spend'
        when lifetime_gmv < 100    then 'low'
        when lifetime_gmv < 500    then 'mid'
        when lifetime_gmv < 2000   then 'high'
        else 'vip'
    end                             as ltv_bucket,

    -- raw metrics (useful for BI joins)
    total_orders,
    delivered_orders,
    lifetime_gmv,
    lifetime_freight,
    avg_order_value,
    avg_freight_pct,
    first_order_at,
    last_order_at,
    customer_lifespan_days,
    avg_days_to_deliver,
    late_orders,
    late_order_rate,
    avg_review_score,
    most_used_payment_type

from {{ ref('int_customers_enriched') }}
