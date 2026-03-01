/*
  dim_sellers
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Grain: one row per seller_id

  Used to filter/slice by seller attributes in BI tools
  and as a target for mart_seller_health.
*/

select
    seller_id,
    state,
    city,
    seller_lat,
    seller_lng,

    -- performance tier
    case
        when total_revenue = 0          then 'inactive'
        when total_revenue < 5000       then 'small'
        when total_revenue < 50000      then 'mid'
        else 'large'
    end                                 as seller_tier,

    -- quality tier
    case
        when avg_review_score >= 4.5    then 'excellent'
        when avg_review_score >= 4.0    then 'good'
        when avg_review_score >= 3.0    then 'average'
        when avg_review_score is null   then 'unrated'
        else 'poor'
    end                                 as quality_tier,

    is_high_revenue_low_rating          as is_risk_seller,

    -- raw metrics
    total_orders,
    total_items_sold,
    distinct_products,
    total_revenue,
    total_freight_collected,
    avg_item_price,
    avg_days_to_deliver,
    late_delivery_rate,
    avg_review_score,
    one_star_reviews,
    five_star_reviews,
    one_star_rate

from {{ ref('int_sellers_enriched') }}
