/*
  mart_seller_health
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Grain: one row per seller_id

  Answers:
    • Who are the top 10 sellers by revenue?
    • Which sellers have high revenue but low review scores (risk flag)?
    • Are sellers geographically concentrated?

  This is essentially a wide, BI-ready version of dim_sellers
  with pre-computed rankings and risk flags added.
*/

with sellers as (
    select * from {{ ref('dim_sellers') }}
),

-- rank sellers by revenue globally
ranked as (
    select
        *,
        row_number() over (
            order by total_revenue desc
        )                               as revenue_rank,

        row_number() over (
            order by avg_review_score desc nulls last
        )                               as review_rank,

        -- percentile bands
        ntile(10) over (
            order by total_revenue desc
        )                               as revenue_decile   -- 1 = top 10%

    from sellers
    where total_orders > 0
),

-- state-level concentration: what % of sellers are in each state?
state_concentration as (
    select
        state,
        count(*)                        as sellers_in_state,
        count(*) * 100.0 / sum(count(*)) over ()
                                        as pct_of_all_sellers
    from sellers
    group by 1
)

select
    r.seller_id,
    r.state,
    r.city,
    r.seller_lat,
    r.seller_lng,

    -- tiers
    r.seller_tier,
    r.quality_tier,
    r.is_risk_seller,

    -- rankings
    r.revenue_rank,
    r.review_rank,
    r.revenue_decile,
    case when r.revenue_rank <= 10 then true else false end  as is_top_10_seller,

    -- revenue & volume
    r.total_orders,
    r.total_items_sold,
    r.distinct_products,
    r.total_revenue,
    r.avg_item_price,

    -- delivery
    r.avg_days_to_deliver,
    r.late_delivery_rate,

    -- quality
    r.avg_review_score,
    r.one_star_reviews,
    r.five_star_reviews,
    r.one_star_rate,

    -- geo concentration context
    sc.sellers_in_state,
    sc.pct_of_all_sellers                               as pct_sellers_in_state

from ranked r
left join state_concentration sc on r.state = sc.state
order by r.revenue_rank
