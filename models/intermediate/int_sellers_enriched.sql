/*
  int_sellers_enriched
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Grain: one row per seller_id

  Combines:
    - seller attributes from stg_sellers
    - order/revenue metrics from stg_order_items + int_orders_enriched
    - review scores from int_orders_enriched
    - delivery performance from int_orders_enriched

  Used by:
    - dim_sellers
    - mart_seller_health
    - mart_delivery_performance
*/

with sellers as (
    select * from {{ ref('stg_sellers') }}
),

geo as (
    select distinct
        zip_code_prefix,
        state,
        city,
        lat,
        lng
    from {{ ref('stg_geolocation') }}
    qualify row_number() over (
        partition by zip_code_prefix order by zip_code_prefix
    ) = 1
),

-- seller-level aggregates from order items
-- note: one order can have items from multiple sellers,
--       so we aggregate at (seller_id) grain from stg_order_items
items_agg as (
    select
        i.seller_id,
        count(distinct i.order_id)              as total_orders,
        count(*)                                as total_items_sold,
        count(distinct i.product_id)            as distinct_products,
        sum(i.item_price)                       as total_revenue,
        sum(i.freight_value)                    as total_freight_collected,
        avg(i.item_price)                       as avg_item_price
    from {{ ref('stg_order_items') }} i
    group by 1
),

-- delivery & review performance: join via order
-- because reviews and delivery are at order level, not item level.
-- we bring in only orders where this seller had at least one item.
order_perf as (
    select
        i.seller_id,
        avg(o.days_to_deliver)                          as avg_days_to_deliver,
        avg(case
            when o.estimated_delivery_at is not null
            then datediff('day', o.shipped_at, o.estimated_delivery_at)
        end)                                            as avg_promised_window,
        sum(case when o.is_late_delivery then 1 else 0 end) as late_orders,
        count(distinct i.order_id)                      as scorable_orders,
        avg(o.review_score)                             as avg_review_score,
        sum(case when o.review_score = 1 then 1 else 0 end) as one_star_reviews,
        sum(case when o.review_score = 5 then 1 else 0 end) as five_star_reviews
    from {{ ref('stg_order_items') }} i
    inner join {{ ref('int_orders_enriched') }} o
        on i.order_id = o.order_id
    where o.order_status = 'delivered'
    group by 1
),

final as (
    select
        -- identity
        s.seller_id,
        s.zip_code_prefix,

        -- geo
        coalesce(g.state, s.state)              as state,
        coalesce(g.city,  s.city)               as city,
        g.lat                                   as seller_lat,
        g.lng                                   as seller_lng,

        -- volume & revenue
        coalesce(ia.total_orders, 0)            as total_orders,
        coalesce(ia.total_items_sold, 0)        as total_items_sold,
        coalesce(ia.distinct_products, 0)       as distinct_products,
        coalesce(ia.total_revenue, 0)           as total_revenue,
        coalesce(ia.total_freight_collected, 0) as total_freight_collected,
        ia.avg_item_price,

        -- delivery performance
        op.avg_days_to_deliver,
        op.avg_promised_window,
        coalesce(op.late_orders, 0)             as late_orders,
        case
            when coalesce(ia.total_orders, 0) > 0
            then coalesce(op.late_orders, 0)::float / ia.total_orders
            else null
        end                                     as late_delivery_rate,

        -- quality signals
        op.avg_review_score,
        coalesce(op.one_star_reviews, 0)        as one_star_reviews,
        coalesce(op.five_star_reviews, 0)       as five_star_reviews,
        case
            when coalesce(op.scorable_orders, 0) > 0
            then op.one_star_reviews::float / op.scorable_orders
            else null
        end                                     as one_star_rate,

        -- risk flag: high revenue but low reviews
        -- threshold is a business decision; adjust as needed
        case
            when coalesce(ia.total_revenue, 0) > 10000
             and coalesce(op.avg_review_score, 5) < 3
            then true
            else false
        end                                     as is_high_revenue_low_rating

    from sellers s
    left join geo      g  on s.zip_code_prefix = g.zip_code_prefix
    left join items_agg ia on s.seller_id      = ia.seller_id
    left join order_perf op on s.seller_id     = op.seller_id
)

select * from final
