/*
  int_customers_enriched
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Grain: one row per customer_id

  Combines:
    - customer attributes from stg_customers
    - lifetime order metrics from int_orders_enriched
    - geo attributes from stg_geolocation (state, city)

  Used by:
    - dim_customers
    - mart_delivery_performance (freight % by region)
*/

with customers as (
    select * from {{ ref('stg_customers') }}
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

-- aggregate orders at customer level
orders_agg as (
    select
        customer_id,

        -- volume
        count(*)                                    as total_orders,
        count(case when order_status = 'delivered'
              then 1 end)                           as delivered_orders,

        -- revenue
        sum(gross_order_value)                      as lifetime_gmv,
        sum(freight_revenue)                        as lifetime_freight,
        avg(gross_order_value)                      as avg_order_value,

        -- freight as % of order value (computed at order level, averaged)
        avg(case
            when gross_order_value > 0
            then freight_revenue / gross_order_value
            else null
        end)                                        as avg_freight_pct,

        -- timing
        min(purchased_at)                           as first_order_at,
        max(purchased_at)                           as last_order_at,

        -- delivery experience
        avg(days_to_deliver)                        as avg_days_to_deliver,
        sum(case when is_late_delivery then 1 else 0 end) as late_orders,

        -- reviews
        avg(review_score)                           as avg_review_score,

        -- payment behaviour
        max(primary_payment_type)                   as most_used_payment_type

    from {{ ref('int_orders_enriched') }}
    group by 1
),

final as (
    select
        -- identity
        c.customer_id,
        c.customer_unique_id,
        c.zip_code_prefix,

        -- geo (from geolocation lookup)
        coalesce(g.state, c.state)                  as state,
        coalesce(g.city,  c.city)                   as city,
        g.lat                                       as customer_lat,
        g.lng                                       as customer_lng,

        -- lifetime metrics
        coalesce(o.total_orders, 0)                 as total_orders,
        coalesce(o.delivered_orders, 0)             as delivered_orders,
        coalesce(o.lifetime_gmv, 0)                 as lifetime_gmv,
        coalesce(o.lifetime_freight, 0)             as lifetime_freight,
        o.avg_order_value,
        o.avg_freight_pct,

        -- behavioural flags
        case
            when coalesce(o.total_orders, 0) > 1 then true
            else false
        end                                         as is_repeat_buyer,

        -- timing
        o.first_order_at,
        o.last_order_at,
        datediff('day', o.first_order_at,
                        o.last_order_at)             as customer_lifespan_days,

        -- delivery experience
        o.avg_days_to_deliver,
        coalesce(o.late_orders, 0)                  as late_orders,
        case
            when coalesce(o.total_orders, 0) > 0
            then o.late_orders::float / o.total_orders
            else null
        end                                         as late_order_rate,

        -- satisfaction
        o.avg_review_score,
        o.most_used_payment_type

    from customers c
    left join geo      g on c.zip_code_prefix = g.zip_code_prefix
    left join orders_agg o on c.customer_id   = o.customer_id
)

select * from final
