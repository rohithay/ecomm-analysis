/*
  int_products_enriched
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Grain: one row per product_id

  Combines:
    - product attributes & physical dimensions from stg_products
    - category name (English) from stg_product_category_name_translation
    - sales performance from stg_order_items
    - review signals from int_orders_enriched

  Used by:
    - dim_products
    - mart_delivery_performance (category delivery time)
    - Product Intelligence questions
*/

with products as (
    select * from {{ ref('stg_products') }}
),

category_translation as (
    select * from {{ ref('stg_product_category_name_translation') }}
),

-- sales performance per product
items_agg as (
    select
        product_id,
        count(*)                            as times_ordered,
        count(distinct order_id)            as distinct_orders,
        count(distinct seller_id)           as distinct_sellers,
        sum(item_price)                     as total_revenue,
        avg(item_price)                     as avg_item_price,
        avg(freight_value)                  as avg_freight_value,
        sum(freight_value)                  as total_freight_value
    from {{ ref('stg_order_items') }}
    group by 1
),

-- review performance per product (via orders)
reviews_agg as (
    select
        i.product_id,
        avg(o.review_score)                     as avg_review_score,
        avg(o.days_to_deliver)                  as avg_days_to_deliver,
        sum(case when o.review_score = 1 then 1 else 0 end) as one_star_count,
        count(distinct i.order_id)              as reviewable_orders
    from {{ ref('stg_order_items') }} i
    inner join {{ ref('int_orders_enriched') }} o
        on i.order_id = o.order_id
    where o.review_score is not null
    group by 1
),

final as (
    select
        -- identity
        p.product_id,
        p.product_category_name,
        coalesce(t.product_category_name_english,
                 p.product_category_name)       as category_name_english,

        -- physical attributes
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,

        -- derived physical attributes
        -- volume in cm³ — proxy for bulkiness
        p.product_length_cm
            * p.product_height_cm
            * p.product_width_cm               as product_volume_cm3,

        -- sales performance
        coalesce(ia.times_ordered, 0)           as times_ordered,
        coalesce(ia.distinct_orders, 0)         as distinct_orders,
        coalesce(ia.distinct_sellers, 0)        as distinct_sellers,
        coalesce(ia.total_revenue, 0)           as total_revenue,
        ia.avg_item_price,
        ia.avg_freight_value,
        ia.total_freight_value,

        -- freight efficiency
        case
            when ia.avg_item_price > 0
            then ia.avg_freight_value / ia.avg_item_price
            else null
        end                                     as freight_to_price_ratio,

        -- review & delivery
        ra.avg_review_score,
        ra.avg_days_to_deliver,
        coalesce(ra.one_star_count, 0)          as one_star_count,
        case
            when coalesce(ra.reviewable_orders, 0) > 0
            then ra.one_star_count::float / ra.reviewable_orders
            else null
        end                                     as one_star_rate

    from products p
    left join category_translation t  on p.product_category_name = t.product_category_name
    left join items_agg ia            on p.product_id = ia.product_id
    left join reviews_agg ra          on p.product_id = ra.product_id
)

select * from final
