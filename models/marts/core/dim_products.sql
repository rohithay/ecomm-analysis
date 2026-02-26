/*
  dim_products
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Product dimension enriched with sales performance stats.
  Grain: one row per product_id.
*/

with products as (
    select * from {{ ref('stg_products') }}
),

items as (
    select
        product_id,
        count(distinct order_id)        as orders_containing_product,
        sum(item_price)                 as total_revenue,
        avg(item_price)                 as avg_selling_price,
        count(*)                        as total_units_sold,
        avg(freight_value)              as avg_freight_value,
        min(item_price)                 as min_price,
        max(item_price)                 as max_price
    from {{ ref('stg_order_items') }}
    group by 1
),

final as (
    select
        p.product_id,
        p.category_name,
        p.category_name_pt,
        p.weight_g,
        p.length_cm,
        p.height_cm,
        p.width_cm,
        p.volume_cm3,
        p.photos_qty,
        p.name_length,
        p.description_length,

        -- Sales stats
        coalesce(i.orders_containing_product, 0)    as orders_count,
        coalesce(i.total_units_sold,          0)    as units_sold,
        coalesce(i.total_revenue,             0.0)  as total_revenue,
        coalesce(i.avg_selling_price,         0.0)  as avg_selling_price,
        coalesce(i.avg_freight_value,         0.0)  as avg_freight_value,
        i.min_price,
        i.max_price,

        -- Freight ratio (higher = bulkier product)
        case
            when coalesce(i.avg_selling_price, 0) > 0
            then round(i.avg_freight_value / i.avg_selling_price, 4)
            else null
        end as freight_to_price_ratio

    from products p
    left join items i on p.product_id = i.product_id
)

select * from final
