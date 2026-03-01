/*
  dim_products
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Grain: one row per product_id
*/

select
    product_id,
    product_category_name,
    category_name_english,

    -- physical
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_volume_cm3,

    -- size bucket — useful for delivery analysis
    case
        when product_volume_cm3 is null     then 'unknown'
        when product_volume_cm3 < 1000      then 'tiny'
        when product_volume_cm3 < 10000     then 'small'
        when product_volume_cm3 < 50000     then 'medium'
        else 'large'
    end                                     as size_bucket,

    -- listing quality
    product_photos_qty,
    product_name_length,
    product_description_length,

    -- performance
    times_ordered,
    total_revenue,
    avg_item_price,
    avg_freight_value,
    freight_to_price_ratio,
    avg_review_score,
    avg_days_to_deliver,
    one_star_rate

from {{ ref('int_products_enriched') }}
