with products as (
    select * from {{ source('olist_raw', 'products') }}
),

translations as (
    select * from {{ source('olist_raw', 'product_category_name_translation') }}
),

joined as (
    select
        p.product_id,
        coalesce(t.product_category_name_english, p.product_category_name, 'unknown')
            as category_name,
        p.product_category_name                         as category_name_pt,
        cast(p.product_name_lenght       as int)        as name_length,
        cast(p.product_description_lenght as int)       as description_length,
        cast(p.product_photos_qty        as int)        as photos_qty,
        cast(p.product_weight_g          as double)     as weight_g,
        cast(p.product_length_cm         as double)     as length_cm,
        cast(p.product_height_cm         as double)     as height_cm,
        cast(p.product_width_cm          as double)     as width_cm,

        -- Derived: volume in cm³
        cast(p.product_length_cm as double)
            * cast(p.product_height_cm as double)
            * cast(p.product_width_cm  as double)       as volume_cm3

    from products p
    left join translations t
        on p.product_category_name = t.product_category_name
)

select * from joined
