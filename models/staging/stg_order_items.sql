with source as (
    select * from {{ source('olist_raw', 'order_items') }}
),

renamed as (
    select
        order_id,
        order_item_id                           as item_seq,
        product_id,
        seller_id,
        cast(price         as double)           as item_price,
        cast(freight_value as double)           as freight_value,
        cast(shipping_limit_date as timestamp)  as shipping_limit_date,
        
        -- Derived fields
        cast(price + freight_value as double)   as total_item_value
    from source
)

select * from renamed
