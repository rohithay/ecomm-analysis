with source as (
    select * from {{ source('olist_raw', 'orders') }}
),

renamed as (
    select
        -- Keys
        order_id,
        customer_id,

        -- Status
        order_status,

        -- Timestamps — cast strings to proper timestamps
        cast(order_purchase_timestamp      as timestamp) as purchased_at,
        cast(order_approved_at             as timestamp) as approved_at,
        cast(order_delivered_carrier_date  as timestamp) as shipped_at,
        cast(order_delivered_customer_date as timestamp) as delivered_at,
        cast(order_estimated_delivery_date as timestamp) as estimated_delivery_at,

        -- Derived fields
        case
            when order_delivered_customer_date is not null
                 and order_estimated_delivery_date is not null
            then datediff(
                'day',
                cast(order_estimated_delivery_date as timestamp),
                cast(order_delivered_customer_date as timestamp)
            )
            else null
        end as delivery_days_vs_estimate, -- positive = late, negative = early

        case
            when order_delivered_customer_date is not null
                 and order_purchase_timestamp is not null
            then datediff(
                'day',
                cast(order_purchase_timestamp as timestamp),
                cast(order_delivered_customer_date as timestamp)
            )
            else null
        end as days_to_deliver

    from source
)

select * from renamed
