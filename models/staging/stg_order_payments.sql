with source as (
    select * from {{ source('olist_raw', 'order_payments') }}
),

renamed as (
    select
        order_id,
        cast(payment_sequential   as int)    as payment_seq,
        payment_type,
        cast(payment_installments as int)    as installments,
        cast(payment_value        as double) as payment_value
    from source
)

select * from renamed
