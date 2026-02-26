with source as (
    select * from {{ source('olist_raw', 'order_reviews') }}
),

-- Take the latest review per order in case of duplicates
deduped as (
    select *,
        row_number() over (
            partition by order_id
            order by cast(review_answer_timestamp as timestamp) desc
        ) as rn
    from source
),

renamed as (
    select
        review_id,
        order_id,
        cast(review_score as int)                   as review_score,
        review_comment_title                        as comment_title,
        review_comment_message                      as comment_message,
        cast(review_creation_date  as timestamp)    as review_created_at,
        cast(review_answer_timestamp as timestamp)  as review_answered_at,

        -- Sentiment bucket
        case
            when cast(review_score as int) >= 4 then 'positive'
            when cast(review_score as int) =  3 then 'neutral'
            else 'negative'
        end as sentiment
    from deduped
    where rn = 1
)

select * from renamed