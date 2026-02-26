/*
  agg_product_category_sales
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Category-level performance for merchandising analysis.
  Grain: one row per (category_name, order_month).
*/

with items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select
        product_id,
        category_name
    from {{ ref('stg_products') }}
),

orders as (
    select
        order_id,
        order_month,
        is_delivered,
        review_score,
        review_sentiment
    from {{ ref('fct_orders') }}
),

joined as (
    select
        coalesce(p.category_name, 'unknown')    as category_name,
        o.order_month,
        i.order_id,
        i.item_price,
        i.freight_value,
        i.total_item_value,
        o.is_delivered,
        o.review_score,
        o.review_sentiment
    from items i
    join orders   o on i.order_id    = o.order_id
    join products p on i.product_id  = p.product_id
),

agg as (
    select
        category_name,
        order_month,
        count(distinct order_id)                            as orders,
        count(*)                                            as units_sold,
        sum(item_price)                                     as product_revenue,
        sum(freight_value)                                  as freight_revenue,
        sum(total_item_value)                               as gross_revenue,
        avg(item_price)                                     as avg_unit_price,
        avg(review_score)                                   as avg_review_score,
        sum(case when review_sentiment = 'positive' then 1 else 0 end) as positive_reviews,
        sum(case when review_sentiment = 'negative' then 1 else 0 end) as negative_reviews

    from joined
    group by 1, 2
),

with_rank as (
    select
        *,
        -- Revenue rank within each month
        rank() over (
            partition by order_month
            order by gross_revenue desc
        ) as monthly_revenue_rank,

        -- Month-over-month revenue change
        lag(gross_revenue) over (
            partition by category_name
            order by order_month
        ) as prev_month_revenue,

        round(
            (gross_revenue - lag(gross_revenue) over (
                partition by category_name order by order_month
            )) / nullif(lag(gross_revenue) over (
                partition by category_name order by order_month
            ), 0) * 100, 2
        ) as mom_revenue_pct_change

    from agg
)

select * from with_rank
order by order_month, monthly_revenue_rank
