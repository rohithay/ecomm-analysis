/*
  mart_revenue_monthly
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Grain: one row per (order_month, category_name_english, primary_payment_type)

  Answers:
    • What is monthly/quarterly GMV trend 2016–2018?
    • Which product categories drive the most revenue vs. volume?
    • What's the Average Order Value by payment method?

  Note: because one order can contain items from multiple categories,
  revenue is attributed at the item level (from stg_order_items joined
  to products). This means order_count here counts an order once per
  category it spans — which is correct for category-level revenue
  attribution but will overcount if you sum order_count across
  categories to get total order count. Use fct_orders for that.
*/

with order_items_categorised as (
    select
        i.order_id,
        i.item_price,
        i.freight_value,
        p.category_name_english
    from {{ ref('stg_order_items') }} i
    left join {{ ref('int_products_enriched') }} p
        on i.product_id = p.product_id
),

orders_base as (
    select
        order_id,
        order_month,
        date_trunc('quarter', purchased_at)     as order_quarter,
        primary_payment_type,
        gross_order_value,
        item_count,
        is_late_delivery,
        order_status
    from {{ ref('fct_orders') }}
    where order_status in ('delivered', 'shipped', 'processing', 'approved')
),

-- join items back to orders to get month + payment context per item
items_with_order_context as (
    select
        o.order_month,
        o.order_quarter,
        o.primary_payment_type,
        ic.category_name_english,
        ic.item_price,
        ic.freight_value,
        o.order_id
    from orders_base o
    inner join order_items_categorised ic on o.order_id = ic.order_id
),

final as (
    select
        order_month,
        order_quarter,
        coalesce(category_name_english, 'unknown')  as category_name_english,
        primary_payment_type,

        -- volume
        count(distinct order_id)                    as order_count,
        sum(item_price + freight_value)             as gross_order_value,
        sum(item_price)                             as items_revenue,
        sum(freight_value)                          as freight_revenue,

        -- averages
        sum(item_price + freight_value)
            / nullif(count(distinct order_id), 0)   as avg_order_value

    from items_with_order_context
    group by 1, 2, 3, 4
)

select * from final
order by order_month, category_name_english
