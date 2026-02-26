/*
  analyses/rfm_segmentation.sql
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  RFM (Recency, Frequency, Monetary) analysis.
  Run with: dbt compile --select analyses/rfm_segmentation
  Then execute the compiled SQL against your DuckDB.
*/

with base as (
    select
        customer_id,
        days_since_last_order       as recency_days,
        total_orders                as frequency,
        lifetime_value              as monetary
    from {{ ref('dim_customers') }}
    where customer_segment != 'no_orders'
),

scored as (
    select
        customer_id,
        recency_days,
        frequency,
        monetary,

        -- Quintile scores (5 = best)
        ntile(5) over (order by recency_days asc)  as r_score,  -- Lower recency = better
        ntile(5) over (order by frequency    asc)  as f_score,
        ntile(5) over (order by monetary     asc)  as m_score

    from base
),

labeled as (
    select
        *,
        r_score || f_score || m_score           as rfm_segment,
        round((r_score + f_score + m_score) / 3.0, 2) as rfm_avg,

        case
            when r_score >= 4 and f_score >= 4  then 'Champions'
            when r_score >= 3 and f_score >= 3  then 'Loyal Customers'
            when r_score >= 4 and f_score <= 2  then 'Recent Customers'
            when r_score >= 3 and f_score <= 3
                 and m_score >= 3               then 'Potential Loyalists'
            when r_score <= 2 and f_score >= 4  then 'At Risk'
            when r_score <= 2 and f_score <= 2
                 and m_score <= 2               then 'Lost'
            else 'Needs Attention'
        end as rfm_label

    from scored
)

select
    rfm_label,
    count(*)                    as customer_count,
    round(avg(recency_days), 1) as avg_recency_days,
    round(avg(frequency), 2)    as avg_orders,
    round(avg(monetary), 2)     as avg_ltv,
    round(sum(monetary), 2)     as total_ltv
from labeled
group by 1
order by total_ltv desc
