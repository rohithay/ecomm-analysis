-- Delivered orders should always have at least one line item
select
    order_id,
    order_status,
    item_count
from {{ ref('fct_orders') }}
where order_status = 'delivered'
  and item_count = 0
