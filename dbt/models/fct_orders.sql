with order_totals as (
    select order_id, sum(net_line_amount) as net_order_amount
    from {{ ref('fct_order_items') }}
    group by order_id
)
select
    orders.order_id,
    orders.customer_id,
    cast(orders.order_date as date) as order_date,
    orders.status,
    coalesce(order_totals.net_order_amount, 0) as net_order_amount
from {{ source('core', 'orders') }} as orders
left join order_totals on orders.order_id = order_totals.order_id
