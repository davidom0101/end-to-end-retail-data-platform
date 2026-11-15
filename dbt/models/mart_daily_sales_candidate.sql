{{ config(materialized='table', alias='mart_daily_sales_candidate' ~ var('sales_candidate_suffix', '')) }}

-- Core stores order timestamps in UTC.
select
    cast(orders.order_date as date) as order_date,
    sum(items.quantity * items.unit_price - coalesce(items.discount_amount, 0))
        as net_sales
from {{ source('core', 'orders') }} as orders
inner join {{ source('core', 'order_items') }} as items
    on orders.order_id = items.order_id
where orders.status in ('paid', 'shipped', 'delivered')
group by cast(orders.order_date as date)
