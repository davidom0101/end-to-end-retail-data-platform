with line_totals as (
    select order_id, sum(net_line_amount) as net_amount
    from {{ ref('fct_order_items') }}
    group by order_id
), eligible_lines as (
    select orders.order_date, sum(items.net_amount) as net_amount
    from {{ ref('fct_orders') }} as orders
    left join line_totals as items on orders.order_id = items.order_id
    where orders.status in ('paid', 'shipped', 'delivered')
    group by orders.order_date
), expected as (
    select coalesce(sum(net_amount), 0) as net_amount from eligible_lines
), marts as (
    select 'products' as mart, coalesce(sum(net_sales), 0) as net_amount
    from {{ ref('mart_product_performance_candidate') }}
    union all
    select 'customers', coalesce(sum(net_sales), 0)
    from {{ ref('mart_customer_activity_candidate') }}
)
select 'order' as grain, orders.order_id as record_id
from {{ ref('fct_orders') }} as orders
left join line_totals as items on orders.order_id = items.order_id
where orders.net_order_amount <> coalesce(items.net_amount, 0)
union all
select 'day', cast(coalesce(lines.order_date, daily.order_date) as varchar)
from eligible_lines as lines
full join {{ ref('mart_daily_sales_candidate') }} as daily on lines.order_date = daily.order_date
where lines.order_date is null or daily.order_date is null
    or coalesce(lines.net_amount, 0) <> daily.net_sales
union all
select 'mart', marts.mart from marts cross join expected
where marts.net_amount <> expected.net_amount
