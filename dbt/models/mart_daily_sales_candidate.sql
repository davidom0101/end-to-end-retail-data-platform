{{ config(materialized='table', alias='mart_daily_sales_candidate' ~ var('sales_candidate_suffix', '')) }}

select
    order_date,
    sum(net_order_amount) as net_sales,
    count(*) as order_count,
    cast(avg(net_order_amount) as decimal(38, 2)) as average_order_value
from {{ ref('fct_orders') }}
where status in ('paid', 'shipped', 'delivered')
group by order_date
