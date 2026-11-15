{{ config(alias='mart_customer_activity_candidate' ~ var('sales_candidate_suffix', '')) }}

with customer_sales as (
    select
        customer_id,
        count(*) as order_count,
        sum(net_order_amount) as net_sales,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from {{ ref('fct_orders') }}
    where status in ('paid', 'shipped', 'delivered')
    group by customer_id
)
select
    customers.customer_id,
    customers.customer_segment,
    coalesce(sales.order_count, 0) as order_count,
    coalesce(sales.net_sales, 0) as net_sales,
    sales.first_order_date,
    sales.last_order_date,
    coalesce(sales.order_count, 0) >= 2 as is_repeat_customer
from {{ ref('dim_customers') }} as customers
left join customer_sales as sales on customers.customer_id = sales.customer_id
