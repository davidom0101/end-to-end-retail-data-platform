{{ config(alias='mart_product_performance_candidate' ~ var('sales_candidate_suffix', '')) }}

with product_sales as (
    select
        items.product_id,
        sum(items.net_line_amount) as net_sales,
        sum(items.quantity) as units_sold,
        count(distinct items.order_id) as order_count
    from {{ ref('fct_order_items') }} as items
    inner join {{ ref('fct_orders') }} as orders on items.order_id = orders.order_id
    where orders.status in ('paid', 'shipped', 'delivered')
    group by items.product_id
)
select
    products.product_id,
    products.product_name,
    products.category,
    coalesce(sales.net_sales, 0) as net_sales,
    coalesce(sales.units_sold, 0) as units_sold,
    coalesce(sales.order_count, 0) as order_count
from {{ ref('dim_products') }} as products
left join product_sales as sales on products.product_id = sales.product_id
