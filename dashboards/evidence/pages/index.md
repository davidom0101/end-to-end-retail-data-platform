---
title: Retail performance
---

```sql sales_summary
select
    coalesce(sum(net_sales), 0) as net_sales,
    coalesce(sum(order_count), 0) as order_count,
    coalesce(sum(net_sales) / nullif(sum(order_count), 0), 0) as average_order_value
from retail.daily_sales
```

```sql sales_by_day
select order_date, net_sales, order_count, average_order_value
from retail.daily_sales
order by order_date
```

```sql top_products
select product_name, product_id, category, net_sales
from retail.product_performance
order by net_sales desc, product_id
limit 10
```

```sql category_sales
select category, sum(net_sales) as net_sales
from retail.product_performance
group by category
order by net_sales desc, category
```

```sql customers
select
    count(case when order_count > 0 then 1 end) as purchasing_customers,
    coalesce(sum(repeat_customer), 0) as repeat_customers,
    coalesce(sum(repeat_customer) * 1.0 / nullif(count(case when order_count > 0 then 1 end), 0), 0)
        as repeat_rate
from retail.customer_activity
```

<BigValue data={sales_summary} value=net_sales title="Net sales" fmt={'"€"#,##0.00'} />
<BigValue data={sales_summary} value=order_count title="Orders" fmt='#,##0' />
<BigValue data={sales_summary} value=average_order_value title="Average order value" fmt={'"€"#,##0.00'} />

Sales include paid, shipped, and delivered orders after line discounts. Dates use UTC.

## Daily sales

<LineChart data={sales_by_day} x=order_date y=net_sales yFmt={'"€"#,##0.00'} xAxisTitle="Order date (UTC)" yAxisTitle="Net sales" />
<BarChart data={sales_by_day} x=order_date y=order_count yFmt='#,##0' xAxisTitle="Order date (UTC)" yAxisTitle="Orders" />
<LineChart data={sales_by_day} x=order_date y=average_order_value yFmt={'"€"#,##0.00'} xAxisTitle="Order date (UTC)" yAxisTitle="Average order value" />

## Products and categories

<BarChart data={category_sales} x=category y=net_sales swapXY=true yFmt={'"€"#,##0.00'} title="Net sales by current category" />

### Top 10 products by net sales

<DataTable data={top_products} rows=10>
    <Column id=product_name title="Product" />
    <Column id=category title="Category" />
    <Column id=net_sales title="Net sales" fmt={'"€"#,##0.00'} />
</DataTable>

## Repeat purchasing

<BigValue data={customers} value=purchasing_customers title="Purchasing customers" fmt='#,##0' />
<BigValue data={customers} value=repeat_customers title="Repeat customers" fmt='#,##0' />
<BigValue data={customers} value=repeat_rate title="Repeat customer rate" fmt='0.0%' />

A repeat customer has at least two eligible orders. The rate uses customers with at least one eligible order.
