select
    item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    cast(coalesce(discount_amount, 0) as decimal(12, 2)) as discount_amount,
    cast(quantity * unit_price - coalesce(discount_amount, 0) as decimal(26, 2))
        as net_line_amount
from {{ source('core', 'order_items') }}
