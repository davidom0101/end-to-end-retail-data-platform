select product_id, name as product_name, category, current_price, is_active
from {{ source('core', 'products') }}
