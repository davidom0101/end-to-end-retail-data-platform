select customer_id, created_at, country, customer_segment
from {{ source('core', 'customers') }}
