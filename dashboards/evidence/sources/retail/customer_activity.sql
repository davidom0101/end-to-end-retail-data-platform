select order_count, case when is_repeat_customer then 1 else 0 end as repeat_customer
from mart_customer_activity
