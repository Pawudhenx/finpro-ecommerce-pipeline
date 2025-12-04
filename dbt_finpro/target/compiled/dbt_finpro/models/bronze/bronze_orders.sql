

select
    order_id,
    user_id,
    product_id,
    quantity,
    price,
    total_amount,
    status,
    reason,
    cast(created_date as timestamp) as created_at
from `jcdeah-006`.`fauzan_finpro`.`orders`