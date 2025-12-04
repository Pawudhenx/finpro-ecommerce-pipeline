

select
    product_id,
    product_name,
    category,
    cast(price as float64) as price,
    cast(created_date as timestamp) as created_at
from `jcdeah-006`.`fauzan_finpro`.`products`