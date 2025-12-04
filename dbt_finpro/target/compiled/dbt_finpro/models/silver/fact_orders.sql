

with orders as (
    select *
    from `jcdeah-006`.`fauzan_finpro`.`bronze_orders`
),

dim_users as (
    select *
    from `jcdeah-006`.`fauzan_finpro`.`dim_users`
),

dim_products as (
    select *
    from `jcdeah-006`.`fauzan_finpro`.`dim_products`
)

select
    o.order_id,
    o.user_id as user_key,
    o.product_id as product_key,

    u.first_name,
    u.last_name,
    u.email_domain,

    p.product_name,
    p.category,
    p.price as product_price,

    o.quantity,
    o.price as order_price,
    o.total_amount,
    o.status,
    o.reason,
    o.created_at,

    extract(date from o.created_at) as order_date
from orders o
left join dim_users u on o.user_id = u.user_key
left join dim_products p on o.product_id = p.product_key