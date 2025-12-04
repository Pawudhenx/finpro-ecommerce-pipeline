{{ config(
    materialized='table',
    alias='bronze_orders'
) }}

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
from {{ source('raw_finpro', 'orders') }}
