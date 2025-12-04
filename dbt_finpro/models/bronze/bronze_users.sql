{{ config(
    materialized='table',
    alias='bronze_users'
) }}

select
    user_id,
    name,
    email,
    cast(created_date as timestamp) as created_at
from {{ source('raw_finpro', 'users') }}
