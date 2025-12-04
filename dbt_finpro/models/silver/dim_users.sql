{{ config(
    materialized='table',
    alias='dim_users'
) }}

with src as (

    select
        user_id,
        name,
        email,
        cast(created_at as timestamp) as created_at
    from {{ ref('bronze_users') }}

),

clean as (

    select
        -- surrogate key (boleh pakai user_id langsung)
        user_id as user_key,

        -- nama depan & belakang sederhana
        split(name, ' ')[SAFE_OFFSET(0)] as first_name,
        split(name, ' ')[SAFE_OFFSET(ARRAY_LENGTH(split(name, ' ')) - 1)] as last_name,

        email,
        regexp_extract(email, r'@(.+)$') as email_domain,

        created_at,
        extract(year from created_at) as signup_year

    from src

)

select * from clean
