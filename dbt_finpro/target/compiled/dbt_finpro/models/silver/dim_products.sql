

with src as (

    select
        product_id,
        product_name,
        category,
        price,
        cast(created_at as timestamp) as created_at
    from `jcdeah-006`.`fauzan_finpro`.`bronze_products`

),

clean as (

    select
        product_id as product_key,
        product_name,
        upper(category) as category,
        price,
        created_at,
        extract(year from created_at) as created_year

    from src

)

select * from clean