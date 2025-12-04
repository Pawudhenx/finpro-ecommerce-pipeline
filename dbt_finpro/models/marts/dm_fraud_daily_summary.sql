{{ config(
    materialized='table',
    alias='dm_fraud_daily_summary'
) }}

with orders as (

    select
        order_id,
        status,
        total_amount,
        created_at
    from {{ ref('fact_orders') }}

),

daily as (

    select
        date(created_at) as order_date,
        count(*) as total_tx,
        countif(status = 'fraud') as fraud_count,
        countif(status = 'genuine') as genuine_count,

        sum(case when status = 'fraud' then total_amount else 0 end) as total_fraud_amount,
        avg(case when status = 'fraud' then total_amount else null end) as avg_fraud_amount,
        sum(total_amount) as total_amount_all,

        -- fraud rate
        safe_divide(countif(status = 'fraud'), count(*)) as fraud_rate,

        -- saved amount (asumsi fraud = amount yang “dicegah”)
        sum(case when status = 'fraud' then total_amount else 0 end) as saved_amount

    from orders
    group by 1

)

select * from daily
order by order_date desc
