{{ config(
    materialized='table',
    alias='dm_user_risk_profile'
) }}

with orders as (
    select
        user_key,
        status,
        total_amount
    from {{ ref('fact_orders') }}
),

agg as (
    select
        user_key,
        count(*) as total_orders,
        countif(status = 'fraud') as fraud_orders,
        countif(status = 'genuine') as genuine_orders,

        safe_divide(countif(status = 'fraud'), count(*)) as fraud_ratio,

        sum(case when status = 'fraud' then total_amount else 0 end) as total_fraud_amount
    from orders
    group by 1
),

risk as (
    select
        user_key,
        total_orders,
        fraud_orders,
        genuine_orders,
        fraud_ratio,
        total_fraud_amount,

        case
            when fraud_ratio >= 0.4 then 'HIGH RISK'
            when fraud_ratio >= 0.2 then 'MEDIUM RISK'
            else 'LOW RISK'
        end as risk_level

    from agg
)

select * from risk
order by fraud_ratio desc
