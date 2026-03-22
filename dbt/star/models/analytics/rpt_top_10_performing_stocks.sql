{{ config(
    materialized='table',
    tags=["daily", "analytics"]
) }}

with ranked_stocks as (
    select
        company_symbol,
        open_price,
        load_date,
        row_number() over (partition by company_symbol order by load_date desc) as rn
    from {{ ref('fact_stocks') }}
),

latest_and_previous as (
    select
        l.company_symbol,
        p.open_price as previous_open_price,
        l.open_price as latest_open_price,
        l.load_date as latest_date,
        p.load_date as previous_date
    from ranked_stocks l
    join ranked_stocks p on l.company_symbol = p.company_symbol and p.rn = 2
    where l.rn = 1
),

growth_calc as (
    select
        company_symbol,
        previous_open_price,
        latest_open_price,
        latest_date,
        previous_date,
        case
            when previous_open_price > 0 then ((latest_open_price - previous_open_price) / previous_open_price) * 100
            else null
        end as percent_increase,
        (latest_open_price - previous_open_price) as absolute_increase
    from latest_and_previous
)

select
    company_symbol,
    previous_open_price,
    latest_open_price,
    absolute_increase,
    percent_increase,
    latest_date,
    previous_date
from growth_calc
order by percent_increase desc nulls last
limit 10
