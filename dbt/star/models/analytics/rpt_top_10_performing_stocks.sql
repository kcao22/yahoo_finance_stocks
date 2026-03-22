{{ config(
    materialized='table',
    tags=["daily", "analytics"]
) }}

with base as (
    select
        company_symbol,
        open_price,
        load_date,
        row_number() over (partition by company_symbol order by load_date desc) as rn
    from {{ ref('fact_stocks') }}
),

latest_prices as (
    select
        company_symbol,
        open_price as latest_open_price,
        load_date as latest_date
    from base
    where rn = 1
),

-- Find the most recent price for each stock that is different from its current price to avoid weekends and possibly holidays
previous_different_prices as (
    select
        b.company_symbol,
        b.open_price as previous_open_price,
        b.load_date as previous_date,
        row_number() over (partition by b.company_symbol order by b.load_date desc) as diff_rn
    from base b
    inner join latest_prices l 
        on b.company_symbol = l.company_symbol 
        and b.open_price != l.latest_open_price
        and b.load_date < l.latest_date
),

latest_and_previous as (
    select
        l.company_symbol,
        p.previous_open_price,
        l.latest_open_price,
        l.latest_date,
        p.previous_date
    from latest_prices l
    join previous_different_prices p 
        on l.company_symbol = p.company_symbol 
        and p.diff_rn = 1
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
where percent_increase > 0
order by percent_increase desc
limit 10