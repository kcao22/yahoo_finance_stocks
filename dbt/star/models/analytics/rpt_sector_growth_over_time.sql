{{ config(
    materialized='table',
    tags=["daily", "analytics"]
) }}

with stock_dates as (
    select
        company_symbol,
        min(load_date) as earliest_date,
        max(load_date) as latest_date
    from {{ ref('fact_stocks') }}
    group by 1
),

earliest_prices as (
    select
        f.company_symbol,
        f.open_price as earliest_open_price
    from {{ ref('fact_stocks') }} f
    join stock_dates d on f.company_symbol = d.company_symbol and f.load_date = d.earliest_date
),

latest_prices as (
    select
        f.company_symbol,
        f.open_price as latest_open_price
    from {{ ref('fact_stocks') }} f
    join stock_dates d on f.company_symbol = d.company_symbol and f.load_date = d.latest_date
),

company_growth as (
    select
        e.company_symbol,
        e.earliest_open_price,
        l.latest_open_price,
        case
            when e.earliest_open_price > 0 then ((l.latest_open_price - e.earliest_open_price) / e.earliest_open_price) * 100
            else null
        end as percent_growth
    from earliest_prices e
    join latest_prices l on e.company_symbol = l.company_symbol
)

select
    s.company_sector as sector,
    avg(cg.percent_growth) as avg_sector_growth_pct,
    count(cg.company_symbol) as num_companies
from company_growth cg
join {{ ref('dim_sectors') }} s on cg.company_symbol = s.company_symbol
group by s.company_sector
