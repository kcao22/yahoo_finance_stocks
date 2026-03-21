{{ config(
    materialized='incremental',
    unique_key='pk_ticker_date',
    tags=["daily"]
) }}

with
    stocks as (
        select
            pk_ticker_date,
            company_symbol,
            previous_close_price,
            open_price,
            bid_price,
            bid_size,
            ask_price,
            ask_size,
            day_low,
            day_high,
            company_market_value,
            stock_volatility,
            price_to_earnings_ratio,
            earnings_per_share,
            forward_dividend_payout,
            forward_dividend_yield,
            ex_dividend_date,
            one_year_target_estimate,
            load_date
        from {{ ref('clean_stocks') }}
        {% if is_incremental() %}
            where load_date > (select coalesce(max(load_date), date('1900-01-01')) from {{ this }})
        {% endif %}
    )

select *
from stocks
