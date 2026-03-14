with
    stocks as (
        select
            pk_ticker_date,
            company_symbol,
            previous_close,
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
    )

select *
from stocks
