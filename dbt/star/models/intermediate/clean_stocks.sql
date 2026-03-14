{{
  config(
    materialized='incremental',
    unique_key='rpk_ticker_date'
  )
}}


with
    source_data as (
        select
            company_symbol,
            previous_close,
            open as raw_open,  -- renamed to avoid reserved word issues
            bid,
            ask,
            day_range,
            volume,
            avg_volume,
            intraday_market_cap,
            beta,
            pe_ratio,
            eps,
            earnings_date,
            forward_dividend_and_yield,
            ex_dividend_date,
            one_year_target_estimate,
            load_date,
            load_filename
        from {{ source("raw", "stocks") }}  -- updated to match your source name 'yahoo'
        {% if is_incremental() %}
            where load_date > (select max(load_date) from {{ this }})
        {% endif %}
    ),

    cleaned_data as (
        select
            company_symbol,
            safe_cast(previous_close as float64) as previous_close_price,
            safe_cast(raw_open as float64) as open_price,

            -- bid/ask splits
            safe_cast(split(bid, ' x ')[safe_offset(0)] as float64) as bid_price,
            safe_cast(
                replace(split(bid, ' x ')[safe_offset(1)], ',', '') as float64
            ) as bid_size,
            safe_cast(split(ask, ' x ')[safe_offset(0)] as float64) as ask_price,
            safe_cast(
                replace(split(ask, ' x ')[safe_offset(1)], ',', '') as float64
            ) as ask_size,

            -- day range splits
            safe_cast(split(day_range, ' - ')[safe_offset(0)] as float64) as day_low,
            safe_cast(split(day_range, ' - ')[safe_offset(1)] as float64) as day_high,

            -- market cap conversions
            case
                when intraday_market_cap like '%t'
                then safe_cast(replace(intraday_market_cap, 't', '') as float64) * 1e12
                when intraday_market_cap like '%b'
                then safe_cast(replace(intraday_market_cap, 'b', '') as float64) * 1e9
                when intraday_market_cap like '%m'
                then safe_cast(replace(intraday_market_cap, 'm', '') as float64) * 1e6
                when intraday_market_cap like '%k'
                then safe_cast(replace(intraday_market_cap, 'k', '') as float64) * 1e3
                else safe_cast(intraday_market_cap as float64)
            end as company_market_value,

            safe_cast(beta as float64) as stock_volatility,
            safe_cast(pe_ratio as float64) as price_to_earnings_ratio,
            safe_cast(eps as float64) as earnings_per_share,

            -- dividend splits (stripping parentheses and %)
            safe_cast(
                split(forward_dividend_and_yield, ' ')[safe_offset(0)] as float64
            ) as forward_dividend_payout,
            safe_cast(
                regexp_replace(
                    split(forward_dividend_and_yield, ' ')[safe_offset(1)], r'[()%]', ''
                ) as float64
            )
            / 100 as forward_dividend_yield,

            safe_cast(ex_dividend_date as date) as ex_dividend_date,
            safe_cast(one_year_target_estimate as float64) as one_year_target_estimate,
            load_date,
            load_filename
        from source_data
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(['company_symbol', 'load_date']) }}
            as pk_ticker_date,
            *
        from cleaned_data
    )

select *
from final
