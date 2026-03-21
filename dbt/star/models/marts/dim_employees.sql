with profiles as (
    select
        pk_ticker_date,
        company_symbol,
        company_full_time_employees,
        load_date
    from {{ ref('clean_profiles') }}
), added_dates as (
    select
        pk_ticker_date,
        company_symbol,
        company_full_time_employees,
        load_date as valid_from,
        lead(load_date) over (partition by company_symbol order by load_date) as next_load_date
    from profiles
), final as (
    select
        pk_ticker_date,
        company_symbol,
        company_full_time_employees,
        valid_from,
        case when next_load_date is null then date '9999-12-31' else date_sub(next_load_date, interval 1 day) end as valid_to,
        case when next_load_date is null then true else false end as is_current_day
    from added_dates
)
select *
from final
