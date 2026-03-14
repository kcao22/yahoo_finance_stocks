{{
  config(
    materialized='incremental',
    unique_key='pk_ticker_date'
  )
}}


with
    source as (
        select
            company_symbol,
            company_name,
            company_sector,
            company_industry,
            company_full_time_employees,
            company_description,
            load_date,
            load_filename
        from {{ source("raw", "profiles") }}

        {% if is_incremental() %}
            where load_date > (select max(load_date) from {{ this }})
        {% endif %}
    ),
    cleaned_data as (
        select
            company_symbol,
            company_name,
            company_sector,
            company_industry,
            safe_cast(
                replace(company_full_time_employees, ',', '') as int64
            ) as company_full_time_employees,
            company_description,
            load_date,
            load_filename
        from source
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
