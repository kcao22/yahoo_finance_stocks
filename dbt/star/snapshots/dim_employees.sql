{% snapshot dim_employees %}

{{
    config(
        target_schema='marts',
        unique_key='company_symbol',
        strategy='check',
        check_cols=['company_full_time_employees']
    )
}}

with latest_profiles as (
    select
        company_symbol,
        company_full_time_employees,
        load_date
    from (
        select *, row_number() over (partition by company_symbol order by load_date desc) as rn
        from {{ ref('clean_profiles') }}
    )
    where rn = 1
)

select
    company_symbol,
    company_full_time_employees,
    load_date
from latest_profiles

{% endsnapshot %}
