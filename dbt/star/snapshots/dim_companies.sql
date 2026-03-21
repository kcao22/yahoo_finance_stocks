{% snapshot dim_companies %}

{{
    config(
        target_schema='marts',
        unique_key='company_symbol',
        strategy='check',
        check_cols=['company_name', 'company_description']
    )
}}

with latest_profiles as (
    select
        company_symbol,
        company_name,
        company_description,
        load_date
    from (
        select *, row_number() over (partition by company_symbol order by load_date desc) as rn
        from {{ ref('clean_profiles') }}
    )
    where rn = 1
)

select
    company_symbol,
    company_name,
    company_description,
    load_date
from latest_profiles

{% endsnapshot %}
