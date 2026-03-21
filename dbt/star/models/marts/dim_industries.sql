with profiles as (
    select
        company_symbol,
        company_industry
    from {{ ref('clean_profiles') }}
)

select distinct
    company_symbol,
    company_industry
from profiles
