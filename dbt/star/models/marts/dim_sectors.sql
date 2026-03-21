with profiles as (
    select
        company_symbol,
        company_sector
    from {{ ref('clean_profiles') }}
)

select distinct
    company_symbol,
    company_sector
from profiles
