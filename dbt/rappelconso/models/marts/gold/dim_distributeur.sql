with source as (
    select distinct distributeurs
    from {{ ref('stg_silver__rappelconso') }}
    where distributeurs is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['distributeurs']) }} as distributeur_key,
    distributeurs
from source
