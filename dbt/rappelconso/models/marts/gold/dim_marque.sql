with source as (
    select distinct marque_produit
    from {{ ref('stg_silver__rappelconso') }}
    where marque_produit is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['marque_produit']) }} as marque_key,
    marque_produit
from source
