with source as (
    select distinct
        risques_encourus,
        motif_rappel
    from {{ ref('stg_silver__rappelconso') }}
    where risques_encourus is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['risques_encourus', 'motif_rappel']) }} as risque_key,
    risques_encourus,
    motif_rappel
from source
