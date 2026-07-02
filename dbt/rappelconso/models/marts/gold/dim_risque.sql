with source as (
    select distinct
        risques_encourus,
        motif_rappel
    from {{ ref('stg_silver__rappelconso') }}
    where risques_encourus is not null
),

known as (
    select
        {{ dbt_utils.generate_surrogate_key(['risques_encourus', 'motif_rappel']) }} as risque_key,
        risques_encourus,
        motif_rappel
    from source
),

unknown as (
    select
        {{ unknown_key('risque') }} as risque_key,
        'Unknown' as risques_encourus,
        cast(null as text) as motif_rappel
)

select * from known
union all
select * from unknown
