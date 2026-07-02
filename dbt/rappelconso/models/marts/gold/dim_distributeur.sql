with source as (
    select distinct distributeurs
    from {{ ref('stg_silver__rappelconso') }}
    where distributeurs is not null
),

known as (
    select
        {{ dbt_utils.generate_surrogate_key(['distributeurs']) }} as distributeur_key,
        distributeurs
    from source
),

unknown as (
    select
        {{ unknown_key('distributeur') }} as distributeur_key,
        'Unknown' as distributeurs
)

select * from known
union all
select * from unknown
