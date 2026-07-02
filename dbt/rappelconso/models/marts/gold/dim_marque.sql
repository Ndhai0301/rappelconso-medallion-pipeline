with source as (
    select distinct marque_produit
    from {{ ref('stg_silver__rappelconso') }}
    where marque_produit is not null
),

known as (
    select
        {{ dbt_utils.generate_surrogate_key(['marque_produit']) }} as marque_key,
        marque_produit
    from source
),

unknown as (
    select
        {{ unknown_key('marque') }} as marque_key,
        'Unknown' as marque_produit
)

select * from known
union all
select * from unknown
