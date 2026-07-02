with source as (
    select distinct
        categorie_produit,
        sous_categorie_produit,
        libelle,
        modeles_ou_references
    from {{ ref('stg_silver__rappelconso') }}
    where libelle is not null
),

known as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'categorie_produit', 'sous_categorie_produit', 'libelle', 'modeles_ou_references'
        ]) }} as produit_key,
        categorie_produit,
        sous_categorie_produit,
        libelle as nom_produit,
        modeles_ou_references
    from source
),

unknown as (
    select
        {{ unknown_key('produit') }} as produit_key,
        cast(null as text) as categorie_produit,
        cast(null as text) as sous_categorie_produit,
        'Unknown' as nom_produit,
        cast(null as text) as modeles_ou_references
)

select * from known
union all
select * from unknown
