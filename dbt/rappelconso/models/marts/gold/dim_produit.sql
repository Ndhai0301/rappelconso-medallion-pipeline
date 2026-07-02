with source as (
    select distinct
        categorie_produit,
        sous_categorie_produit,
        libelle,
        modeles_ou_references
    from {{ ref('stg_silver__rappelconso') }}
    where libelle is not null
)

select
    {{ dbt_utils.generate_surrogate_key([
        'categorie_produit', 'sous_categorie_produit', 'libelle', 'modeles_ou_references'
    ]) }} as produit_key,
    categorie_produit,
    sous_categorie_produit,
    libelle as nom_produit,
    modeles_ou_references
from source
