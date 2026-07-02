with snap as (
    select *
    from {{ ref('snap_rappelconso') }}
),

produit as (
    select * from {{ ref('dim_produit') }}
),

marque as (
    select * from {{ ref('dim_marque') }}
),

risque as (
    select * from {{ ref('dim_risque') }}
),

distributeur as (
    select * from {{ ref('dim_distributeur') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['snap.deduplication_key', 'snap.dbt_valid_from']) }} as fact_rappel_key,
    snap.deduplication_key,
    snap.numero_fiche,
    snap.rappel_guid,
    snap.numero_version,
    cast(to_char(snap.date_publication, 'YYYYMMDD') as int) as date_key,
    coalesce(produit.produit_key, {{ unknown_key('produit') }}) as produit_key,
    coalesce(marque.marque_key, {{ unknown_key('marque') }}) as marque_key,
    coalesce(risque.risque_key, {{ unknown_key('risque') }}) as risque_key,
    coalesce(distributeur.distributeur_key, {{ unknown_key('distributeur') }}) as distributeur_key,
    jsonb_array_length(coalesce(snap.identification_produits, '[]'::jsonb)) as nb_produits_identifies,
    (snap.date_de_fin_de_la_procedure_de_rappel - snap.date_publication::date) as jours_procedure_rappel,
    snap.dbt_valid_from as valid_from,
    snap.dbt_valid_to as valid_to,
    (snap.dbt_valid_to is null) as is_current
from snap
left join produit
    on produit.categorie_produit is not distinct from snap.categorie_produit
   and produit.sous_categorie_produit is not distinct from snap.sous_categorie_produit
   and produit.nom_produit is not distinct from snap.libelle
   and produit.modeles_ou_references is not distinct from snap.modeles_ou_references
left join marque
    on marque.marque_produit is not distinct from snap.marque_produit
left join risque
    on risque.risques_encourus is not distinct from snap.risques_encourus
   and risque.motif_rappel is not distinct from snap.motif_rappel
left join distributeur
    on distributeur.distributeurs is not distinct from snap.distributeurs
