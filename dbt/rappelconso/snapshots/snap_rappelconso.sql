{% snapshot snap_rappelconso %}
{{
    config(
        target_schema='snapshots',
        unique_key='deduplication_key',
        strategy='timestamp',
        updated_at='silver_updated_at',
    )
}}
select
    {{ dbt_utils.star(from=source('silver', 'rappelconso_clean'), except=["silver_updated_at"]) }},
    -- dbt's snapshot metadata columns (dbt_valid_from/dbt_valid_to) are
    -- timestamp without time zone on Postgres; casting here avoids a
    -- DATETIME/DATETIMETZ mismatch in the timestamp-strategy comparison.
    silver_updated_at::timestamp without time zone as silver_updated_at
from {{ source('silver', 'rappelconso_clean') }}
{% endsnapshot %}
