{% snapshot snap_rappelconso %}
{{
    config(
        target_schema='snapshots',
        unique_key='deduplication_key',
        strategy='timestamp',
        updated_at='silver_updated_at',
    )
}}
select *
from {{ source('silver', 'rappelconso_clean') }}
{% endsnapshot %}
