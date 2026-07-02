{#
    Use the custom schema name (e.g. "silver", "gold") exactly as given instead
    of dbt's default "<target_schema>_<custom_schema>" prefixing, so models
    land in the literal bronze/silver/gold schemas the warehouse expects.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
