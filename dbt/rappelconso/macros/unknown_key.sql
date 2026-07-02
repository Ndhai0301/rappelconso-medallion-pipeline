{#
    Deterministic surrogate key for a dimension's "Unknown" member, used to
    map fact rows whose source attribute was NULL (rather than leaving the
    fact's foreign key NULL). Kept as one macro so the dim table's Unknown
    row and fact_rappel's coalesce always agree on the same key.
#}
{% macro unknown_key(dimension_name) %}
{{ dbt_utils.generate_surrogate_key(["'unknown_" ~ dimension_name ~ "'"]) }}
{% endmacro %}
