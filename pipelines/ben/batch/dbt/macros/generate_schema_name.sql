-- Override dbt's default schema naming so that +schema values are used as-is,
-- without being prefixed by the target schema name.
-- This allows staging models to land in `olist_silver_ben` and marts in `olist_gold_ben`.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
