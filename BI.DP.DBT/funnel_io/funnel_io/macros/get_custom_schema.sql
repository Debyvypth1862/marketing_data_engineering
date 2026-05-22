-- see https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas/#an-alternative-pattern-for-generating-schema-names
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
