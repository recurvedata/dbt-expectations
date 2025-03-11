{%- macro count_distinct(field) -%}
{{ adapter.dispatch('count_distinct', 'dbt_expectations')(field) }}
{%- endmacro -%}

{%- macro default__count_distinct(field) -%}
count(distinct {{ field }})
{%- endmacro -%}

