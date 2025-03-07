{%- macro stdevp(field) -%}
{{ adapter.dispatch('stdevp', 'dbt_expectations')(field) }}
{%- endmacro -%}

{%- macro default__stdevp(field) -%}
stddev_pop({{ field }})
{%- endmacro -%}

{%- macro sqlserver__stdevp(field) -%}
stdevp({{ field }})
{%- endmacro -%}