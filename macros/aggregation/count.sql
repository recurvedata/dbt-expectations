{% macro count(field) %}
{{ adapter.dispatch('count', 'dbt_expectations')(field) }}
{% endmacro %}

{% macro default__count(field) %}
count({{ field }})
{% endmacro %}
