{% macro min(field) %}
{{ adapter.dispatch('min', 'dbt_expectations')(field) }}
{% endmacro %}

{% macro default__min(field) %}
min({{ field }})
{% endmacro %}
