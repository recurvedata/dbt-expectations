{% macro sum(field) %}
{{ adapter.dispatch('sum', 'dbt_expectations')(field) }}
{% endmacro %}

{% macro default__sum(field) %}
sum({{ field }})
{% endmacro %}
