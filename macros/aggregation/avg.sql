{% macro avg(field) %}
{{ adapter.dispatch('avg', 'dbt_expectations')(field) }}
{% endmacro %}

{% macro default__avg(field) %}
avg({{ field }})
{% endmacro %}
