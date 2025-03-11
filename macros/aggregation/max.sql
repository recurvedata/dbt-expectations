{% macro max(field) %}
{{ adapter.dispatch('max', 'dbt_expectations')(field) }}
{% endmacro %}

{% macro default__max(field) %}
max({{ field }})
{% endmacro %}
