{% macro var(field) %}
{{ adapter.dispatch('var', 'dbt_expectations')(field) }}
{% endmacro %}

{% macro default__var(field) %}
var({{ field }})
{% endmacro %}

{% macro postgres__var(field) %}
var_samp({{ field }})
{% endmacro %}

{% macro doris__var(field) %}
var_samp({{ field }})
{% endmacro %}





