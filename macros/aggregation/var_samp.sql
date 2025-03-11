{% macro var_samp(field) %}
    {{ adapter.dispatch('var_samp', 'dbt_expectations')(field) }}
{% endmacro %}

{% macro default__var_samp(field) %}
var_samp({{ field }})
{% endmacro %}
