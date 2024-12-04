{% test expect_column_median_to_be_between(model, column_name,
                                                    min_value=None,
                                                    max_value=None,
                                                    group_by=None,
                                                    row_condition=None,
                                                    strictly=False
                                                    ) %}


{{ adapter.dispatch('median_expr_between', 'dbt_expectations') (model,
                                                                column_name,
                                                                min_value,
                                                                max_value,
                                                                group_by,
                                                                row_condition,
                                                                strictly
                                                                ) }}
{% endtest %}

{% macro default__median_expr_between(model, column_name, min_value, max_value, group_by, row_condition, strictly) %}
{% set expression %}
{{ dbt_expectations.median(model, column_name, 'a') }}
{% endset %}
{{ dbt_expectations.expression_between(model,
                                        expression=expression,
                                        min_value=min_value,
                                        max_value=max_value,
                                        group_by_columns=group_by,
                                        row_condition=row_condition,
                                        strictly=strictly
                                        ) }}
{% endmacro %}

{% macro mysql__median_expr_between(model, column_name, min_value, max_value, group_by, row_condition, strictly) %}
{% set expression_a %}
{{ dbt_expectations.median(model, column_name, 'a') }}
{% endset %}
{% set expression_b %}
{{ dbt_expectations.median(model, column_name, 'b') }}
{% endset %}
{%- if min_value is none and max_value is none -%}
{{ exceptions.raise_compiler_error(
    "You have to provide either a min_value, max_value or both."
) }}
{%- endif -%}

{%- set strict_operator = "" if strictly else "=" -%}

{% set expression_min_max %}
( 1=1
{%- if min_value is not none %} and {{ expression_a | trim }} >{{ strict_operator }} {{ min_value }}{% endif %}
{%- if max_value is not none %} and {{ expression_b | trim }} <{{ strict_operator }} {{ max_value }}{% endif %}
)
{% endset %}

{{ dbt_expectations.expression_is_true(model,
                                        expression=expression_min_max,
                                        group_by_columns=group_by,
                                        row_condition=row_condition)
                                        }}
{% endmacro %}
