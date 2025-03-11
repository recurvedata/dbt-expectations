{%- test expect_table_aggregation_to_equal_other_table(model,
                                                expression,
                                                compare_model,
                                                compare_expression=None,
                                                group_by=None,
                                                compare_group_by=None,
                                                row_condition=None,
                                                compare_row_condition=None,
                                                tolerance=0.0,
                                                tolerance_percent=None
                                                ) -%}
{% if expression is not none %}
    {% set processed_expression = dbt_expectations.expr2macro(expression) %}
{% else %}
    {% set processed_expression = expression %}
{% endif %}


{% if compare_expression is not none %}
    {% set processed_compare_expression = dbt_expectations.expr2macro(compare_expression) %}
{% else %}
    {% set processed_compare_expression = compare_expression %}
{% endif %}


{{ dbt_expectations.test_equal_expression(
    model,
    expression=processed_expression,
    compare_model=compare_model,
    compare_expression=processed_compare_expression,
    group_by=group_by,
    compare_group_by=compare_group_by,
    row_condition=row_condition,
    compare_row_condition=compare_row_condition,
    tolerance=tolerance,
    tolerance_percent=tolerance_percent
) }}

{%- endtest -%}
