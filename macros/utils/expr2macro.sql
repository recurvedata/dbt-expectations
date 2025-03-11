{% macro expr2macro(expr) %}
    {#
    Convert the aggregate expression string to the corresponding aggregate function call
    
    Supported:
    1. Single function call: min(amount)
    2. Function with multiple parameters: percentile(amount, 0.9)
    3. Compound expression: median(salary), min(salary)
    #}
    
    
    {% if "," in expr and "(" in expr and ")" in expr and expr.find(",") < expr.find("(") %}
        {{ return(dbt_expectations.process_single_function(expr)) }}
    {% else %}
        {% set has_multiple_functions = false %}
        {% set open_count = 0 %}
        {% set close_count = 0 %}
        {% set comma_positions = [] %}
        
        {% for i in range(expr|length) %}
            {% set char = expr[i] %}
            {% if char == "(" %}
                {% set open_count = open_count + 1 %}
            {% elif char == ")" %}
                {% set close_count = close_count + 1 %}
            {% elif char == "," and open_count == close_count %}
                {% do comma_positions.append(i) %}
                {% set has_multiple_functions = true %}
            {% endif %}
        {% endfor %}
        
        {% if has_multiple_functions %}
            
            {% set result_parts = [] %}
            {% set start_pos = 0 %}
            
            {% for pos in comma_positions %}
                {% set func_expr = expr[start_pos:pos] | trim %}
                {% do result_parts.append(dbt_expectations.process_single_function(func_expr)) %}
                {% set start_pos = pos + 1 %}
            {% endfor %}
            
            {% set last_func = expr[start_pos:] | trim %}
            {% do result_parts.append(dbt_expectations.process_single_function(last_func)) %}
            
            {{ return(result_parts | join(", ")) }}
        {% else %}
            {{ return(dbt_expectations.process_single_function(expr)) }}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro process_single_function(expr) %}
    {% if "(" not in expr or ")" not in expr %}
        {{ return(expr) }}
    {% endif %}
    
    {% set func_name = expr.split("(")[0] | trim %}
    {% set args_str = expr.split("(")[1].split(")")[0] %}
    
    {% set args = [] %}
    {% for arg in args_str.split(",") %}
        {% do args.append(arg | trim) %}
    {% endfor %}
    
    
    {% if func_name == "min" %}
        {{ return(dbt_expectations.min(args[0])) }}
    {% elif func_name == "max" %}
        {{ return(dbt_expectations.max(args[0])) }}
    {% elif func_name == "sum" %}
        {{ return(dbt_expectations.sum(args[0])) }}
    {% elif func_name == "avg" %}
        {{ return(dbt_expectations.avg(args[0])) }}
    {% elif func_name == "median" %}
        {% if args | length == 3 %}
            {{ return(dbt_expectations.median(args[0], args[1], args[2])) }}
        {% elif args | length == 2 %}
            {{ return(dbt_expectations.median(args[0], args[1])) }}
        {% else %}
            {{ return(dbt_expectations.median(args[0])) }}
        {% endif %}
    {% elif func_name == "count" %}
        {{ return(dbt_expectations.count(args[0])) }}
    {% elif func_name == "count_distinct" %}
        {{ return(dbt_expectations.count_distinct(args[0])) }}
    {% elif func_name == "var_samp" %}
        {{ return(dbt_expectations.var_samp(args[0])) }}
    {% elif func_name == "stdevp" %}
        {{ return(dbt_expectations.stdevp(args[0])) }}
    {% else %}
        {{ return(expr) }}
    {% endif %}
{% endmacro %}