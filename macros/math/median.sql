{% macro median(field, model=None, expr_var=None) %}
{{ adapter.dispatch('median', 'dbt_expectations')(field, model, expr_var) }}
{% endmacro %}

{% macro default__median(field, model, expr_var) %}
    {{ dbt_expectations.percentile_cont(field, 0.5) }}
{% endmacro %}

{% macro mysql__median(field, model, expr_var) %}
(
select
	avg({{field}})
from
	(
	select
		{{field}},
		@{{expr_var}} := @{{expr_var}} + 1 row_num
	from
		{{model}},
		(
		select
			@{{expr_var}} := 0) t2
	order by
		{{field}}) t
where
	row_num between @{{expr_var}} / 2 and @{{expr_var}} / 2 + 1
)
{% endmacro %}
