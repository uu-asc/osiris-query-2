{% if aggfunc or values %}
{% extends 'utils/agg.sql' %}
{% else %}
{% extends 'utils/cte.sql' %}
{% endif %}
/*
{% block comments %}
[parameters]
select = { type = "list[str]", optional = true }
where = { type = "list[str]", optional = true }
order_by = { type = "list[str]", optional = true }
n = { type = "int", optional = true }
random = { type = "bool", default = false }
cte = { type = "str", default = "cte$" }
{% endblock comments %}
*/

{% set cte = cte | default('cte$') %}
{% set random = random | default(false) %}
{% block query %}
{{ body | trim('\n') }}{% if has_cte %},{% endif %}


{% if not has_cte %}with {% endif %}cte$ as (
{% filter indent(width=4, first=True) %}
{{ main_statement }}
{% endfilter %}
),

query$ as (
    {% if select %}
    select
        {{ select | join(',\n') | indent(width=8) }}
    {% else %}
    select *
    {% endif %}
    from {{ cte }}
    where
        1 = 1
        {% if where %}
        {% for criterium in where %}
        and {{ criterium }}
        {% endfor %}
        {% endif %}
    {% if random %}
    order by dbms_random.value
    {% endif %}
    {% if n %}
    fetch first {{ n }} rows only
    {% endif %}
){% endblock query %}
