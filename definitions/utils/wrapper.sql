{% if aggfunc or values %}
{% extends 'utils/agg.sql' %}
{% else %}
{% extends 'utils/cte.sql' %}
{% endif %}
/*
{% block comments %}
[parameters.cte]
select = { type = "str|list[str]|dict[str, str]", optional = true }
where = { type = "str|list[str]", optional = true }
order_by = { type = "str|list[str]", optional = true }
n = { type = "int", optional = true }
random = { type = "bool", default = false }
cte = { type = "str", default = "cte$" }
{% endblock comments %}
*/

{% from 'utils/cte/macro.where.jinja' import handle_where %}
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
    select *
    from {{ cte }}
    {% if where %}
    {{ handle_where(where) | indent(width=8) | trim('\n') }}
    {% endif %}
    {% if random %}
    order by dbms_random.value
    {% endif %}
    {% if n %}
    fetch first {{ n }} rows only
    {% endif %}
){% endblock query %}
