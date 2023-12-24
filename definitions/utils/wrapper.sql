/*
{% block comments %}
[parameters]
select = { type = "list[str]", optional = true }
where = { type = "list[str]", optional = true }
order_by = { type = "list[str]", optional = true }
n = { type = "int", optional = true }
random = { type = "bool", optional = true, default = false }
cte = { type = "str", optional = true, default = "cte$" }
{% endblock comments %}
*/
{% set cte = cte | default('cte$') %}
{% set random = random | default(false) %}
{{ body | trim('\n') }}{% if has_cte %},{% endif %}


{% if not has_cte %}with {% endif %}cte$ as (
{% filter indent(width=4, first=True) -%}
{{ main_statement }}
{%- endfilter %}
),

query$ as (
    select
        {% if select %}
        {{ select | join(', ') }}
        {% else %}
        *
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
)

select *
from query$
{% if order_by %}
order by
    {{ order_by | join(', ') }}
{% endif %}
