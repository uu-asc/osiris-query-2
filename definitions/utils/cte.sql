/*
{% block comments %}{% endblock comments %}
*/
{% from 'utils/cte/macro.select.jinja' import handle_select %}
{% from 'utils/cte/macro.order_by.jinja' import handle_order_by %}
{% block query %}{% endblock query %}


{% if not select %}
select *
{% else %}
select
    {{ handle_select(select) | indent(width=4 ) | trim('\n') }}
{% endif %}
from query$
{% if order_by %}
{{ handle_order_by(order_by) }}
{% endif %}
