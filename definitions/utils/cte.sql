/*
{% block comments %}{% endblock comments %}
*/
{% block query %}{% endblock query %}

{% if select %}
select
    {{ select | join(',\n') | indent(width=4) }}
{% else %}
select *
{% endif %}
from query$
{% if order_by %}
order by
    {{ order_by | join(', ') }}
{% endif %}
