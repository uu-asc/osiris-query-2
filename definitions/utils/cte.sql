/*
{% block comments %}{% endblock comments %}
*/
{% block query %}{% endblock query %}

select *
from query$
{% if order_by %}
order by
    {{ order_by | join(', ') }}
{% endif %}
