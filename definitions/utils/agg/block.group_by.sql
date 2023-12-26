
group by
{% if not totals %}
    {{ columns | join(', ') }}
{% else %}
    {% if grouping_sets %}
    grouping sets (
    {% for grouping_set in grouping_sets %}
        {% if grouping_set is string %}
        ({{ grouping_set }}),
        {% else %}
        ({{ grouping_set | join(', ') }}),
        {% endif %}
    {% endfor %}
        ()
    )
    {% else %}
    cube({{ columns | join(', ') }})
    {% endif %}
{% endif %}
