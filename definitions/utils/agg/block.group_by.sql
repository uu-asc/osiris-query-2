
group by
{% if not totals %}
    {{ columns | join(', ') }}
{% elif cube_totals %}
    cube({{ columns | join(', ') }})
{% elif grouping_sets %}
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
    {# Default hierarchical totals when totals=true #}
    grouping sets (
        ({{ columns | join(', ') }}),
    {% for i in range(1, columns|length) %}
        ({{ columns[:-i] | join(', ') }}),
    {% endfor %}
        ()
    )
{% endif %}
