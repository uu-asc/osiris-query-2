
group by
{% if not totals %}
    {{ columns | join(', ') }}
{% elif cube_totals %}
    cube({{ columns | join(', ') }})
{% elif grouping_sets %}
    grouping sets (
    {% for grouping_set in grouping_sets %}
        {% if grouping_set is string %}
        ({{ grouping_set }}){% if not loop.last %},{% endif %}
        {% else %}
        ({{ grouping_set | join(', ') }}){% if not loop.last %},{% endif %}
        {% endif %}
    {% endfor %}
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
