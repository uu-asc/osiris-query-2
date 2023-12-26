
order by
{% if not totals %}
    {{ columns | join(', ') }}
{% else %}
    {% for column in columns %}
    grouping({{ column }}),
    {{ column }}{% if not loop.last %},
    {% endif %}
    {% endfor %}
{% endif %}
