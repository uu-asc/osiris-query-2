{% set ns = namespace(error_message = none) %}
{% if aggfunc is none or aggfunc is undefined %}
    {% if not values %}
        {% set ns.error_message = "Neither `aggfunc` or `values` was set." %}
    {% else %}
        {% if values is string %}
            {% set ns.error_message = "If `values` is string, then `aggfunc` is required." %}
        {% elif values is mapping and 'aggfunc' not in values %}
            {% set ns.error_message = "If `values` is mapping, then mapping should contain `aggfunc` key or `aggfunc` should be set." %}
        {% elif values is iterable %}
            {% for item in values %}
                {% if item is string %}
                    {% set ns.error_message = "If any item in `values` is string, then `aggfunc` is required." %}
                {% elif item is mapping %}
                    {% if 'aggfunc' not in item %}
                        {% set ns.error_message = "If any item in `values` is a mapping, then mapping should contain `aggfunc` key or `aggfunc` should be set." %}
                    {% endif%}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}
{% endif %}
{% if not ns.error_message is none %}
{{ raise('AGG: ' ~ ns.error_message) }}
{% endif %}
