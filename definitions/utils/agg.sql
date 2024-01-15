/*
{% block comments %}{% endblock comments %}

[parameters.aggregation]
aggfunc = { type = "str", optional = true }
distinct = { type = "bool", default = false }
columns = { type = "list[str]", optional = true }
values = { type = "str|list[str]|list[dict[str,str]]", optional = true }
label_val = { type = "str", default = "aantal" }
keep_na = { type = "bool", default = false }
label_na = { type = "str", default = "<NA>" }
totals = { type = "bool", default = false }
grouping_sets = { type = "str|list[str]", optional = true }
label_totals = { type = "str", default = "Total" }
*/
{#
    if we don't keep null values,
    then we filter them out in the where clause
    of the query$ table
 #}
{# test if arguments are filled out correctly #}
{% include 'utils/agg/block.assert.sql' %}
{% set aggfunc = aggfunc | default(none) %}
{% set distinct = distinct | default(false) %}
{# coerce `columns` to list #}
{% if columns is string %}
{% set columns = [columns] %}
{% endif %}
{# coerce `where` to list #}
{% if where is string %}
{% set where = [where] %}
{% elif where is undefined or where is none %}
{% set where = [] %}
{% endif %}
{# coerce `having` to list #}
{% if having is string %}
{% set having = [having] %}
{% endif %}
{# handle `keep_na` #}
{% set keep_na = keep_na | default(false) %}
{% if not keep_na %}
    {# append "col is not null" for all `columns` to where clause #}
    {% for column in columns %}
        {% set _ = where.append(column ~ " is not null") %}
    {% endfor %}
{% endif %}
{# set global values (to be used in macros below) #}
{% set LABEL_NA = label_na | default('<NA>') %}
{% set LABEL_VAL = label_val | default(aggfunc) %}
{% set LABEL_TOTALS = label_totals | default("Total") %}
{% from 'utils/agg/macro.values.jinja' import handle_values with context %}
{% from 'utils/agg/macro.colnames.jinja' import handle_colnames with context %}
{# render template #}
{% block query %}{% endblock query %},

{#
    add aggregation operations to the query
    and add $group columns in order to track
    what values are subaggregates (needed for
    labelling null/NA and totals correctly)
#}
agg$ as (
    select
    {% if columns %}
        {% for column in columns %}
        {{ column }},
        grouping({{ column }}) {{ column }}$group,
        {% endfor %}
    {% endif %}
        {{ handle_values(
            aggfunc,
            values,
            distinct=distinct) | indent(width=8) | trim('\n') }}
    from query$
{% if columns %}
{% filter indent(width=4, first=true) | trim('\n') %}
{% include 'utils/agg/block.group_by.sql' with context %}
{% endfilter %}
{% endif %}
{% if having %}{{ '\n' }}
    having
        1 = 1
        {% for criterium in having %}
        and {{ criterium }}
        {% endfor %}
{% endif %}
{% if columns %}
{% filter indent(width=4, first=true) | trim('\n') %}
{% include 'utils/agg/block.order_by.sql' with context %}
{% endfilter %}
{% endif %}
)

{#
    finally, name total and null rows correctly
    and remove grouping columns from output
 #}
select
    {% if columns %}
    {% for column in columns %}
    case
        when {{ column }} is not null then {{ column }}
        when {{ column}}$group = 0 then '{{ LABEL_NA }}'
        else '{{ LABEL_TOTALS }}'
    end {{ column }},
    {% endfor %}
    {% endif %}
    {% filter indent(width=4) %}
    {{ handle_colnames(values) }}
    {% endfilter %}
from agg$
