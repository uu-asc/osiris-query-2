{% set n_sample_values = n_sample_values | default(7) %}
with stats as (
    select
        count(*) total_rows,
        count({{ column }}) non_null_rows,
        count(distinct {{ column }}) unique_values,
        min({{ column }}) min_value,
        max({{ column }}) max_value,
        -- Percentage of rows that are null
        (count(*) - count({{ column }})) / count(*) null_percentage,
        -- Percentage of distinct values
        count(distinct {{ column }}) / nullif(count({{ column }}), 0) distinct_percentage,
        -- Most frequent value and its count
        max(freq_value) keep (
            dense_rank first
            order by freq_count desc
        ) most_frequent_value,
        max(freq_count) keep (
            dense_rank first
            order by freq_count desc
        ) most_frequent_count
    from (
        select
            {{ column }},
            count(*) over (partition by {{ column }}) freq_count,
            {{ column }} freq_value
        from {{ table }}
    ) t
),
sample_vals as (
    select listagg(val, ', ') within group (order by val) sample_list
    from (
        select distinct {{ column }} val
        from {{ table }}
        order by {{ column }}
        fetch first {{ n_sample_values }} rows only
    )
)
select
    s.*,
    case
        when s.unique_values <= {{ n_sample_values }} then (
            select listagg(distinct {{ column }}, ', ') within group (order by {{ column }})
            from {{ table }}
        )
        else (select sample_list || '...' from sample_vals)
    end sample_values
from stats s
