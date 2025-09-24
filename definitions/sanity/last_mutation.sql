with max_mutation_date as (
    select max({{ mutation_date_column }}) max_mutation_date from {{ table }}
)

select
    '{{ table }}' "table",
    max_mutation_date,
    (sysdate - max_mutation_date) / 24 n_hours,
    {{ threshold_in_hours }} threshold_in_hours,
    case
        when ((sysdate - max_mutation_date) / 24) < {{ threshold_in_hours }} then 'Y'
        else 'N'
    end below_threshold
from max_mutation_date
