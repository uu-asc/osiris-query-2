select
    cols.column_name,
    cols.data_type,
    cols.data_length,
    cols.nullable is_nullable,
    cols.data_precision,
    cols.data_scale,
    stat.num_distinct distinct_values,
    stat.num_nulls null_count,
    case
        when stat.num_nulls is not null and tbls.num_rows > 0
        then stat.num_nulls / tbls.num_rows
    end null_percentage,
    tbls.num_rows total_rows,
    stat.last_analyzed stats_collected_date
from
    all_tab_columns cols

    left join all_tab_col_statistics stat
    on cols.table_name = stat.table_name
    and cols.column_name = stat.column_name

    left join all_tables tbls
    on cols.table_name = tbls.table_name
where cols.table_name = upper('{{ table }}')
order by cols.column_id
