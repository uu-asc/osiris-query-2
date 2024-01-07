/*
https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/ALL_TABLES.html#GUID-6823CD28-0681-468E-950B-966C6F71325D
*/
select
    owner,
    table_name,
    status,
    pct_free,
    pct_used,
    logging,
    backed_up,
    num_rows,
    avg_row_len,
    instances,
    cache,
    table_lock,
    sample_size,
    last_analyzed,
    duplicated,
    has_sensitive_column,
    admit_null,
    logical_replication
from all_tables
where
    owner not in (
        'ANONYMOUS','CTXSYS','DBSNMP','EXFSYS', 'LBACSYS', 'MDSYS', 'MGMT_VIEW','OLAPSYS','OWBSYS','ORDPLUGINS', 'ORDSYS','OUTLN', 'SI_INFORMTN_SCHEMA','SYS','SYSMAN','SYSTEM','TSMSYS','WK_TEST','WKSYS', 'WKPROXY','WMSYS','XDB','APEX_040000', 'APEX_PUBLIC_USER','DIP', 'FLOWS_30000','FLOWS_FILES','MDDATA', 'ORACLE_OCM', 'XS$NULL', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR', 'PUBLIC'
    )
order by
    owner,
    table_name
