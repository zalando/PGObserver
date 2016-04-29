with q_last_data_collection_timestamp as (
    select
      max(tsd_timestamp) as tsd_timestamp
    from
      monitor_data.table_size_data
    where
        tsd_host_id = %(host_id)s
        and tsd_timestamp < now() - '60 seconds'::interval -- safety interval as data is not inserted in one tx by the gatherer
        and tsd_timestamp > current_date - 1
),
q_filter_condition as (
        select
          tsd_host_id as host_id,
          'seq_scans'::text as metric,
          t_schema||'.'||t_name as ident
          --pg_size_pretty(tsd_table_size) as size
        from
          monitor_data.table_size_data
          join
          monitor_data.tables on tsd_table_id = t_id
        where
          tsd_host_id = %(host_id)s
          and tsd_host_id = %(host_id)s
          and tsd_timestamp = (select tsd_timestamp from q_last_data_collection_timestamp)
          and tsd_table_size > 1000 * 1024^2   -- > 1000 MB
        --order by
        --  tsd_table_size desc
),
q_insert_new as (
    insert into olad.metric_ident_filter (mif_host_id, mif_metric, mif_ident)
    select
        %(host_id)s,
        'seq_scans',
        q_filter_condition.ident
    from q_filter_condition
    where not exists (
        select 1 from olad.metric_ident_filter where mif_host_id =q_filter_condition.host_id and mif_metric = q_filter_condition.metric and mif_ident = q_filter_condition.ident
    )
    returning *
),
q_delete_expired as (
    DELETE FROM
        olad.metric_ident_filter
    WHERE
        mif_host_id = %(host_id)s
        AND mif_metric = 'seq_scans'
        AND NOT EXISTS (
          select 1 from q_filter_condition
          where (q_filter_condition.host_id, q_filter_condition.metric, q_filter_condition.ident) = (mif_host_id, mif_metric, mif_ident)
        )
        AND NOT mif_is_permanent
    RETURNING *
)
select
    mif_ident as ident
from
    olad.metric_ident_filter
where
    (mif_host_id, mif_metric) = (%(host_id)s, 'seq_scans')
union
select
    mif_ident
from
    q_insert_new
except
select
    mif_ident
from
    q_delete_expired