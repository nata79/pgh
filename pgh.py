import psycopg2
import click
import getpass
from tabulate import tabulate

def print_results(cursor):
  headers = map(lambda column: column.name, cursor.description)
  click.echo(tabulate(cursor, headers=headers, tablefmt="psql"))

def pg_stat_statement_available(cursor):
  sql = """
    SELECT exists(
      SELECT 1 FROM pg_extension e LEFT JOIN pg_namespace n ON n.oid = e.extnamespace
      WHERE e.extname='pg_stat_statements' AND n.nspname = 'public'
    ) AS available
  """
  cursor.execute(sql)
  return cursor.fetchone()[0]

def database_command(fn):
  @click.command(fn.__name__)
  @click.pass_context
  def wrapper(ctx):
    connection = psycopg2.connect(ctx.obj)
    cursor = connection.cursor()

    results = fn(cursor)

    if results:
      print_results(results)

    cursor.close()
    connection.close()

  return wrapper

@database_command
def index_sizes(cursor):
  sql = """
    SELECT c.relname AS name,
      pg_size_pretty(sum(c.relpages::bigint*8192)::bigint) AS size
    FROM pg_class c
    LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace)
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND n.nspname !~ '^pg_toast'
      AND c.relkind = 'i'
    GROUP BY c.relname
    ORDER BY sum(c.relpages) DESC;
  """

  cursor.execute(sql)

  return cursor

@database_command
def bloat(cursor):
  sql = """
    WITH constants AS (
      SELECT current_setting('block_size')::numeric AS bs, 23 AS hdr, 4 AS ma
    ), bloat_info AS (
      SELECT ma,bs,schemaname,tablename,
        (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
        (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
      FROM (
        SELECT schemaname, tablename, hdr, ma, bs,
          SUM((1-null_frac)*avg_width) AS datawidth,
          MAX(null_frac) AS maxfracsum,
          hdr+(
            SELECT 1+count(*)/8 FROM pg_stats s2
            WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
          ) AS nullhdr
        FROM pg_stats s, constants
        GROUP BY 1,2,3,4,5
      ) AS foo
    ), table_bloat AS (
      SELECT schemaname, tablename, cc.relpages, bs,
        CEIL((cc.reltuples*((datahdr+ma-
          (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta
      FROM bloat_info
      JOIN pg_class cc ON cc.relname = bloat_info.tablename
      JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = bloat_info.schemaname AND nn.nspname <> 'information_schema'
    ), index_bloat AS (
      SELECT schemaname, tablename, bs,
        COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
        COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
      FROM bloat_info
      JOIN pg_class cc ON cc.relname = bloat_info.tablename
      JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = bloat_info.schemaname AND nn.nspname <> 'information_schema'
      JOIN pg_index i ON indrelid = cc.oid
      JOIN pg_class c2 ON c2.oid = i.indexrelid
    ) SELECT
        type, schemaname, object_name, bloat, pg_size_pretty(raw_waste) as waste
      FROM
        (
          SELECT 'table' as type, schemaname, tablename as object_name,
            ROUND(CASE WHEN otta=0 THEN 0.0 ELSE table_bloat.relpages/otta::numeric END,1) AS bloat,
            CASE WHEN relpages < otta THEN '0' ELSE (bs*(table_bloat.relpages-otta)::bigint)::bigint END AS raw_waste
          FROM table_bloat
          UNION SELECT 'index' as type, schemaname, tablename || '::' || iname as object_name,
            ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) AS bloat,
            CASE WHEN ipages < iotta THEN '0' ELSE (bs*(ipages-iotta))::bigint END AS raw_waste
          FROM index_bloat
        ) bloat_summary
      ORDER BY raw_waste DESC, bloat DESC
  """

  cursor.execute(sql)

  return cursor

@database_command
def blocking(cursor):
  sql = """
    SELECT bl.pid AS blocked_pid,
      ka.query AS blocking_statement,
      now() - ka.query_start AS blocking_duration,
      kl.pid AS blocking_pid,
      a.query AS blocked_statement,
      now() - a.query_start AS blocked_duration
    FROM pg_catalog.pg_locks bl
    JOIN pg_catalog.pg_stat_activity a ON bl.pid = a.pid
      JOIN pg_catalog.pg_locks kl
        JOIN pg_catalog.pg_stat_activity ka ON kl.pid = ka.pid
      ON bl.transactionid = kl.transactionid AND bl.pid != kl.pid
    WHERE NOT bl.granted
  """

  cursor.execute(sql)

  return cursor

@database_command
def cache_hit(cursor):
  sql = """
    SELECT 'index hit rate' AS name,
      (sum(idx_blks_hit)) / nullif(sum(idx_blks_hit + idx_blks_read),0) AS ratio
    FROM pg_statio_user_indexes
    UNION ALL
      SELECT 'table hit rate' AS name,
        sum(heap_blks_hit) / nullif(sum(heap_blks_hit) + sum(heap_blks_read),0) AS ratio
      FROM pg_statio_user_tables
  """

  cursor.execute(sql)

  return cursor

@database_command
def calls(cursor):
  if pg_stat_statement_available(cursor):
    sql = """
      SELECT query AS qry,
        interval '1 millisecond' * total_time AS exec_time,
        to_char((total_time/sum(total_time) OVER()) * 100, 'FM90D0') || '%'  AS prop_exec_time,
        to_char(calls, 'FM999G999G990') AS ncalls,
        interval '1 millisecond' * (blk_read_time + blk_write_time) AS sync_io_time
      FROM pg_stat_statements WHERE userid = (SELECT usesysid FROM pg_user WHERE usename = current_user LIMIT 1)
      ORDER BY calls DESC LIMIT 10
    """
    cursor.execute(sql)

    return cursor
  else:
    click.echo("pg_stat_statements extension need to be installed in the public schema first.")
    click.echo("This extension is only available on Postgres versions 9.2 or greater. You can install it by running:")
    click.echo("\n\tCREATE EXTENSION pg_stat_statements;\n\n")

@database_command
def index_usage(cursor):
  sql = """
    SELECT relname,
      CASE idx_scan
        WHEN 0 THEN 'Insufficient data'
        ELSE (100 * idx_scan / (seq_scan + idx_scan))::text
      END percent_of_times_index_used
    FROM pg_stat_user_tables
    ORDER BY percent_of_times_index_used ASC
  """

  cursor.execute(sql)

  return cursor

@database_command
def locks(cursor):
  sql = """
    SELECT pg_stat_activity.pid, pg_class.relname, pg_locks.transactionid,
      pg_locks.granted, pg_stat_activity.query AS query_snippet,
       age(now(),pg_stat_activity.query_start) AS "age"
    FROM pg_stat_activity,pg_locks
    LEFT OUTER JOIN pg_class ON (pg_locks.relation = pg_class.oid)
    WHERE pg_stat_activity.query <> '<insufficient privilege>'
      AND pg_locks.pid = pg_stat_activity.pid
      AND pg_locks.mode = 'ExclusiveLock'
      AND pg_stat_activity.pid <> pg_backend_pid() order by query_start
  """

  cursor.execute(sql)

  return cursor

@database_command
def long_running_queries(cursor):
  sql = """
    SELECT
      pid, now() - pg_stat_activity.query_start AS duration, query
    FROM
      pg_stat_activity
    WHERE pg_stat_activity.query <> ''::text
      AND state <> 'idle'
      AND now() - pg_stat_activity.query_start > interval '5 minutes'
    ORDER BY now() - pg_stat_activity.query_start DESC
  """

  cursor.execute(sql)

  return cursor

@database_command
def outliers(cursor):
  if pg_stat_statement_available(cursor):
    sql = """
      SELECT interval '1 millisecond' * total_time AS total_exec_time,
        to_char((total_time/sum(total_time) OVER()) * 100, 'FM90D0') || '%'  AS prop_exec_time,
        to_char(calls, 'FM999G999G999G990') AS ncalls,
        interval '1 millisecond' * (blk_read_time + blk_write_time) AS sync_io_time, query
      FROM pg_stat_statements
      WHERE userid = (SELECT usesysid FROM pg_user WHERE usename = current_user LIMIT 1)
      ORDER BY total_time DESC LIMIT 10
    """

    cursor.execute(sql)

    return cursor
  else:
    click.echo("pg_stat_statements extension need to be installed in the public schema first.")
    click.echo("This extension is only available on Postgres versions 9.2 or greater. You can install it by running:")
    click.echo("\n\tCREATE EXTENSION pg_stat_statements;\n\n")

@database_command
def ps(cursor):
  sql = """
    SELECT pid, state, application_name AS source,
      age(now(),xact_start) AS running_for, waiting, query
    FROM pg_stat_activity
    WHERE query <> '<insufficient privilege>' AND state <> 'idle'
      AND pid <> pg_backend_pid()
    ORDER BY query_start DESC
  """

  cursor.execute(sql)

  return cursor

@database_command
def records_rank(cursor):
  sql = """
    SELECT relname AS name, n_live_tup AS estimated_count
    FROM pg_stat_user_tables
    ORDER BY n_live_tup DESC
  """

  cursor.execute(sql)

  return cursor

@database_command
def seq_scans(cursor):
  sql = """
    SELECT relname AS name, seq_scan as count
    FROM pg_stat_user_tables
    ORDER BY seq_scan DESC
  """

  cursor.execute(sql)

  return cursor

@database_command
def table_size(cursor):
  sql = """
    SELECT c.relname AS name,
        pg_size_pretty(pg_table_size(c.oid)) AS size
    FROM pg_class c
    LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace)
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND n.nspname !~ '^pg_toast'
      AND c.relkind='r'
    ORDER BY pg_table_size(c.oid) DESC
  """

  cursor.execute(sql)

  return cursor

@database_command
def total_table_size(cursor):
  sql = """
    SELECT c.relname AS name,
        pg_size_pretty(pg_total_relation_size(c.oid)) AS size
    FROM pg_class c
    LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace)
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND n.nspname !~ '^pg_toast'
      AND c.relkind='r'
    ORDER BY pg_total_relation_size(c.oid) DESC
  """

  cursor.execute(sql)

  return cursor

@database_command
def unused_indexes(cursor):
  sql = """
    SELECT schemaname || '.' || relname AS table, indexrelname AS index,
      pg_size_pretty(pg_relation_size(i.indexrelid)) AS index_size,
      idx_scan as index_scans
    FROM pg_stat_user_indexes ui
    JOIN pg_index i ON ui.indexrelid = i.indexrelid
    WHERE NOT indisunique AND idx_scan < 50 AND pg_relation_size(relid) > 5 * 8192
    ORDER BY
      pg_relation_size(i.indexrelid) / nullif(idx_scan, 0) DESC NULLS FIRST,
      pg_relation_size(i.indexrelid) DESC
  """

  cursor.execute(sql)

  return cursor

@click.group()
@click.pass_context
@click.argument('database_url')
def cli(ctx, database_url):
  ctx.obj = database_url

cli.add_command(index_sizes)
cli.add_command(bloat)
cli.add_command(blocking)
cli.add_command(cache_hit)
cli.add_command(calls)
cli.add_command(index_usage)
cli.add_command(locks)
cli.add_command(long_running_queries)
cli.add_command(outliers)
cli.add_command(ps)
cli.add_command(records_rank)
cli.add_command(seq_scans)
cli.add_command(table_size)
cli.add_command(total_table_size)
cli.add_command(unused_indexes)
