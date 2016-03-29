import psycopg2
import click
import getpass
from tabulate import tabulate

def print_results(cursor):
  headers = map(lambda column: column.name, cursor.description)
  click.echo(tabulate(cursor, headers=headers, tablefmt="psql"))

def database_command(fn):
  @click.command(fn.__name__)
  @click.pass_context
  def wrapper(ctx):
    connection = psycopg2.connect(ctx.obj)
    cursor = connection.cursor()

    print_results(fn(cursor))

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

@click.group()
@click.pass_context
@click.argument('database_url')
def cli(ctx, database_url):
  ctx.obj = database_url

cli.add_command(index_sizes)
cli.add_command(bloat)
cli.add_command(blocking)
