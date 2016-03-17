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

@click.group()
@click.pass_context
@click.argument('database_url')
def cli(ctx, database_url):
  ctx.obj = database_url

cli.add_command(index_sizes)
