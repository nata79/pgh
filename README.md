# PGH

PGH is a CLI tool to help you manage your PostgreSQL database. It provides a list of utility commands to help you keep track of what's going on.

```
pgh $DATABASE_URL total_table_size
+-----------------------------+------------+
| name                        | size       |
|-----------------------------+------------|
| posts                       | 99 GB      |
| media                       | 99 GB      |
| comments                    | 11 GB      |
| users                       | 4511 MB    |
| oauth_access_tokens         | 4359 MB    |
| followers                   | 3403 MB    |
| devices                     | 2645 MB    |
| notifications               | 1821 MB    |
+-----------------------------+------------+
```

*Example calculates the size of each table including indexes.*

## Instalation

```
pip install pgh
```

## Usage

```
pgh DATABASE_URL COMMAND
```

Where `DATABASE_URL` should be a valid Postgres connection URI with the format:

```
postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
```

Example:

```
pgh postgres://andre@localhost/test index_sizes

+---------------------------+---------+
| name                      | size    |
|---------------------------+---------|
| h_table_id_index          | 4096 MB |
| b_table_id_index          | 3873 MB |
+---------------------------+---------+
```

## Commands

| Command | Description |
| --- | --- |
| bloat | show table and index bloat in your database ordered by most wasteful |
| blocking | display queries holding locks other queries are waiting to be released |
| cache\_hit | calculates your cache hit rate (effective databases are at 99% and up) |
| calls | show 10 most frequently called queries |
| index\_size | show the size of indexes, descending by size |
| index\_usage | calculates your index hit rate (effective databases are at 99% and up) |
| locks | display queries with active locks |
| long\_running\_queries | show all queries longer than five minutes by descending duration |
| outliers | show 10 queries that have longest execution time in aggregate |
| ps | view active queries with execution time |
| records\_rank | show all tables and the number of rows in each ordered by number of rows descending |
| seq\_scans | show the count of sequential scans by table descending by order |
| table\_size | show the size of the tables (excluding indexes), descending by size |
| total\_table\_size | show the size of the tables (including indexes), descending by size |
| unused\_indexes | show unused and almost unused indexes |

## Roadmap

- Integrate with AWS to to get the connection string from RDS (something like `pgh --rds command`);
- Integrate with Heroku API to get the connection string (something like `pgh --heroku command`);
- Implement `pull` command to copy data from a remote database to a target;
- - Implement `diagnose` command to generate a report of the general health of the database;
- Support connection parameters as specified [here](http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS).

## Acknowledgements

This tool is heavily based on the command tools built by [Heroku](http://heroku.com/). A lot of the commands and database queries present here are either inspired or directly taken from commands and database queries from [heroku cli](https://github.com/heroku/heroku) and [heroku pg extras](https://github.com/heroku/heroku-pg-extras).
