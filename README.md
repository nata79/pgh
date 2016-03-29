# PGH

PGH is a command tool to help you monitor and debug your PostgreSQL database.

## Instalation (comming soon)

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

| Implemented | Command | Description |
| --- | --- | --- |
| [x] | bloat | show table and index bloat in your database ordered by most wasteful |
| [x] | blocking | display queries holding locks other queries are waiting to be released |
| [x] | cache\_hit | calculates your cache hit rate (effective databases are at 99% and up) |
| [x] | calls | show 10 most frequently called queries |
| [ ] | diagnose | run diagnostics report on your database |
| [x] | index\_size | show the size of indexes, descending by size |
| [x] | index\_usage | calculates your index hit rate (effective databases are at 99% and up) |
| [x] | locks | display queries with active locks |
| [x] | long\_running\_queries | show all queries longer than five minutes by descending duration |
| [x] | outliers | show 10 queries that have longest execution time in aggregate |
| [x] | ps | view active queries with execution time |
| [ ] | pull TARGET_DATABASE | pull from your database to TARGET_DATABASE |
| [ ] | records\_rank | show all tables and the number of rows in each ordered by number of rows descending |
| [ ] | seq\_scans | show the count of sequential scans by table descending by order |
| [ ] | table\_size | show the size of the tables (excluding indexes), descending by size |
| [ ] | total\_table\_size | show the size of the tables (including indexes), descending by size |
| [ ] | unused_indexes | show unused and almost unused indexes |

## Roadmap

- Implement missing commands;
- Support connection parameters as specified [here](http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS);
- Integrate with Heroku API to get the connection string (something like `pgh --heroku command`);
- Integrate with AWS to to get the connection string from RDS (something like `pgh --rds command`);

## Acknowledgements

This tool is heavily based on the command tools built by [Heroku](http://heroku.com/). A lot of the commands and database queries present here are either inspired or directly taken from commands and database queries from [heroku cli](https://github.com/heroku/heroku) and [heroku pg extras](https://github.com/heroku/heroku-pg-extras).
