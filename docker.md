Install and run the postgres server:

```
$ docker run -d -v /tmp/data:/var/lib/postgresql/data --name some-postgres clkao/postgres-plv8:9.5
```

If you launch the container with only bash you have access to commands like `createdb`, `createuser` which are pretty handy.

Lets create a user:

```
$ docker run --link some-postgres:postgres --rm -ti clkao/postgres-plv8:9.5 bash
root@70eb107404d3:/#
```

Create a new superuser called `test_user` with password and echo resulting sql:

```
root@70eb107404d3:/# createuser -U postgres -h $POSTGRES_PORT_5432_TCP_ADDR -P -s -e test_user
Enter password for new role: woopwoop
Enter it again: woopwoop
CREATE ROLE test_user PASSWORD 'md5ae600751c0774680c9fe5fe7ee8e94c2' SUPERUSER CREATEDB CREATEROLE INHERIT LOGIN;
root@70eb107404d3:/#
```

Lets connect to the db and setup a database and a table so we have something to play around with. The prompt changes if login is successful.

```
root@70eb107404d3:/# psql -U postgres -h $POSTGRES_PORT_5432_TCP_ADDR
postgres=#
```

Create a database named `postgresdown`:

```
postgres=# CREATE DATABASE postgresdown;
CREATE DATABASE
```

Make sure it's there:

```
postgres=# \l
List of databases
Name     |  Owner   | Encoding |  Collate   |   Ctype    |   Access privileges
--------------+----------+----------+------------+------------+-----------------------
postgres     | postgres | UTF8     | en_US.utf8 | en_US.utf8 |
postgresdown | postgres | UTF8     | en_US.utf8 | en_US.utf8 |
template0    | postgres | UTF8     | en_US.utf8 | en_US.utf8 | =c/postgres          +
             |          |          |            |            | postgres=CTc/postgres
template1    | postgres | UTF8     | en_US.utf8 | en_US.utf8 | =c/postgres          +
             |          |          |            |            | postgres=CTc/postgres
(4 rows)
```

Connect to postgresdown database:

```
postgres=# \c postgresdown;
You are now connected to database "postgresdown" as user "postgres".
```

Create a table:

```
postgres=# CREATE TABLE data(key varchar PRIMARY KEY, value jsonb not null);
CREATE TABLE
```

