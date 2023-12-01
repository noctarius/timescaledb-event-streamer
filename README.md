# timescaledb-event-streamer

`timescaledb-event-streamer` is a command line program to create a stream of
CDC (Change Data Capture) TimescaleDB™ Hypertable and Continuous Aggregate
events from a PostgreSQL installation running the TimescaleDB extension.
In addition, it also supports capturing events of vanilla PostgreSQL tables.

Change Data Capture is a technology where insert, update, delete and similar
operations inside the database generate a corresponding set of events, which
are commonly distributed through a messaging connector, such as Kafka, NATS,
or similar.

_**Attention:** This is not an official Timescale™ project, but just developed
by a person who used to work for Timescale._

_**Trademark information:** Timescale (TIMESCALE) and TimescaleDB (TIMESCALEDB)
are registered trademarks of Timescale, Inc._

# Why not just Debezium?

- Slightly outdated:<br/>
_While [Debezium](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
already supports PostgreSQL, the implementation doesn't really support the internals
of TimescaleDB, most specifically the way data is chunked or partitioned. It is
possible to use Debezium to capture change events of the actual chunks itself,
but without the catalog handling. This means that every chunk would emit changes on
its own, but with no reference to its parent hypertable. The `timescaledb-event-streamer`
changes this, by handling the catalog updates and resolving the parent hypertable
before emitting the events._
- Current status:<br/>
_While [Debezium](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
already supports PostgreSQL and TimescaleDB, the implementation for TimescaleDB is very
basic at this point may never catch up to this tool, which is much more specific. It is
possible to use Debezium to catch changes on chunks (and the corresponding hypertable name
will be provided in the Kafka header), but changes are still happening on chunk-individual
streams. That said, it is still necessary to implement the handling / merging of chunks
streams into their hypertable parent on the application side (behind Kafka). While there
may be use-cases for this, the more general use-case would be to "ignore" that the data
actually come from a hypertable, since, in most cases, this information won't mean
anything to the target application._

# Getting Started

## Installing prebuilt Packages

To automatically download the latest version into the current directory, the repository
provides a bash script. This script supports downloading on Linux, MacOS, and FreeBSD.

When you want to use Windows, please download the zip file manually from the repository's
download page at https://github.com/noctarius/timescaledb-event-streamer/releases/latest
which automatically redirects to the latest available download.

```bash
curl -L https://raw.github.com/noctarius/timescaledb-event-streamer/master/get.sh | bash
```

## Using Docker

To run `timescaledb-event-streamer` via Docker, access the latest version of the Docker image
using `ghcr.io/noctarius/timescaledb-event-streamer:latest`.

The environment variable `TIMESCALEDB_EVENT_STREAMER_CONFIG` provides the location of the
configuration file to the tool. This file must be mounted into the running container at the
given location.

The following command shows an example on how to run it.

```bash
docker run \
  --name timescaledb-event-streamer \
  -v ./config.tml:/etc/config.toml \
  -e TIMESCALEDB_EVENT_STREAMER_CONFIG='/etc/config.toml' \
  ghcr.io/noctarius/timescaledb-event-streamer:latest
```

## Installation from Source

`timescaledb-event-streamer` requires the [Go runtime (version 1.20+)](https://go.dev/doc/install)
to be installed. With this requirement satisfied, the installation can be
kicked off using:

```bash
$ go install github.com/noctarius/timescaledb-event-streamer/cmd/timescaledb-event-streamer@latest
```

## Before you start

Before using the program, a configuration file needs to be created. An example
configuration can be
found [here](https://raw.githubusercontent.com/noctarius/timescaledb-event-streamer/main/config.example.toml).

For a full reference of the existing configuration options, see the [Configuration](#configuration)
section.

Starting from TimescaleDB 2.12, the extension provides the possibility to use PostgreSQL 14+
logical replication messages to mark the start and end of decompressions (due to insert,
update, delete in compressed chunks). If you are on PG14+ and use TimescaleDB 2.12+, you
should enable those markers.

In your `postgresql.conf`, set the following line:
```plain
timescaledb.enable_decompression_logrep_markers=on
```

This property cannot be set at runtime! After changing this property you have to restart
your PostgreSQL server instance.

## Support for non-privileged users (non-superuser roles)

In addition to the program itself, a function has to be installed into the database which will
be used to generate change events from. The function is used to create the initial logical
replication publication, since the internal catalog tables from TimescaleDB are owned by
the `postgres` user, as required for a trusted extension.

The function needs to be created by the `postgres` user when added to the database and runs
in `security definer` mode, inheriting the permissions and ownership of the defining user before
giving up the increased permissions voluntarily.

To install the function, please run the following code snippet as `postgres` user against your
database. `timescaledb-event-streamer` will automatically use it when starting up. If the
function is not available the startup will fail!

The function can be found in the
[github repository](https://raw.githubusercontent.com/noctarius/timescaledb-event-streamer/main/create_timescaledb_catalog_publication.sql).

To install the function you can use as following:

```bash
$ wget https://raw.githubusercontent.com/noctarius/timescaledb-event-streamer/main/create_timescaledb_catalog_publication.sql
$ psql "<connstring>" < create_timescaledb_catalog_publication.sql
```

## Creating the Replication User

The user used for logical replication needs to have owner permission to the entities, no matter if
Hypertable, Continuous Aggregate (Materialized Hypertable), or vanilla PostgreSQL table.

That said, when creating a user for logical replication, the new user has to be granted the owner
as a role, as well as `REPLICATION` attribute set.

The following example assumes the new replication user to be named `repl_user` and the entity's
owner to be named `pg_user`.

```sql
CREATE ROLE repl_user LOGIN REPLICATION ENCRYPTED PASSWORD '<<password>>';
GRANT pg_user TO repl_user;
```

In your configuration, you would set `repl_user` as the user for replication, as well as the chosen
password.

```toml
postgresql.connection = 'postgres://repl_user@<<hostname>>:<<port>>/<<database>>'
postgresql.password = '<<password>>'
```

## Using timescaledb-event-streamer

After creating a configuration file, `timescaledb-event-streamer` can be executed
with the following command:

```bash
$ timescaledb-event-streamer -config=./config.toml
```

The tool will connect to your TimescaleDB database, and start replicating incoming
events.

# Supported PostgreSQL Data Type

`timescaledb-event-streamer` supports almost all default data types available in
PostgreSQL, which some exception that shouldn't be seen in real-world scenarios.
Apart from that, it lacks support for structural data types (composite types,
and similar). Support is worked on though.

The following list describes all available data types and their schema type
mappings. For Array data types, the element type of array children is also
named. Furthermore, starting with 0.4.0, `timescaledb-event-streamer`
supports user defined Enum data types, as well as Composite types and
handles them correctly.

| Type Name                              | PostgreSQL Type         | Schema Type | Schema Element Type |
|----------------------------------------|-------------------------|-------------|---------------------|
| Bit                                    | bit, bit(n)             | STRING      |                     |
| Bit Array                              | bit[], bit(n)[]         | ARRAY       | STRING              |
| Bit Varying                            | varbit, varbit(n)       | STRING      |                     |
| Bit Varying Array                      | varbit[], varbit(n)[]   | ARRAY       | STRING              |
| Boolean                                | boolean                 | BOOLEAN     |                     |
| Boolean Array                          | boolean[]               | ARRAY       | BOOLEAN             |
| Box                                    | box                     | STRING      |                     |
| Box Array                              | box[]                   | ARRAY       | STRING              |
| Byte Array (bytea)                     | bytea                   | STRING      |                     |
| Byte Array (bytea) Array               | bytea[]                 | ARRAY       | STRING              |
| CID                                    | cid                     | INT64       |                     |
| CID Array                              | cid[]                   | ARRAY       | INT64               |
| CIDR (IPv4)                            | cidr                    | STRING      |                     |
| CIDR (IPv4) Array                      | cidr[]                  | ARRAY       | STRING              |
| CIDR (IPv6)                            | cidr                    | STRING      |                     |
| CIDR (IPv6) Array                      | cidr[]                  | ARRAY       | STRING              |
| Circle                                 | circle                  | STRING      |                     |
| Circle Array                           | circle[]                | ARRAY       | STRING              |
| Date                                   | date                    | INT32       |                     |
| Date Array                             | date[]                  | ARRAY       | INT32               |
| Date Range                             | daterange               | STRING      |                     |
| Date Range Array                       | daterange[]             | ARRAY       | STRING              |
| Fixed Length Char                      | char(x)                 | STRING      |                     |
| Fixed Length Char Array                | char(x)[]               | ARRAY       | STRING              |
| Float (32bit)                          | float4                  | FLOAT32     |                     |
| Float (32bit) Array                    | float4[]                | ARRAY       | FLOAT32             |
| Float (64bit)                          | float8                  | FLOAT64     |                     |
| Float (64bit) Array                    | float8[]                | ARRAY       | FLOAT64             |
| Geography (PostGUS)                    | geography               | STRUCT      |                     |
| Geography Array  (PostGUS)             | geography[]             | ARRAY       | STRUCT              |
| Geometry (PostGUS)                     | geometry                | STRUCT      |                     |
| Geometry Array (PostGIS)               | geometry[]              | ARRAY       | STRUCT              |
| Hstore                                 | hstore                  | MAP         |                     |
| Hstore Array                           | hstore[]                | ARRAY       | MAP                 |
| Inet (IPv4)                            | inet                    | STRING      |                     |
| Inet (IPv4) Array                      | inet[]                  | ARRAY       | STRING              |
| Inet (IPv6)                            | inet                    | STRING      |                     |
| Inet (IPv6) Array                      | inet[]                  | ARRAY       | STRING              |
| Int (16bit)                            | int2                    | INT16       |                     |
| Int (16bit) Array                      | int2[]                  | ARRAY       | INT16               |
| Int (32bit)                            | int4                    | INT32       |                     |
| Int (32bit) Array                      | int4[]                  | ARRAY       | INT32               |
| Int (64bit)                            | int8                    | INT64       |                     |
| Int (64bit) Array                      | int8[]                  | ARRAY       | INT64               |
| Int4Range                              | int4range               | STRING      |                     |
| Int4Range Array                        | int4range[]             | ARRAY       | STRING              |
| Int8Range                              | int8range               | STRING      |                     |
| Int8Range Array                        | int8range[]             | ARRAY       | STRING              |
| Interval                               | interval                | INT64       |                     |
| Interval Array                         | interval[]              | ARRAY       | INT64               |
| JSON                                   | json                    | STRING      |                     |
| JSON Array                             | json[]                  | ARRAY       | STRING              |
| JSONB                                  | jsonb                   | STRING      |                     |
| JSONB Array                            | jsonb[]                 | ARRAY       | STRING              |
| Line                                   | line                    | STRING      |                     |
| Line Array                             | line[]                  | ARRAY       | STRING              |
| Lseg                                   | lseg                    | STRING      |                     |
| Lseg Array                             | lseg[]                  | ARRAY       | STRING              |
| Ltree                                  | ltree                   | STRING      |                     |
| Ltree Array                            | ltree[]                 | ARRAY       | STRING              |
| MAC Address                            | macaddr                 | STRING      |                     |
| MAC Address (EUI-64)                   | macaddr8                | STRING      |                     |
| MAC Address (EUI-64) Array             | macaddr8[]              | ARRAY       | STRING              |
| MAC Address Array                      | macaddr[]               | ARRAY       | STRING              |
| Name                                   | name                    | STRING      |                     |
| Name Array                             | name[]                  | ARRAY       | STRING              |
| Numeric                                | numeric                 | FLOAT64     |                     |
| Numeric Array                          | numeric[]               | ARRAY       | FLOAT64             |
| Numeric Range                          | numrange                | STRING      |                     |
| Numeric Range Array                    | numrange[]              | ARRAY       | STRING              |
| OID                                    | oid                     | INT64       |                     |
| OID Array                              | oid[]                   | ARRAY       | INT64               |
| Path                                   | path                    | STRING      |                     |
| Path Array                             | path[]                  | ARRAY       | STRING              |
| Point                                  | point                   | STRING      |                     |
| Point Array                            | point[]                 | ARRAY       | STRING              |
| Polygon                                | polygon                 | STRING      |                     |
| Polygon Array                          | polygon[]               | ARRAY       | STRING              |
| Quoted Char                            | "char"                  | STRING      |                     |
| Quoted Char Array                      | "char"[]                | ARRAY       | STRING              |
| Text                                   | text                    | STRING      |                     |
| Text Array                             | text[]                  | ARRAY       | STRING              |
| Time With Timezone                     | timetz                  | STRING      |                     |
| Time With Timezone Array               | timetz[]                | ARRAY       | STRING              |
| Time Without Timezone                  | time                    | STRING      |                     |
| Time Without Timezone Array            | time[]                  | ARRAY       | STRING              |
| Timestamp With Timezone                | timestamptz             | STRING      |                     |
| Timestamp With Timezone Array          | timestamptz[]           | ARRAY       | STRING              |
| Timestamp With Timezone Range          | tstzrange               | STRING      |                     |
| Timestamp With Timezone Range Array    | tstzrange[]             | ARRAY       | STRING              |
| Timestamp Without Timezone             | timestamp               | INT64       |                     |
| Timestamp Without Timezone Array       | timestamp[]             | ARRAY       | INT64               |
| Timestamp Without Timezone Range       | tsrange                 | STRING      |                     |
| Timestamp Without Timezone Range Array | tsrange[]               | ARRAY       | STRING              |
| User Defined Composite Type            | compositetype           | STRUCT      |                     |
| User Defined Composite Type Array      | compositetype[]         | ARRAY       | STRUCT              |
| User Defined Enum                      | enumtype                | STRING      |                     |
| User Defined Enum Array                | enumtype[]              | ARRAY       | STRING              |
| UUID                                   | uuid                    | STRING      |                     |
| UUID Array                             | uuid[]                  | ARRAY       | STRING              |
| Varchar                                | varchar, varchar(n)     | STRING      |                     |
| Varchar Array                          | varchar[], varchar(n)[] | ARRAY       | STRING              |
| XID                                    | xid                     | INT64       |                     |
| XID Array                              | xid[]                   | ARRAY       | INT64               |
| XML                                    | xml                     | STRING      |                     |
| XML Array                              | xml[]                   | ARRAY       | STRING              |

# Configuration

`timescaledb-event-streamer` utilizes [TOML](https://toml.io/en/v1.0.0), or
[YAML](https://yaml.org/) as its configuration file format due to its simplicity.

In addition to the configuration file, all values can be provided as environment
variables. In this case, the property name uses underscore (_) characters instead
of dots (.) characters and all characters are used as uppercase. That means,
`postgresql.connection` becomes `POSTGRESQL_CONNECTION`. In case the standard
property name already contains an underscore the one underscore character is
duplicated (`test.some_value` becomes `TEST_SOME__VALUE`).

## PostgreSQL Configuration

| Property                                |                                                                                                                                                                                                                                       Description |        Data Type |                                 Default Value |
|-----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|-----------------:|----------------------------------------------:|
| `postgresql.connection`                 |                                                                                                                                                                                        The connection string in one of the libpq-supported forms. |           string | host=localhost user=repl_user sslmode=disable |
| `postgresql.password`                   |                                                                                                                                                                                                              The password to connect to the user. |           string |            Environment variable: `PGPASSWORD` |
| `postgresql.snapshot.batchsize`         |                                                                                                                                                                  The size of rows requested in a single batch iteration when snapshotting tables. |              int |                                          1000 |
| `postgresql.snapshot.initial`           |                                                                                                  The value describes the startup behavior for snapshotting. Valid values are `always`, `never`, `initial_only`. **NOT YET IMPLEMENTED: `always`** |           string |                                       `never` |
| `postgresql.publication.name`           |                                                                                                                                                                                                    The name of the publication inside PostgreSQL. |           string |                                  empty string |
| `postgresql.publication.create`         |                                                                                                                                     The value describes if a non-existent publication of the defined name should be automatically created or not. |          boolean |                                         false |
| `postgresql.publication.autodrop`       |                                                                                                                                   The value describes if a previously automatically created publication should be dropped when the program exits. |          boolean |                                          true | 
| `postgresql.replicationslot.name`       |                                                                                                                                    The name of the replication slot inside PostgreSQL. If not configured, a random 20 characters name is created. |           string |                            random string (20) |
| `postgresql.replicationslot.create`     |                                                                                                                                The value describes if a non-existent replication slot of the defined name should be automatically created or not. |          boolean |                                          true |
| `postgresql.replicationslot.autodrop`   |                                                                                                                              The value describes if a previously automatically created replication slot should be dropped when the program exits. |          boolean |                                          true |
| `postgresql.transaction.window.enabled` |                                                                 The value describes if a transaction window should be opened or not. Transaction windows are used to try to collect all WAL entries of the transaction before replicating it out. |          boolean |                                          true |
| `postgresql.transaction.window.timeout` |      The value describes the maximum time to wait for a transaction end (COMMIT) to be received. The value is the number of seconds. If the COMMIT isn't received inside the given time window, replication will start to prevent memory hogging. |              int |                                            60 |
| `postgresql.transaction.window.maxsize` |                      The value describes the maximum number of cached entries to wait for a transaction end (COMMIT) to be received. If the COMMIT isn't received inside the given time window, replication will start to prevent memory hogging. |              int |                                         10000 |
| `postgresql.tables.includes`            | The includes definition defines which vanilla tables to include in the event stream generation. The available patters are explained in [Includes and Excludes Patterns](#includes-and-excludes-patterns). Excludes have precedence over includes. | array of strings |                                   empty array |
| `postgresql.tables.excludes`            | The excludes definition defines which vanilla tables to exclude in the event stream generation. The available patters are explained in [Includes and Excludes Patterns](#includes-and-excludes-patterns). Excludes have precedence over includes. | array of strings |                                   empty array |
| `postgresql.events.read`                |                                                                                                                                                                             The property defines if read events for vanilla tables are generated. |          boolean |                                          true |
| `postgresql.events.insert`              |                                                                                                                                                                           The property defines if insert events for vanilla tables are generated. |          boolean |                                          true |
| `postgresql.events.update`              |                                                                                    The property defines if update events for vanilla tables are generated. If old values should be captured, `REPLICA IDENTITY FULL` needs to be seton the table. |          boolean |                                          true |
| `postgresql.events.delete`              |                                                                                    The property defines if delete events for vanilla tables are generated. If old values should be captured, `REPLICA IDENTITY FULL` needs to be seton the table. |          boolean |                                          true |
| `postgresql.events.truncate`            |                                                                                                                                                                         The property defines if truncate events for vanilla tables are generated. |          boolean |                                          true |
| `postgresql.events.message`             |                                                                                                                                                                         The property defines if logical replication message events are generated. |          boolean |                                         false |

## Topic Configuration

| Property                    |                                                                               Description | Data Type | Default Value |
|-----------------------------|------------------------------------------------------------------------------------------:|----------:|--------------:|
| `topic.namingstrategy.type` | The naming strategy of topic names. At the moment only the value `debezium` is supported. |    string |    `debezium` |
| `topic.prefix`              |                                                           The prefix for all topic named. |    string | `timescaledb` |

## State Storage Configuration

| Property                 |                                                                                                                 Description | Data Type | Default Value |
|--------------------------|----------------------------------------------------------------------------------------------------------------------------:|----------:|--------------:|
| `statestorage.type`      | The strategy to store internal state (such as restart points and snapshot information). Valid values are `file` and `none`. |    string |        `none` |
| `statestorage.file.path` |                                If the type is `file`, this property defines the file system path of the state storage file. |    string |  empty string |

## TimescaleDB Configuration

| Property                           |                                                                                                                                                                                                                                    Description |        Data Type | Default Value |
|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|-----------------:|--------------:|
| `timescaledb.hypertables.includes` | The includes definition defines which hypertables to include in the event stream generation. The available patters are explained in [Includes and Excludes Patterns](#includes-and-excludes-patterns). Excludes have precedence over includes. | array of strings |   empty array |
| `timescaledb.hypertables.excludes` | The excludes definition defines which hypertables to exclude in the event stream generation. The available patters are explained in [Includes and Excludes Patterns](#includes-and-excludes-patterns). Excludes have precedence over includes. | array of strings |   empty array |
| `timescaledb.events.read`          |                                                                                                                                                                             The property defines if read events for hypertables are generated. |          boolean |          true |
| `timescaledb.events.insert`        |                                                                                                                                                                           The property defines if insert events for hypertables are generated. |          boolean |          true |
| `timescaledb.events.update`        |                                                                                    The property defines if update events for hypertables are generated. If old values should be captured, `REPLICA IDENTITY FULL` needs to be seton the table. |          boolean |          true |
| `timescaledb.events.delete`        |                                                                                    The property defines if delete events for hypertables are generated. If old values should be captured, `REPLICA IDENTITY FULL` needs to be seton the table. |          boolean |          true |
| `timescaledb.events.truncate`      |                                                                                                                                                                         The property defines if truncate events for hypertables are generated. |          boolean |          true |
| `timescaledb.events.compression`   |                                                                                                                                                                      The property defines if compression events for hypertables are generated. |          boolean |         false |
| `timescaledb.events.decompression` |                                                                                                                                                                    The property defines if decompression events for hypertables are generated. |          boolean |         false |
| `timescaledb.events.message`       |                                                                                             The property defines if logical replication message events are generated. This property is **deprecated**, please see `postgresql.events.message`. |          boolean |         false |

## Sink Configuration

| Property                    |                                                                                                                                                                                          Description |                 Data Type | Default Value |
|-----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|--------------------------:|--------------:|
| `sink.type`                 |                                                                                  The property defines which sink adapter is to be used. Valid values are `stdout`, `nats`, `kafka`, `redis`, `http`. |                    string |      `stdout` |
| `sink.tombstone`            |                                                                                                                    The property defines if delete events will be followed up with a tombstone event. |                   boolean |         false |
| `sink.filters.<name>.<...>` | The filters definition defines filters to be executed against potentially replicated events. This property is a map with the filter name as its key and a [Sink Filter](#sink-filter-configuration). | map of filter definitions |     empty map |

### Sink Filter configuration

| Property                              |                                                                                                                                                                                                                                   Description |        Data Type | Default Value |
|---------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|-----------------:|--------------:|
| `sink.filters.<name>.condition`       |                                                                                                        This property defines the filter expression to be executed. The expression language used is [Expr](https://github.com/antonmedv/expr). |           string |  empty string |
| `sink.filters.<name>.default`         |                                                                                              This property defines the value to be returned from the filter expression is true. This makes it possible to make negative and positive filters. |          boolean |          true | 
| `sink.filters.<name>.tables.includes` | The includes definition defines to which tables the filter expression should be applied to. The available patters are explained in [Includes and Excludes Patterns](#includes-and-excludes-patterns). Excludes have precedence over includes. | array of strings |   empty array |
| `sink.filters.<name>.tables.excludes` | The excludes definition defines to which tables the filter expression should be applied to. The available patters are explained in [Includes and Excludes Patterns](#includes-and-excludes-patterns). Excludes have precedence over includes. | array of strings |   empty array |

The include and exclude filters are optional. If neither is provided, the filter
is expected to apply to all hypertables.

Events generated for excluded hypertables will be replicated, as the filter isn't
tested.

### NATS Sink Configuration

NATS specific configuration, which is only used if `sink.type` is set to `nats`.

| Property                            |                                                                      Description |        Data Type | Default Value |
|-------------------------------------|---------------------------------------------------------------------------------:|-----------------:|--------------:|
| `sink.nats.address`                 | The NATS connection address, according to the NATS connection string definition. |           string |  empty string |
| `sink.nats.authorization`           | The NATS authorization type. Valued values are `userinfo`, `credentials`, `jwt`. |           string |  empty string |
| `sink.nats.userinfo.username`       |                                  The username of userinfo authorization details. |           string |  empty string | 
| `sink.nats.userinfo.password`       |                                  The password of userinfo authorization details. |           string |  empty string | 
| `sink.nats.credentials.certificate` |           The path of the certificate file of credentials authorization details. |           string |  empty string | 
| `sink.nats.credentials.seeds`       |                 The paths of seeding files of credentials authorization details. | array of strings |   empty array | 

### Kafka Sink Configuration

Kafka specific configuration, which is only used if `sink.type` is set to `kafka`.

| Property                    |                                                                                                    Description |       Data Type |    Default Value |
|-----------------------------|---------------------------------------------------------------------------------------------------------------:|----------------:|-----------------:|
| `sink.kafka.brokers`        |                                                                                         The Kafka broker urls. | array of string |      empty array |
| `sink.kafka.idempotent`     |                                                        The property defines if message handling is idempotent. |         boolean |            false |
| `sink.kafka.sasl.enabled`   |                                                         The property defines if SASL authorization is enabled. |         boolean |            false | 
| `sink.kafka.sasl.user`      |                                                             The user value to be used with SASL authorization. |          string |     empty string | 
| `sink.kafka.sasl.password`  |                                                         The password value to be used with SASL authorization. |          string |     empty string | 
| `sink.kafka.sasl.mechanism` |                                    The mechanism to be used with SASL authorization. Valid values are `PLAIN`. |          string |          `PLAIN` | 
| `sink.kafka.tls.enabled`    |                                                                        The property defines if TLS is enabled. |         boolean |            false | 
| `sink.kafka.tls.skipverify` |                                           The property defines if verification of TLS certificates is skipped. |         boolean |            false | 
| `sink.kafka.tls.clientauth` | The property defines the client auth value (as defined in [Go](https://pkg.go.dev/crypto/tls#ClientAuthType)). |             int | 0 (NoClientCert) | 

### Redis Sink Configuration

Redis specific configuration, which is only used if `sink.type` is set to `redis`.

| Property                         |                                                                                                    Description | Data Type |     Default Value |
|----------------------------------|---------------------------------------------------------------------------------------------------------------:|----------:|------------------:|
| `sink.redis.network`             |                                      The network type of the redis connection. Valid values are `tcp`, `unix`. |    string |             `tcp` |
| `sink.redis.address`             |                                                                           The connection address as host:port. |    string |  `localhost:6379` |
| `sink.redis.password`            |                                                              Optional password to connect to the redis server. |    string |      empty string |
| `sink.redis.database`            |                                                             Database to select after connecting to the server. |       int |                 0 |
| `sink.redis.poolsize`            |                                                                          Maximum number of socket connections. |       int |        10 per cpu |
| `sink.redis.retries.maxattempts` |                                                                    Maximum number of retries before giving up. |       int |                 0 |
| `sink.redis.retries.backoff.min` |                          Minimum backoff between each retry in milliseconds. A value of `-1` disables backoff. |       int |                 8 |
| `sink.redis.retries.backoff.max` |                          Maximum backoff between each retry in milliseconds. A value of `-1` disables backoff. |       int |               512 |
| `sink.redis.timeouts.dial`       |                                                      Dial timeout for establishing new connections in seconds. |       int |                 5 |
| `sink.redis.timeouts.read`       |                                     Timeout for socket reads in seconds. A value of `-1` disables the timeout. |       int |                 3 |
| `sink.redis.timeouts.write`      |                                    Timeout for socket writes in seconds. A value of `-1` disables the timeout. |       int |      read timeout |
| `sink.redis.timeouts.pool`       |   Amount of time in seconds client waits for connection if all connections are busy before returning an error. |       int | read timeout + 1s |
| `sink.redis.timeouts.idle`       |                                          Amount of time in minutes after which client closes idle connections. |       int |                 5 |
| `sink.redis.tls.enabled`         |                                                                        The property defines if TLS is enabled. |      bool |             false |
| `sink.redis.tls.skipverify`      |                                           The property defines if verification of TLS certificates is skipped. |      bool |             false |
| `sink.redis.tls.clientauth`      | The property defines the client auth value (as defined in [Go](https://pkg.go.dev/crypto/tls#ClientAuthType)). |       int |  0 (NoClientCert) |

### AWS Kinesis Sink Configuration

| Property                         |                                                                                                                                                                                                             Description | Data Type | Default Value |
|----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|----------:|--------------:|
| `sink.kinesis.stream.name`       |                                                                                                           The Name of the AWS Kinesis stream to send events to. The events will be partitioned based on the topic name. |    string |  empty string |
| `sink.kinesis.stream.create`     |                                                                                                  Defines if the stream should be created at startup if non-existent. The below properties configure the created stream. |   boolean |          true |
| `sink.kinesis.stream.shardcount` |                                                                                                                                                                   The number if shards to use when creating the stream. |       int |             1 |
| `sink.kinesis.stream.mode`       | The mode to use when creating the stream. Valid values are `ON_DEMAND`, and `PROVISIONED`. More details in the [AWS documentation](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StreamModeDetails.html). |    string |  empty string |
| `sink.kinesis.aws.<...>`         |                                                                                                                             AWS specific content as defined in [AWS service configuration](#aws-service-configuration). |    struct |  empty struct |

### AWS SQS Sink Configuration

AWS SQS queues when configured as **FIFO** queues. No content based deduplication
is required, since the sink creates a deduplication id based on the LSN,
transaction id (if available), and content of the message.

| Property             |                                                                                 Description | Data Type | Default Value |
|----------------------|--------------------------------------------------------------------------------------------:|----------:|--------------:|
| `sink.sqs.queue.url` |                                                           The URL of the FIFO queue in SQS. |    string |  empty string |
| `sink.sqs.aws.<...>` | AWS specific content as defined in [AWS service configuration](#aws-service-configuration). |    struct |  empty struct |

### HTTP Sink Configuration

HTTP specific configuration, which is only used if `sink.type` is set to `http`.
This Sink is an HTTP client that `POST`s the events as the payload.
Usage of TLS is inferred automatically from the prefix of the `url`, if `url` has
the `https://` prefix, then the respective TLS settings will be set according to
the properties defined in `sink.http.tls`.

| Property                                  |                                                                                                    Description | Data Type |         Default Value |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------------:|----------:|----------------------:|
| `sink.http.url`                           |             The url where the requests are sent. You have to include the protocol scheme (`http`/`https`) too. |    string | `http://localhost:80` |
| `sink.http.authentication.type`           |             Type of authentication to use when making the request. Valid values are `none`, `basic`, `header`. |    string |                  none |
| `sink.http.authentication.basic.username` |           If the authentication type is set to `basic` then this is the username used when making the request. |    string |          empty string |
| `sink.http.authentication.basic.password` |                                                                          Maximum number of socket connections. |       int |          empty string |
| `sink.http.authentication.header.name`    |                                                                    Maximum number of retries before giving up. |       int |          empty string |
| `sink.http.authentication.header.value`   |                          Minimum backoff between each retry in milliseconds. A value of `-1` disables backoff. |       int |          empty string |
| `sink.http.tls.skipverify`                |                                           The property defines if verification of TLS certificates is skipped. |      bool |                 false |
| `sink.http.tls.clientauth`                | The property defines the client auth value (as defined in [Go](https://pkg.go.dev/crypto/tls#ClientAuthType)). |       int |      0 (NoClientCert) |


### AWS Service Configuration

This configuration is the basic configuration for AWS, including the region,
or credentials. If the latter isn't configured, the sink will try to use
environment variables (similar to the official AWS clients) to provide
connection details.

| Property                    |                Description | Data Type | Default Value |
|-----------------------------|---------------------------:|----------:|--------------:|
| `<...>.aws.region`          |            The AWS region. |    string |  empty string |
| `<...>.aws.endpoint`        |          The AWS endpoint. |    string |  empty string |
| `<...>.aws.accesskeyid`     |     The AWS Access Key Id. |    string |  empty string |
| `<...>.aws.secretaccesskey` | The AWS Secret Access Key. |    string |  empty string |
| `<...>.aws.sessiontoken`    |     The AWS Session Token. |    string |  empty string |

## Logging Configuration

This section describes the logging configuration. There is one standard logger.
In addition to that, loggers are named (as seen in the log messages). Those names
can be used to define a more specific logger configuration and override log levels
or outputs.

Valid logging levels are `panic`, `fatal`, `error`, `warn`, `notice`, `info`,
`verbose`, `debug`, and `trace`. Earlier levels include all following ones
as well.

| Property                       |                                                                                                                                                                    Description |                                             Data Type | Default Value |
|--------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|------------------------------------------------------:|--------------:|
| `logging.level`                |                                                                                      This property defines the default logging level. If not defined, default value is `info`. |                                                string |        `info` |
| `logging.outputs.<...>`        |                                                                               This property defines the outputs for the default logger. By default console logging is enabled. | [output configuration](#logging-output-configuration) |  empty struct |
| `logging.loggers.<name>.<...>` | This property provides the possibility to override the logging for certain parts of the system. The <name> is the name the logger uses to identify itself in the log messages. | [sub-logger configuration](#sub-logger-configuration) |  empty struct |

### Sub-Logger Configuration

| Property                               |                                                                                      Description |                                             Data Type | Default Value |
|----------------------------------------|-------------------------------------------------------------------------------------------------:|------------------------------------------------------:|--------------:|
| `logging.loggers.<name>.level`         |        This property defines the default logging level. If not defined, default value is `info`. |                                                string |        `info` |
| `logging.loggers.<name>.outputs.<...>` | This property defines the outputs for the default logger. By default console logging is enabled. | [output configuration](#logging-output-configuration) |  empty struct |

### Logging Output Configuration

| Property                         |                                                                                                                                                             Description | Data Type | Default Value |
|----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|----------:|--------------:|
| `<...>.outputs.console.enabled`  |      This property is used to enabled the console logger. If this configuration is used for the default logger, this value is `true` by default, but `false` otherwise. |   boolean |         false |
| `<...>.outputs.file.enabled`     |                                                                                                                       This property is used to enabled the file logger. |   boolean |         false |
| `<...>.outputs.file.path`        |                                                                                                                 This property defines the log file path for the logger. |    string |  empty string |
| `<...>.outputs.file.rotate`      |                                                                                                                This property defines if the log file should be rotated. |   boolean |          true |
| `<...>.outputs.file.maxduration` | This property defines the maximum runtime (in seconds) of the log file before rotation. If both, `maxduration` and `maxsize` are defined, `maxduration` has precedence. |       int |            60 | 
| `<...>.outputs.file.maxsize`     | This property defines the maximum file size (in bytes) of the log file before rotation. If both, `maxduration` and `maxsize` are defined, `maxduration` has precedence. |       int | 5242880 (5MB) |
| `<...>.outputs.file.compress`    |                                                                                                     This property defines if the rotated log file should be compressed. |   boolean |         false |

# Includes and Excludes Patterns

Includes and Excludes can be defined as fully canonical references to hypertables
(equivalent to [PostgreSQL regclass definition](https://www.postgresql.org/docs/15/datatype-oid.html))
or as patterns with wildcards.

For an example we assume the following hypertables to exist.

| Schema    |      Hypertable |         Canonical Name |
|-----------|----------------:|-----------------------:|
| public    |         metrics |         public.metrics |
| public    | status_messages | public.status_messages |
| invoicing |        invoices |     invoicing.invoices |
| alarming  |          alarms |        alarming.alarms |

To note, excludes have precedence over includes, meaning, that if both includes and
excludes match a specific hypertable, the hypertable will be excluded from the event
generation process.

Hypertables can be referred to by their canonical name (dotted notation of schema and
hypertable table name).
`timescaledb.hypertables.includes = [ 'public.metrics', 'invoicing.invoices' ]`

When referring to the hypertable by its canonical name, the matcher will only match
the exact hypertable. That said, the above example will yield events for the hypertables
`public.metrics` and  `invoicing.invoices` but none of the other ones.

## Wildcards

Furthermore, includes and excludes can utilize wildcard characters to match a subset
of tables based on the provided pattern.

`timescaledb-event-streamer` understands 3 types of wildcards:

| Wildcard |                                                   Description |
|----------|--------------------------------------------------------------:|
| *        |    The asterisk (*) character matches zero or more characters |
| +        |         The plus (+) character matches one or more characters |
| ?        | The question mark (?) character matches exactly one character |

Wildcards can be used in the schema or table names. It is also possible to have them
in schema and table names at the same time.

### Asterisk: Zero Or More Matching

| Schema |      Hypertable |         Canonical Name |
|--------|----------------:|-----------------------:|
| public |         metrics |         public.metrics |
| public | status_messages | public.status_messages |

`timescaledb.hypertables.includes = [ 'public.*' ]` matches all hypertables in schema
`public`.

| Schema |      Hypertable |         Canonical Name |
|--------|----------------:|-----------------------:|
| public |       status_1h |       public.status_1h |
| public |      status_12h |      public.status_12h |
| public | status_messages | public.status_messages |

`timescaledb.hypertables.includes = [ 'public.statis_1*' ]` matches `public.status_1h`
and `public.status_12h`, but not `public.status_messages`.

| Schema    | Hypertable |    Canonical Name |
|-----------|-----------:|------------------:|
| customer1 |    metrics | customer1.metrics |
| customer2 |    metrics | customer2.metrics |

Accordingly, it is possible to match a specific hypertable in all customer schemata
using a pattern such as `timescaledb.hypertables.includes = [ 'customer*.metrics' ]`.

### Plus: One or More Matching

| Schema |     Hypertable |         Canonical Name |
|--------|---------------:|-----------------------:|
| public |   status_1_day |    public.status_1_day |
| public | status_1_month |  public.status_1_month |
| public |  status_1_year | public.status_12_month |

`timescaledb.hypertables.includes = [ 'public.statis_+_month' ]` matches
hypertables `public.status_1_month` and `public.status_12_month`, but not
`public.status_1_day`.

| Schema    | Hypertable |    Canonical Name |
|-----------|-----------:|------------------:|
| customer1 |    metrics | customer1.metrics |
| customer2 |    metrics | customer2.metrics |

Accordingly, it is possible to match a specific hypertable in all customer schemata
using a pattern such as `timescaledb.hypertables.includes = [ 'customer+.metrics' ]`.

### Question Mark: Exactly One Matching

| Schema |    Hypertable |       Canonical Name |
|--------|--------------:|---------------------:|
| public |  status_1_day |  public.status_1_day |
| public |  status_7_day |  public.status_7_day |
| public | status_14_day | public.status_14_day |

`timescaledb.hypertables.includes = [ 'public.statis_?_day' ]` matches
hypertables `public.status_1_day` and `public.status_7_day`, but not
`public.status_14_day`.
