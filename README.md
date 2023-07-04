# timescaledb-event-streamer

`timescaledb-event-streamer` is a command line program to create a stream of
CDC (Chance Data Capture) TimescaleDB Hypertable events from a PostgreSQL
installation running the TimescaleDB extension.

Change Data Capture is a technology where insert, update, delete and similar
operations inside the database generate a corresponding set of events, which
are commonly distributed through a messaging connector, such as Kafka, NATS,
or similar.

_**Attention:** This is not an official Timescale project, but just developed
by a person who works for Timescale. This may change at some point in the future
but it is not a given._

# Why not just Debezium?

While [Debezium](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
already supports PostgreSQL, the implementation doesn't really support the internals
of TimescaleDB, most specifically the way data is chunked or partitioned. It is
possible to use Debezium to capture change events of the actual chunks itself,
but without the catalog handling. This means that every chunk would emit changes on
its own, but with no reference to its parent hypertable. The `timescaledb-event-streamer`
changes this, by handling the catalog updates and resolving the parent hypertable
before emitting the events.

Anyhow, the final goal is to provide an implementation for Debezium when the prototype
(which may stay as its own standalone project) is fully working and every complication
has been found and fixed.

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

## Supporting non-privileged users (without postgres user)

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

## Using timescaledb-event-streamer

After creating a configuration file, `timescaledb-event-streamer` can be executed
with the following command:

```bash
$ timescaledb-event-streamer -config=./config.toml
```

The tool will connect to your TimescaleDB database, and start replicating incoming
events.

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

| Property                                |                                                                                                                                                                                                                                  Description | Data Type |                                 Default Value |
|-----------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|----------:|----------------------------------------------:|
| `postgresql.connection`                 |                                                                                                                                                                                   The connection string in one of the libpq-supported forms. |    string | host=localhost user=repl_user sslmode=disable |
| `postgresql.password`                   |                                                                                                                                                                                                         The password to connect to the user. |    string |            Environment variable: `PGPASSWORD` |
| `postgresql.snapshot.batchsize`         |                                                                                                                                                             The size of rows requested in a single batch iteration when snapshotting tables. |       int |                                          1000 |
| `postgresql.snapshot.initial`           |                                                                                             The value describes the startup behavior for snapshotting. Valid values are `always`, `never`, `initial_only`. **NOT YET IMPLEMENTED: `always`** |    string |                                       `never` |
| `postgresql.publication.name`           |                                                                                                                                                                                               The name of the publication inside PostgreSQL. |    string |                                  empty string |
| `postgresql.publication.create`         |                                                                                                                                The value describes if a non-existent publication of the defined name should be automatically created or not. |   boolean |                                         false |
| `postgresql.publication.autodrop`       |                                                                                                                              The value describes if a previously automatically created publication should be dropped when the program exits. |   boolean |                                          true | 
| `postgresql.replicationslot.name`       |                                                                                                                               The name of the replication slot inside PostgreSQL. If not configured, a random 20 characters name is created. |    string |                            random string (20) |
| `postgresql.replicationslot.create`     |                                                                                                                           The value describes if a non-existent replication slot of the defined name should be automatically created or not. |   boolean |                                          true |
| `postgresql.replicationslot.autodrop`   |                                                                                                                         The value describes if a previously automatically created replication slot should be dropped when the program exits. |   boolean |                                          true |
| `postgresql.transaction.window.enabled` |                                                            The value describes if a transaction window should be opened or not. Transaction windows are used to try to collect all WAL entries of the transaction before replicating it out. |   boolean |                                          true |
| `postgresql.transaction.window.timeout` | The value describes the maximum time to wait for a transaction end (COMMIT) to be received. The value is the number of seconds. If the COMMIT isn't received inside the given time window, replication will start to prevent memory hogging. |       int |                                            60 |
| `postgresql.transaction.window.maxsize` |                 The value describes the maximum number of cached entries to wait for a transaction end (COMMIT) to be received. If the COMMIT isn't received inside the given time window, replication will start to prevent memory hogging. |       int |                                         10000 |

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
| `timescaledb.events.read`          |                                                                                                                                                                                             The property defines if read events are generated. |          boolean |          true |
| `timescaledb.events.insert`        |                                                                                                                                                                                           The property defines if insert events are generated. |          boolean |          true |
| `timescaledb.events.update`        |                                                                                                                                                                                           The property defines if update events are generated. |          boolean |          true |
| `timescaledb.events.delete`        |                                                                                                                                                                                           The property defines if delete events are generated. |          boolean |          true |
| `timescaledb.events.truncate`      |                                                                                                                                                                                         The property defines if truncate events are generated. |          boolean |          true |
| `timescaledb.events.message`       |                                                                                                                                                                      The property defines if logical replication message events are generated. |          boolean |         false |
| `timescaledb.events.compression`   |                                                                                                                                                                                      The property defines if compression events are generated. |          boolean |         false |
| `timescaledb.events.decompression` |                                                                                                                                                                                    The property defines if decompression events are generated. |          boolean |         false |

## Sink Configuration

| Property                    |                                                                                                                                                                                          Description |                 Data Type | Default Value |
|-----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|--------------------------:|--------------:|
| `sink.type`                 |                                                                                          The property defines which sink adapter is to be used. Valid values are `stdout`, `nats`, `kafka`, `redis`. |                    string |      `stdout` |
| `sink.tombstone`            |                                                                                                                    The property defines if delete events will be followed up with a tombstone event. |                   boolean |         false |
| `sink.filters.<name>.<...>` | The filters definition defines filters to be executed against potentially replicated events. This property is a map with the filter name as its key and a [Sink Filter](#sink-filter-configuration). | map of filter definitions |     empty map |

### Sink Filter configuration

| Property                                   |                                                                                                                                                                                                                                        Description |        Data Type | Default Value |
|--------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|-----------------:|--------------:|
| `sink.filters.<name>.condition`            |                                                                                                                     This property defines the filter expression to be executed. The expression language used is [Expr](github.com/antonmedv/expr). |           string |  empty string |
| `sink.filters.<name>.default`              |                                                                                                   This property defines the value to be returned from the filter expression is true. This makes it possible to make negative and positive filters. |          boolean |          true | 
| `sink.filters.<name>.hypertables.includes` | The includes definition defines to which hypertables the filter expression should be applied to. The available patters are explained in [Includes and Excludes Patterns](#includes-and-excludes-patterns). Excludes have precedence over includes. | array of strings |   empty array |
| `sink.filters.<name>.hypertables.excludes` | The excludes definition defines to which hypertables the filter expression should be applied to. The available patters are explained in [Includes and Excludes Patterns](#includes-and-excludes-patterns). Excludes have precedence over includes. | array of strings |   empty array |

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

| Property                    |                                                                                                    Description |       Data Type | Default Value |
|-----------------------------|---------------------------------------------------------------------------------------------------------------:|----------------:|--------------:|
| `sink.kafka.brokers`        |                                                                                         The Kafka broker urls. | array of string |   empty array |
| `sink.kafka.idempotent`     |                                                        The property defines if message handling is idempotent. |         boolean |         false |
| `sink.kafka.sasl.enabled`   |                                                         The property defines if SASL authorization is enabled. |         boolean |         false | 
| `sink.kafka.sasl.user`      |                                                             The user value to be used with SASL authorization. |          string |  empty string | 
| `sink.kafka.sasl.password`  |                                                         The password value to be used with SASL authorization. |          string |  empty string | 
| `sink.kafka.sasl.mechanism` |                                    The mechanism to be used with SASL authorization. Valid values are `PLAIN`. |          string |       `PLAIN` | 
| `sink.kafka.tls.enabled`    |                                                                        The property defines if TLS is enabled. |         boolean |         false | 
| `sink.kafka.tls.skipverify` |                                           The property defines if verification of TLS certificates is skipped. |         boolean |         false | 
| `sink.kafka.tls.clientauth` | The property defines the client auth value (as defined in [Go](https://pkg.go.dev/crypto/tls#ClientAuthType)). |         boolean |         false | 

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
| `sink.redis.tls.clientauth`      | The property defines the client auth value (as defined in [Go](https://pkg.go.dev/crypto/tls#ClientAuthType)). |       int |                 0 |

## Logging Configuration

This section describes the logging configuration. There is one standard logger.
In addition to that, loggers are named (as seen in the log messages). Those names
can be used to define a more specific logger configuration and override log levels
or outputs.

Valid logging levels are `panic`, `fatal`, `error`, `warn`, `notice`, `info`,
`verbose`, `debug`, and `trace`. Earlier levels include all following ones
as well.

| Property                       |                                                                                                                                                                   Description |                                             Data Type | Default Value |
|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|------------------------------------------------------:|--------------:|
| `logging.level`                |                                                                                     This property defines the default logging level. If not defined, default value is `info`. |                                                string |        `info` |
| `logging.outputs.<...>`        |                                                                              This property defines the outputs for the default logger. By default console logging is enabled. | [output configuration](#logging-output-configuration) |  empty struct |
| `logging.loggers.<name>.<...>` | This property provides the possibility to override the logging for certain parts of the system. The <name> is the name the logger uses to identify itself in the log mesages. | [sub-logger configuration](#sub-logger-configuration) |  empty struct |

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
