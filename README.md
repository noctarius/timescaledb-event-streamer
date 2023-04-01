# timescaledb-event-streamer

`timescaledb-event-streamer` is a command line program to create a stream of
CDC (Chance Data Capture) TimescaleDB Hypertable events from a PostgreSQL
installation running the TimescaleDB extension.

Change Data Capture is a technology where insert, update, delete and similar
operations inside the database generate a corresponding set of events, which
are commonly distributed through a messaging connector, such as Kafka, NATS,
or similar.

_**Attention:** This software is in prototyping / alpha stage and not meant for
production usage. No guarantees on the functionality are given._

_**Attention 2:** This is not an official Timescale project, but just developed
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

`timescaledb-event-streamer` requires the [Go runtime (version 1.20+)](https://go.dev/doc/install)
to be installed. With this requirement satisfied, the installation can be
kicked off using:

```bash
$ go install github.com/noctarius/timescaledb-event-streamer/cmd/timescaledb-event-streamer@latest
```

Before using the program, a configuration file needs to be created. An example
configuration can be
found [here](https://raw.githubusercontent.com/noctarius/timescaledb-event-streamer/main/config.example.toml).

For a full reference of the existing configuration options, see the [Configuration](#configuration)
section.

## Using timescaledb-event-streamer

After creating a configuration file, `timescaledb-event-streamer` can be executed
with the following command:

```bash
$ timescaledb-event-streamer -config=./config.toml
```

The tool will connect to your TimescaleDB database, and start replicating incoming
events.

# Configuration

`timescaledb-event-streamer` utilizes [TOML](https://toml.io/en/v1.0.0) as its
configuration file format due to its simplicity.

The actual configuration values are designed as canonical name (dotted keys).

In addition to the configuration file, all values can be provided as envrionment
variables. In this case, the property name uses underscore (_) characters instead
of dots (.) characters and all characters are used as uppercase. That means,
`postgresql.connection` becomes `POSTGRESQL_CONNECTION`. In case the standard
property name already contains an underscore the one underscore character is
duplicated (`test.some_value` becomes `TEST_SOME__VALUE`).

## PostgreSQL Configuration

| Property                        |                                                                                                                            Description | Data Type |                                 Default Value |
|---------------------------------|---------------------------------------------------------------------------------------------------------------------------------------:|----------:|----------------------------------------------:|
| `postgresql.connection`         |                                                                              The connection string in one of the libpq-supported forms |    string | host=localhost user=repl_user sslmode=disable |
| `postgresql.password`           |                                                                                                   The password to connect to the user. |    string |            Environment variable: `PGPASSWORD` |
| `postgresql.snapshot.batchsize` |                                                       The size of rows requested in a single batch iteration when snapshotting tables. |       int |                                          1000 |
| `postgresql.snapshot.initial`   | The value describes the startup behavior for snapshotting. Valid values are `always`, `never`, `initial_only`. **NOT YET IMPLEMENTED** |    string |                                `initial_only` |

## Topic Configuration

| Property                    |                                                                               Description | Data Type | Default Value |
|-----------------------------|------------------------------------------------------------------------------------------:|----------:|--------------:|
| `topic.namingstrategy.type` | The naming strategy of topic names. At the moment only the value `debezium` is supported. |    string |    `debezium` |
| `topic.prefix`              |                                                            The prefix for all topic named |    string | `timescaledb` |

## TimescaleDB Configuration

| Property                           |                                                                                                                                                                                                                                    Description |        Data Type | Default Value |
|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|-----------------:|--------------:|
| `timescaledb.hypertables.includes` | The includes definition defines which hypertables to include in the event stream generation. The available patters are explained in [Includes and Excludes Patterns](#includes-and-excludes-patterns). Excludes have precedence over includes. | array of strings |   empty array |
| `timescaledb.hypertables.excludes` | The excludes definition defines which hypertables to exclude in the event stream generation. The available patters are explained in [Includes and Excludes Patterns](#includes-and-excludes-patterns). Excludes have precedence over includes. | array of strings |   empty array |
| `timescaledb.events.read`          |                                                                                                                                                                                              The property defines if read events are generated |          boolean |          true |
| `timescaledb.events.insert`        |                                                                                                                                                                                            The property defines if insert events are generated |          boolean |          true |
| `timescaledb.events.update`        |                                                                                                                                                                                            The property defines if update events are generated |          boolean |          true |
| `timescaledb.events.delete`        |                                                                                                                                                                                            The property defines if delete events are generated |          boolean |          true |
| `timescaledb.events.truncate`      |                                                                                                                                                                                          The property defines if truncate events are generated |          boolean |          true |
| `timescaledb.events.compression`   |                                                                                                                                                                                       The property defines if compression events are generated |          boolean |         false |
| `timescaledb.events.decompression` |                                                                                                                                                                                     The property defines if decompression events are generated |          boolean |         false |

## Sink Configuration

| Property    |                                                                                       Description | Data Type | Default Value |
|-------------|--------------------------------------------------------------------------------------------------:|----------:|--------------:|
| `sink.type` | The property defines which sink adapter is to be used. Valid values are `stdout`, `nats`, `kafka` |    string |      `stdout` |

NATS specific configuration, which is only used if `sink.type` is set to `nats`.

| Property                            |                                                                     Description |        Data Type | Default Value |
|-------------------------------------|--------------------------------------------------------------------------------:|-----------------:|--------------:|
| `sink.nats.address`                 | The NATS connection address, according to the NATS connection string definition |           string |  empty string |
| `sink.nats.authorization`           | The NATS authorization type. Valued values are `userinfo`, `credentials`, `jwt` |           string |  empty string |
| `sink.nats.userinfo.username`       |                                  The username of userinfo authorization details |           string |  empty string | 
| `sink.nats.userinfo.password`       |                                  The password of userinfo authorization details |           string |  empty string | 
| `sink.nats.credentials.certificate` |           The path of the certificate file of credentials authorization details |           string |  empty string | 
| `sink.nats.credentials.seeds`       |                 The paths of seeding files of credentials authorization details | array of strings |   empty array | 

Kafka specific configuration, which is only used if `sink.type` is set to `kafka`.

| Property                    |                                                                                                   Description |       Data Type | Default Value |
|-----------------------------|--------------------------------------------------------------------------------------------------------------:|----------------:|--------------:|
| `sink.kafka.brokers`        |                                                                                         The Kafka broker urls | array of string |   empty array |
| `sink.kafka.idempotent`     |                                                        The property defines if message handling is idempotent |         boolean |         false |
| `sink.kafka.sasl.enabled`   |                                                         The property defines if SASL authorization is enabled |         boolean |         false | 
| `sink.kafka.sasl.user`      |                                                             The user value to be used with SASL authorization |          string |  empty string | 
| `sink.kafka.sasl.password`  |                                                         The password value to be used with SASL authorization |          string |  empty string | 
| `sink.kafka.sasl.mechanism` |                                    The mechanism to be used with SASL authorization. Valid values are `PLAIN` |          string |       `PLAIN` | 
| `sink.kafka.tls.enabled`    |                                                                        The property defines if TLS is enabled |         boolean |         false | 
| `sink.kafka.tls.skipverify` |                                           The property defines if verification of TLS certificates is skipped |         boolean |         false | 
| `sink.kafka.tls.clientauth` | The property defines the client auth value (as defined in [Go](https://pkg.go.dev/crypto/tls#ClientAuthType)) |         boolean |         false | 

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
