/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package config

const (
	PropertyPostgresqlConnection              = "postgresql.connection"
	PropertyPostgresqlPassword                = "postgresql.password"
	PropertyPostgresqlPublicationName         = "postgresql.publication.name"
	PropertyPostgresqlPublicationCreate       = "postgresql.publication.create"
	PropertyPostgresqlPublicationAutoDrop     = "postgresql.publication.autodrop"
	PropertyPostgresqlSnapshotInitialMode     = "postgresql.snapshot.initial"
	PropertyPostgresqlSnapshotBatchsize       = "postgresql.snapshot.batchsize"
	PropertyPostgresqlReplicationSlotName     = "postgresql.replicationslot.name"
	PropertyPostgresqlReplicationSlotCreate   = "postgresql.replicationslot.create"
	PropertyPostgresqlReplicationSlotAutoDrop = "postgresql.replicationslot.autodrop"
	PropertyPostgresqlTxwindowEnabled         = "postgresql.transaction.window.enabled"
	PropertyPostgresqlTxwindowTimeout         = "postgresql.transaction.window.timeout"
	PropertyPostgresqlTxwindowMaxsize         = "postgresql.transaction.window.maxsize"

	PropertySink          = "sink.type"
	PropertySinkTombstone = "sink.tombstone"

	PropertyStatsEnabled        = "stats.enabled"
	PropertyRuntimeStatsEnabled = "stats.runtime.enabled"

	PropertyStateStorageType     = "statestorage.type"
	PropertyFileStateStoragePath = "statestorage.file.path"

	PropertyDispatcherInitialQueueCapacity = "internal.dispatcher.initialqueuecapacity"
	PropertySnapshotterParallelism         = "internal.snapshotter.parallelism"
	PropertyEncodingCustomReflection       = "internal.encoding.customreflection"

	PropertyHypertableEventsRead          = "timescaledb.events.read"
	PropertyHypertableEventsInsert        = "timescaledb.events.insert"
	PropertyHypertableEventsUpdate        = "timescaledb.events.update"
	PropertyHypertableEventsDelete        = "timescaledb.events.delete"
	PropertyHypertableEventsTruncate      = "timescaledb.events.truncate"
	PropertyHypertableEventsCompression   = "timescaledb.events.compression"
	PropertyHypertableEventsDecompression = "timescaledb.events.decompression"
	PropertyHypertableEventsMessage       = "timescaledb.events.message" // FIXME: deprecated

	PropertyPostgresqlEventsRead     = "postgresql.events.read"
	PropertyPostgresqlEventsInsert   = "postgresql.events.insert"
	PropertyPostgresqlEventsUpdate   = "postgresql.events.update"
	PropertyPostgresqlEventsDelete   = "postgresql.events.delete"
	PropertyPostgresqlEventsTruncate = "postgresql.events.truncate"
	PropertyPostgresqlEventsMessage  = "postgresql.events.message"

	PropertyNamingStrategy = "topic.namingstrategy.type"

	PropertyKafkaBrokers       = "sink.kafka.brokers"
	PropertyKafkaSaslEnabled   = "sink.kafka.sasl.enabled"
	PropertyKafkaSaslUser      = "sink.kafka.sasl.user"
	PropertyKafkaSaslPassword  = "sink.kafka.sasl.password"
	PropertyKafkaSaslMechanism = "sink.kafka.sasl.mechanism"
	PropertyKafkaTlsEnabled    = "sink.kafka.tls.enabled"
	PropertyKafkaTlsSkipVerify = "sink.kafka.tls.skipverify"
	PropertyKafkaTlsClientAuth = "sink.kafka.tls.clientauth"

	PropertyNatsAddress                = "sink.nats.address"
	PropertyNatsAuthorization          = "sink.nats.authorization"
	PropertyNatsUserinfoUsername       = "sink.nats.userinfo.username"
	PropertyNatsUserinfoPassword       = "sink.nats.userinfo.password"
	PropertyNatsCredentialsCertificate = "sink.nats.credentials.certificate"
	PropertyNatsCredentialsSeeds       = "sink.nats.credentials.seeds"
	PropertyNatsJwt                    = "sink.nats.jwt.jwt"
	PropertyNatsJwtSeed                = "sink.nats.jwt.seed"

	PropertyRedisNetwork           = "sink.redis.network"
	PropertyRedisAddress           = "sink.redis.address"
	PropertyRedisPassword          = "sink.redis.password"
	PropertyRedisDatabase          = "sink.redis.database"
	PropertyRedisPoolsize          = "sink.redis.poolsize"
	PropertyRedisRetriesMax        = "sink.redis.retries.maxattempts"
	PropertyRedisRetriesBackoffMin = "sink.redis.retries.backoff.min"
	PropertyRedisRetriesBackoffMax = "sink.redis.retries.backoff.max"
	PropertyRedisTimeoutDial       = "sink.redis.timeouts.dial"
	PropertyRedisTimeoutRead       = "sink.redis.timeouts.read"
	PropertyRedisTimeoutWrite      = "sink.redis.timeouts.write"
	PropertyRedisTimeoutPool       = "sink.redis.timeouts.pool"
	PropertyRedisTimeoutIdle       = "sink.redis.timeouts.idle"
	PropertyRedisTlsSkipVerify     = "sink.redis.tls.skipverify"
	PropertyRedisTlsClientAuth     = "sink.redis.tls.clientauth"

	PropertyKinesisStreamName         = "sink.kinesis.stream.name"
	PropertyKinesisStreamCreate       = "sink.kinesis.stream.create"
	PropertyKinesisStreamShardCount   = "sink.kinesis.stream.shardcount"
	PropertyKinesisStreamMode         = "sink.kinesis.stream.mode"
	PropertyKinesisRegion             = "sink.kinesis.aws.region"
	PropertyKinesisAwsEndpoint        = "sink.kinesis.aws.endpoint"
	PropertyKinesisAwsAccessKeyId     = "sink.kinesis.aws.accesskeyid"
	PropertyKinesisAwsSecretAccessKey = "sink.kinesis.aws.secretaccesskey"
	PropertyKinesisAwsSessionToken    = "sink.kinesis.aws.sessiontoken"

	PropertySqsQueueUrl           = "sink.sqs.queue.url"
	PropertySqsAwsRegion          = "sink.sqs.aws.region"
	PropertySqsAwsEndpoint        = "sink.sqs.aws.endpoint"
	PropertySqsAwsAccessKeyId     = "sink.sqs.aws.accesskeyid"
	PropertySqsAwsSecretAccessKey = "sink.sqs.aws.secretaccesskey"
	PropertySqsAwsSessionToken    = "sink.sqs.aws.sessiontoken"

	PropertyHttpUrl                             = "sink.http.url"
	PropertyHttpAuthenticationType              = "sink.http.authentication.type"
	PropertyHttpBasicAuthenticationUsername     = "sink.http.authentication.basic.username"
	PropertyHttpBasicAuthenticationPassword     = "sink.http.authentication.basic.password"
	PropertyHttpHeaderAuthenticationHeaderName  = "sink.http.authentication.header.name"
	PropertyHttpHeaderAuthenticationHeaderValue = "sink.http.authentication.header.value"
	PropertyHttpTlsEnabled                      = "sink.http.tls.enabled"
	PropertyHttpTlsSkipVerify                   = "sink.http.tls.skipverify"
	PropertyHttpTlsClientAuth                   = "sink.http.tls.clientauth"
)
