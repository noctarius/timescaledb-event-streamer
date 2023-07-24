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

import (
	"crypto/tls"
	"github.com/Shopify/sarama"
	"os"
	"reflect"
	"strings"
)

type StateStorageType string

const (
	NoneStorage StateStorageType = "none"
	FileStorage StateStorageType = "file"
)

type SinkType string

const (
	Stdout     SinkType = "stdout"
	NATS       SinkType = "nats"
	Kafka      SinkType = "kafka"
	Redis      SinkType = "redis"
	AwsKinesis SinkType = "kinesis"
	AwsSQS     SinkType = "sqs"
)

type NamingStrategyType string

const (
	Debezium NamingStrategyType = "debezium"
)

type NatsAuthorizationType string

const (
	UserInfo    NatsAuthorizationType = "userinfo"
	Credentials NatsAuthorizationType = "credentials"
	Jwt         NatsAuthorizationType = "jwt"
)

type InitialSnapshotMode string

const (
	Always      InitialSnapshotMode = "always"
	Never       InitialSnapshotMode = "never"
	InitialOnly InitialSnapshotMode = "initial_only"
)

type PostgreSQLConfig struct {
	Connection      string                `toml:"connection" yaml:"connection"`
	Password        string                `toml:"password" yaml:"password"`
	Publication     PublicationConfig     `toml:"publication" yaml:"publication"`
	ReplicationSlot ReplicationSlotConfig `toml:"replicationslot" yaml:"replicationSlot"`
	Transaction     TransactionConfig     `toml:"transaction" yaml:"transaction"`
	Snapshot        SnapshotConfig        `toml:"snapshot" yaml:"snapshot"`
}

type InternalConfig struct {
	Dispatcher DispatcherConfig `toml:"dispatcher" yaml:"dispatcher"`
}

type DispatcherConfig struct {
	MaxQueueSize uint `toml:"maxqueuesize" yaml:"maxQueueSize"`
}

type SnapshotConfig struct {
	BatchSize uint                 `toml:"batchsize" yaml:"batchSize"`
	Initial   *InitialSnapshotMode `toml:"initial" yaml:"initial"`
}

type PublicationConfig struct {
	Name     string `toml:"name" yaml:"name"`
	Create   *bool  `toml:"create" yaml:"create"`
	AutoDrop *bool  `toml:"autodrop" yaml:"autoDrop"`
}

type ReplicationSlotConfig struct {
	Name     string `toml:"name" yaml:"name"`
	Create   *bool  `toml:"create" yaml:"create"`
	AutoDrop *bool  `toml:"autodrop" yaml:"autoDrop"`
}

type TransactionConfig struct {
	Window TransactionWindowConfig `toml:"window" yaml:"window"`
}

type TransactionWindowConfig struct {
	Enabled *bool `toml:"enabled" yaml:"enabled"`
	Timeout int   `toml:"timeout" yaml:"timeout"`
	MaxSize uint  `toml:"maxsize" yaml:"maxSize"`
}

type SinkConfig struct {
	Type       SinkType                     `toml:"type" yaml:"type"`
	Tombstone  bool                         `toml:"tombstone" yaml:"tombstone"`
	Filters    map[string]EventFilterConfig `toml:"filters" yaml:"filters"`
	Nats       NatsConfig                   `toml:"nats" yaml:"nats"`
	Kafka      KafkaConfig                  `toml:"kafka" yaml:"kafka"`
	Redis      RedisConfig                  `toml:"redis" yaml:"redis"`
	AwsKinesis AwsKinesisConfig             `toml:"kinesis" yaml:"kinesis"`
	AwsSqs     AwsSqsConfig                 `toml:"sqs" yaml:"sqs"`
}

type EventFilterConfig struct {
	Hypertables  *HypertablesConfig `toml:"hypertables" yaml:"hypertables"`
	DefaultValue *bool              `toml:"default" yaml:"default"`
	Condition    string             `toml:"condition" yaml:"condition"`
}

type TopicConfig struct {
	NamingStrategy TopicNamingStrategyConfig `toml:"namingstrategy" yaml:"namingStrategy"`
	Prefix         string                    `toml:"prefix" yaml:"prefix"`
}

type TimescaleDBConfig struct {
	Hypertables HypertablesConfig     `toml:"hypertables" yaml:"hypertables"`
	Events      TimescaleEventsConfig `toml:"events" yaml:"events"`
}

type NatsUserInfoConfig struct {
	Username string `toml:"username" yaml:"username"`
	Password string `toml:"password" yaml:"password"`
}

type NatsCredentialsConfig struct {
	Certificate string   `toml:"certificate" yaml:"certificate"`
	Seeds       []string `toml:"seeds" yaml:"seeds"`
}

type NatsJWTConfig struct {
	JWT  string `toml:"jwt" yaml:"jwt"`
	Seed string `toml:"seed" yaml:"Seed"`
}

type NatsConfig struct {
	Address       string                `toml:"address" yaml:"address"`
	Authorization NatsAuthorizationType `toml:"authorization" yaml:"authorization"`
	UserInfo      NatsUserInfoConfig    `toml:"userinfo" yaml:"userInfo"`
	Credentials   NatsCredentialsConfig `toml:"credentials" yaml:"credentials"`
	JWT           NatsJWTConfig         `toml:"jwt" yaml:"jwt"`
}

type KafkaSaslConfig struct {
	Enabled   bool                 `toml:"enabled" yaml:"enabled"`
	User      string               `toml:"user" yaml:"user"`
	Password  string               `toml:"password" yaml:"password"`
	Mechanism sarama.SASLMechanism `toml:"mechanism" yaml:"mechanism"`
}

type KafkaConfig struct {
	Brokers    []string        `toml:"brokers" yaml:"brokers"`
	Idempotent bool            `toml:"idempotent" yaml:"idempotent"`
	Sasl       KafkaSaslConfig `toml:"sasl" yaml:"sasl"`
	TLS        TLSConfig       `toml:"tls" yaml:"tls"`
}

type RedisConfig struct {
	Network  string             `toml:"network" yaml:"network"`
	Address  string             `toml:"address" yaml:"address"`
	Password string             `toml:"password" yaml:"password"`
	Database int                `toml:"database" yaml:"database"`
	Retries  RedisRetryConfig   `toml:"retries" yaml:"retries"`
	Timeouts RedisTimeoutConfig `toml:"timeouts" yaml:"timeouts"`
	PoolSize int                `toml:"poolsize" yaml:"poolSize"`
	TLS      TLSConfig          `toml:"tls" yaml:"tls"`
}

type RedisRetryConfig struct {
	MaxAttempts int                     `toml:"maxattempts" yaml:"maxAttempts"`
	Backoff     RedisRetryBackoffConfig `toml:"backoff" yaml:"backoff"`
}

type RedisRetryBackoffConfig struct {
	Min int `toml:"min" yaml:"min"`
	Max int `toml:"max" yaml:"max"`
}

type RedisTimeoutConfig struct {
	Dial  int `toml:"dial" yaml:"dial"`
	Read  int `toml:"read" yaml:"read"`
	Write int `toml:"write" yaml:"write"`
	Pool  int `toml:"pool" yaml:"pool"`
	Idle  int `toml:"idle" yaml:"idle"`
}

type TopicNamingStrategyConfig struct {
	Type NamingStrategyType `toml:"type" yaml:"type"`
}

type TLSConfig struct {
	Enabled    bool               `toml:"enabled" yaml:"enabled"`
	SkipVerify bool               `toml:"skipverify" yaml:"skipVerify"`
	ClientAuth tls.ClientAuthType `toml:"clientauth" yaml:"clientAuth"`
}

type HypertablesConfig struct {
	Excludes []string `toml:"excludes" yaml:"excludes"`
	Includes []string `toml:"includes" yaml:"includes"`
}

type TimescaleEventsConfig struct {
	Read          bool `toml:"read" yaml:"read"`
	Insert        bool `toml:"insert" yaml:"insert"`
	Update        bool `toml:"update" yaml:"update"`
	Delete        bool `toml:"delete" yaml:"delete"`
	Truncate      bool `toml:"truncate" yaml:"truncate"`
	Message       bool `toml:"message" yaml:"message"`
	Compression   bool `toml:"compression" yaml:"compression"`
	Decompression bool `toml:"decompression" yaml:"decompression"`
}

type AwsKinesisConfig struct {
	Stream AwsKinesisStreamConfig `toml:"stream" yaml:"stream"`
	Aws    AwsConnectionConfig    `toml:"aws" yaml:"aws"`
}

type AwsKinesisStreamConfig struct {
	Name       *string `toml:"name" yaml:"name"`
	Create     *bool   `toml:"create" yaml:"create"`
	ShardCount *int64  `toml:"shardcount" yaml:"shardCount"`
	Mode       *string `toml:"mode" yaml:"mode"`
}

type AwsSqsConfig struct {
	Queue AwsSqsQueueConfig   `toml:"queue" yaml:"queue"`
	Aws   AwsConnectionConfig `toml:"aws" yaml:"aws"`
}

type AwsSqsQueueConfig struct {
	Url *string `toml:"url" yaml:"url"`
}

type AwsConnectionConfig struct {
	Region          *string `toml:"region" yaml:"region"`
	Endpoint        string  `toml:"endpoint" yaml:"endpoint"`
	AccessKeyId     string  `toml:"accesskeyid" yaml:"accessKeyId"`
	SecretAccessKey string  `toml:"secretaccesskey" yaml:"secretAccessKey"`
	SessionToken    string  `toml:"sessiontoken" yaml:"sessionToken"`
}

type Config struct {
	PostgreSQL   PostgreSQLConfig   `toml:"postgresql" yaml:"postgresql"`
	Sink         SinkConfig         `toml:"sink" yaml:"sink"`
	Topic        TopicConfig        `toml:"topic" yaml:"topic"`
	TimescaleDB  TimescaleDBConfig  `toml:"timescaledb" yaml:"timescaledb"`
	Logging      LoggerConfig       `toml:"logging" yaml:"logging"`
	StateStorage StateStorageConfig `toml:"statestorage" yaml:"stateStorage"`
	Internal     InternalConfig     `toml:"internal" yaml:"internal"`
	Plugins      []string           `toml:"plugins" yaml:"plugins"`
}

type StateStorageConfig struct {
	Type        StateStorageType  `toml:"type" yaml:"type"`
	FileStorage FileStorageConfig `toml:"file" yaml:"file"`
}

type FileStorageConfig struct {
	Path string `toml:"path" yaml:"path"`
}

type LoggerConfig struct {
	Level   string                     `toml:"level" yaml:"level"`
	Outputs LoggerOutputConfig         `toml:"outputs" yaml:"outputs"`
	Loggers map[string]SubLoggerConfig `toml:"loggers" yaml:"loggers"`
}

type LoggerOutputConfig struct {
	Console LoggerConsoleConfig `toml:"console" yaml:"console"`
	File    LoggerFileConfig    `toml:"file" yaml:"file"`
}

type SubLoggerConfig struct {
	Level   *string            `toml:"level" yaml:"level"`
	Outputs LoggerOutputConfig `toml:"outputs" yaml:"outputs"`
}

type LoggerConsoleConfig struct {
	Enabled *bool `toml:"enabled" yaml:"enabled"`
}

type LoggerFileConfig struct {
	Enabled     *bool   `toml:"enabled" yaml:"enabled"`
	Path        string  `toml:"path" yaml:"path"`
	Rotate      *bool   `toml:"rotate" yaml:"rotate"`
	MaxSize     *string `toml:"maxsize" yaml:"maxSize"`
	MaxDuration *int    `toml:"maxduration" yaml:"maxDuration"`
	Compress    bool    `toml:"compress" yaml:"compress"`
}

func GetOrDefault[V any](
	config *Config, canonicalProperty string, defaultValue V,
) V {

	if env, found := findEnvProperty(canonicalProperty, defaultValue); found {
		return env
	}

	properties := strings.Split(canonicalProperty, ".")

	element := reflect.ValueOf(*config)
	for _, property := range properties {
		if e, ok := findProperty(element, property); ok {
			element = e
		} else {
			return defaultValue
		}
	}

	defaultValueType := reflect.TypeOf(defaultValue)
	isPtr := defaultValueType.Kind() == reflect.Ptr

	if !element.IsZero() &&
		!(element.Kind() == reflect.Ptr && element.IsNil()) {

		if element.Kind() == reflect.Ptr {
			if isPtr {
				return element.Convert(defaultValueType).Interface().(V)
			}

			element = element.Elem()
		}

		if isPtr {
			// Some magic needs to be done here to get a
			// pointer of a non-addressable value
			newElement := reflect.New(defaultValueType.Elem())
			newElement.Elem().Set(element)
			element = newElement
		}

		return element.Convert(defaultValueType).Interface().(V)
	}
	return defaultValue
}

func findEnvProperty[V any](
	canonicalProperty string, defaultValue V,
) (V, bool) {

	t := reflect.TypeOf(defaultValue)

	envVarName := strings.ToUpper(canonicalProperty)
	envVarName = strings.ReplaceAll(envVarName, "_", "__")
	envVarName = strings.ReplaceAll(envVarName, ".", "_")
	if val, ok := os.LookupEnv(envVarName); ok {
		v := reflect.ValueOf(val)
		cv := v.Convert(t)
		if !cv.IsZero() &&
			!(cv.Kind() == reflect.Ptr && cv.IsNil()) {
			return cv.Interface().(V), true
		}
	}
	return defaultValue, false
}

func findProperty(
	element reflect.Value, property string,
) (reflect.Value, bool) {

	t := element.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" && !f.Anonymous {
			continue
		}

		if f.Tag.Get("toml") == property {
			return element.Field(i), true
		}
	}
	return reflect.Value{}, false
}
