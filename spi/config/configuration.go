package config

import (
	"crypto/tls"
	"github.com/Shopify/sarama"
	"os"
	"reflect"
	"strings"
	"time"
)

type OffsetStorageType string

const (
	NoneStorage OffsetStorageType = "none"
	FileStorage OffsetStorageType = "file"
)

type SinkType string

const (
	Stdout SinkType = "stdout"
	NATS   SinkType = "nats"
	Kafka  SinkType = "kafka"
	Redis  SinkType = "redis"
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

type PostgreSQLConfig struct {
	Connection      string                `toml:"connection"`
	Password        string                `toml:"password"`
	Publication     PublicationConfig     `toml:"publication"`
	ReplicationSlot ReplicationSlotConfig `toml:"replicationslot"`
	Transaction     TransactionConfig     `toml:"transaction"`
}

type PublicationConfig struct {
	Name     string `toml:"name"`
	Create   *bool  `toml:"create"`
	AutoDrop *bool  `toml:"autodrop"`
}

type ReplicationSlotConfig struct {
	Name     string `toml:"name"`
	Create   *bool  `toml:"create"`
	AutoDrop *bool  `toml:"autodrop"`
}

type TransactionConfig struct {
	Window TransactionWindowConfig `toml:"window"`
}

type TransactionWindowConfig struct {
	Enabled *bool         `toml:"enabled"`
	Timeout time.Duration `toml:"timeout"`
	MaxSize uint          `toml:"maxsize"`
}

type SinkConfig struct {
	Type      SinkType                     `toml:"type"`
	Tombstone bool                         `toml:"tombstone"`
	Filters   map[string]EventFilterConfig `toml:"filters"`
	Nats      NatsConfig                   `toml:"nats"`
	Kafka     KafkaConfig                  `toml:"kafka"`
	Redis     RedisConfig                  `toml:"redis"`
}

type EventFilterConfig struct {
	Hypertables  *HypertablesConfig `toml:"hypertables"`
	DefaultValue *bool              `toml:"default"`
	Condition    string             `toml:"condition"`
}

type TopicConfig struct {
	NamingStrategy TopicNamingStrategyConfig `toml:"namingstrategy"`
	Prefix         string                    `toml:"prefix"`
}

type TimescaleDBConfig struct {
	Hypertables HypertablesConfig     `toml:"hypertables"`
	Events      TimescaleEventsConfig `toml:"events"`
}

type NatsUserInfoConfig struct {
	Username string `toml:"username"`
	Password string `toml:"password"`
}

type NatsCredentialsConfig struct {
	Certificate string   `toml:"certificate"`
	Seeds       []string `toml:"seeds"`
}

type NatsJWTConfig struct {
	JWT  string `toml:"jwt"`
	Seed string `toml:"seed"`
}

type NatsConfig struct {
	Address       string                `toml:"address"`
	Authorization NatsAuthorizationType `toml:"authorization"`
	UserInfo      NatsUserInfoConfig    `toml:"userinfo"`
	Credentials   NatsCredentialsConfig `toml:"credentials"`
	JWT           NatsJWTConfig         `toml:"jwt"`
}

type KafkaSaslConfig struct {
	Enabled   bool                 `toml:"user"`
	User      string               `toml:"user"`
	Password  string               `toml:"password"`
	Mechanism sarama.SASLMechanism `toml:"mechanism"`
}

type KafkaConfig struct {
	Brokers    []string        `toml:"brokers"`
	Idempotent bool            `toml:"idempotent"`
	Sasl       KafkaSaslConfig `toml:"sasl"`
	TLS        TLSConfig       `toml:"tls"`
}

type RedisConfig struct {
	Network  string             `toml:"network"`
	Address  string             `toml:"address"`
	Password string             `toml:"password"`
	Database int                `toml:"database"`
	Retries  RedisRetryConfig   `toml:"retries"`
	Timeouts RedisTimeoutConfig `toml:"timeouts"`
	PoolSize int                `toml:"poolsize"`
	TLS      TLSConfig          `toml:"tls"`
}

type RedisRetryConfig struct {
	MaxAttempts int                     `toml:"maxattempts"`
	Backoff     RedisRetryBackoffConfig `toml:"backoff"`
}

type RedisRetryBackoffConfig struct {
	Min int `toml:"min"`
	Max int `toml:"max"`
}

type RedisTimeoutConfig struct {
	Dial  int `toml:"dial"`
	Read  int `toml:"read"`
	Write int `toml:"write"`
	Pool  int `toml:"pool"`
	Idle  int `toml:"idle"`
}

type TopicNamingStrategyConfig struct {
	Type NamingStrategyType `toml:"type"`
}

type TLSConfig struct {
	Enabled    bool               `toml:"enabled"`
	SkipVerify bool               `toml:"skipverify"`
	ClientAuth tls.ClientAuthType `toml:"clientauth"`
}

type HypertablesConfig struct {
	Excludes []string `toml:"excludes"`
	Includes []string `toml:"includes"`
}

type TimescaleEventsConfig struct {
	Read          bool `toml:"read"`
	Insert        bool `toml:"insert"`
	Update        bool `toml:"update"`
	Delete        bool `toml:"delete"`
	Truncate      bool `toml:"truncate"`
	Message       bool `toml:"message"`
	Compression   bool `toml:"compression"`
	Decompression bool `toml:"decompression"`
}

type Config struct {
	PostgreSQL    PostgreSQLConfig    `toml:"postgresql"`
	Sink          SinkConfig          `toml:"sink"`
	Topic         TopicConfig         `toml:"topic"`
	TimescaleDB   TimescaleDBConfig   `toml:"timescaledb"`
	Logging       LoggerConfig        `toml:"logging"`
	OffsetStorage OffsetStorageConfig `toml:"offsetstorage"`
}

type OffsetStorageConfig struct {
	Type        OffsetStorageType `toml:"type"`
	FileStorage FileStorageConfig `toml:"file"`
}

type FileStorageConfig struct {
	Path string `toml:"path"`
}

type LoggerConfig struct {
	Level   string                     `toml:"level"`
	Outputs LoggerOutputConfig         `toml:"output"`
	Loggers map[string]SubLoggerConfig `toml:"loggers"`
}

type LoggerOutputConfig struct {
	Console LoggerConsoleConfig `toml:"console"`
	File    LoggerFileConfig    `toml:"file"`
}

type SubLoggerConfig struct {
	Level   *string            `toml:"level"`
	Outputs LoggerOutputConfig `toml:"output"`
}

type LoggerConsoleConfig struct {
	Enabled *bool `toml:"enabled"`
}

type LoggerFileConfig struct {
	Enabled     *bool          `toml:"enabled"`
	Path        string         `toml:"path"`
	Rotate      *bool          `toml:"rotate"`
	MaxSize     *string        `toml:"maxsize"`
	MaxDuration *time.Duration `toml:"maxduration"`
	Compress    bool           `toml:"compress"`
}

func GetOrDefault[V any](config *Config, canonicalProperty string, defaultValue V) V {
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

	if !element.IsZero() &&
		!(element.Kind() == reflect.Ptr && element.IsNil()) {

		if element.Kind() == reflect.Ptr {
			element = element.Elem()
		}

		return element.Convert(reflect.TypeOf(defaultValue)).Interface().(V)
	}
	return defaultValue
}

func findEnvProperty[V any](canonicalProperty string, defaultValue V) (V, bool) {
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

func findProperty(element reflect.Value, property string) (reflect.Value, bool) {
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
