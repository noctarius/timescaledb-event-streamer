package configuration

import (
	"crypto/tls"
	"github.com/Shopify/sarama"
	"os"
	"reflect"
	"strings"
)

type SinkType string

const (
	Stdout SinkType = "stdout"
	NATS   SinkType = "nats"
	Kafka  SinkType = "kafka"
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

type Config struct {
	PostgreSQL struct {
		Connection  string `toml:"connection"`
		Password    string `toml:"password"`
		Publication string `toml:"publication"`
	} `toml:"postgresql"`

	Sink struct {
		Type SinkType `toml:"type"`
		Nats struct {
			Address       string                `toml:"address"`
			Authorization NatsAuthorizationType `toml:"authorization"`
			UserInfo      struct {
				Username string `toml:"username"`
				Password string `toml:"password"`
			} `toml:"userinfo"`
			Credentials struct {
				Certificate string   `toml:"certificate"`
				Seeds       []string `toml:"seeds"`
			} `toml:"credentials"`
			JWT struct {
				JWT  string `toml:"jwt"`
				Seed string `toml:"seed"`
			} `toml:"jwt"`
		} `toml:"nats"`
		Kafka struct {
			Brokers    []string `toml:"brokers"`
			Idempotent bool     `toml:"idempotent"`
			Sasl       struct {
				Enabled   bool                 `toml:"user"`
				User      string               `toml:"user"`
				Password  string               `toml:"password"`
				Mechanism sarama.SASLMechanism `toml:"mechanism"`
			} `toml:"sasl"`
			TLS struct {
				Enabled    bool               `toml:"enabled"`
				SkipVerify bool               `toml:"skipverify"`
				ClientAuth tls.ClientAuthType `toml:"clientauth"`
			} `toml:"tls"`
		} `toml:"kafka"`
	} `toml:"sink"`

	Topic struct {
		NamingStrategy struct {
			Type NamingStrategyType `toml:"type"`
		} `toml:"namingstrategy"`
		Prefix string `toml:"prefix"`
	} `toml:"topic"`

	TimescaleDB struct {
		Hypertables struct {
			Excludes []string `toml:"excludes"`
			Includes []string `toml:"includes"`
		} `toml:"hypertables"`
		Events struct {
			Read          bool `toml:"read"`
			Insert        bool `toml:"insert"`
			Update        bool `toml:"update"`
			Delete        bool `toml:"delete"`
			Truncate      bool `toml:"truncate"`
			Compression   bool `toml:"compression"`
			Decompression bool `toml:"decompression"`
		} `toml:"events"`
	} `toml:"timescaledb"`
}

func GetOrDefault[V any](config *Config, canonicalProperty string, defaultValue V) V {
	if env, ok := findEnvProperty(canonicalProperty, defaultValue); ok {
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
	return reflect.Zero(t).Interface().(V), false
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
