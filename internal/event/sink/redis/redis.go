package redis

import (
	"crypto/tls"
	"encoding/json"
	"github.com/go-redis/redis"
	"github.com/noctarius/timescaledb-event-streamer/internal/configuring"
	"github.com/noctarius/timescaledb-event-streamer/internal/event/sink"
	"github.com/noctarius/timescaledb-event-streamer/internal/schema"
	"time"
)

type redisSink struct {
	client *redis.Client
}

func NewRedisSink(config *configuring.Config) (sink.Sink, error) {
	options := &redis.Options{
		Network: configuring.GetOrDefault(
			config, "sink.redis.network", "tcp",
		),
		Addr: configuring.GetOrDefault(
			config, "sink.redis.address", "localhost:6379",
		),
		Password: configuring.GetOrDefault(
			config, "sink.redis.password", "",
		),
		DB: configuring.GetOrDefault(
			config, "sink.redis.database", 0,
		),
		MaxRetries: configuring.GetOrDefault(
			config, "sink.redis.retries.maxattempts", 0,
		),
		MinRetryBackoff: configuring.GetOrDefault(
			config, "sink.redis.retries.backoff.min", time.Duration(8),
		) * time.Microsecond,
		MaxRetryBackoff: configuring.GetOrDefault(
			config, "sink.redis.retries.backoff.max", time.Duration(512),
		) * time.Microsecond,
		DialTimeout: configuring.GetOrDefault(
			config, "sink.redis.timeouts.dial", time.Duration(0),
		),
		ReadTimeout: configuring.GetOrDefault(
			config, "sink.redis.timeouts.read", time.Duration(0),
		) * time.Second,
		WriteTimeout: configuring.GetOrDefault(
			config, "sink.redis.timeouts.write", time.Duration(0),
		) * time.Second,
		PoolSize: configuring.GetOrDefault(
			config, "sink.redis.poolsize", 0,
		),
		PoolTimeout: configuring.GetOrDefault(
			config, "sink.redis.timeouts.pool", time.Duration(0),
		) * time.Second,
		IdleTimeout: configuring.GetOrDefault(
			config, "sink.redis.timeouts.idle", time.Duration(0),
		) * time.Minute,
	}

	if config.Sink.Redis.TLS.Enabled {
		options.TLSConfig = &tls.Config{
			InsecureSkipVerify: configuring.GetOrDefault(
				config, "sink.redis.tls.skipverify", false,
			),
			ClientAuth: configuring.GetOrDefault(
				config, "sink.redis.tls.clientauth", tls.NoClientCert,
			),
		}
	}

	return &redisSink{
		client: redis.NewClient(options),
	}, nil
}

func (r *redisSink) Emit(_ time.Time, topicName string, key, envelope schema.Struct) error {
	keyData, err := json.Marshal(key)
	if err != nil {
		return err
	}
	envelopeData, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	return r.client.XAdd(&redis.XAddArgs{
		Stream: topicName,
		Values: map[string]any{
			"key":      string(keyData),
			"envelope": string(envelopeData),
		},
	}).Err()
}
