package redis

import (
	"crypto/tls"
	"encoding/json"
	"github.com/go-redis/redis"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"time"
)

func init() {
	sink.RegisterSink(spiconfig.Redis, newRedisSink)
}

type redisSink struct {
	client *redis.Client
}

func newRedisSink(config *spiconfig.Config) (sink.Sink, error) {
	options := &redis.Options{
		Network: spiconfig.GetOrDefault(
			config, "sink.redis.network", "tcp",
		),
		Addr: spiconfig.GetOrDefault(
			config, "sink.redis.address", "localhost:6379",
		),
		Password: spiconfig.GetOrDefault(
			config, "sink.redis.password", "",
		),
		DB: spiconfig.GetOrDefault(
			config, "sink.redis.database", 0,
		),
		MaxRetries: spiconfig.GetOrDefault(
			config, "sink.redis.retries.maxattempts", 0,
		),
		MinRetryBackoff: spiconfig.GetOrDefault(
			config, "sink.redis.retries.backoff.min", time.Duration(8),
		) * time.Microsecond,
		MaxRetryBackoff: spiconfig.GetOrDefault(
			config, "sink.redis.retries.backoff.max", time.Duration(512),
		) * time.Microsecond,
		DialTimeout: spiconfig.GetOrDefault(
			config, "sink.redis.timeouts.dial", time.Duration(0),
		),
		ReadTimeout: spiconfig.GetOrDefault(
			config, "sink.redis.timeouts.read", time.Duration(0),
		) * time.Second,
		WriteTimeout: spiconfig.GetOrDefault(
			config, "sink.redis.timeouts.write", time.Duration(0),
		) * time.Second,
		PoolSize: spiconfig.GetOrDefault(
			config, "sink.redis.poolsize", 0,
		),
		PoolTimeout: spiconfig.GetOrDefault(
			config, "sink.redis.timeouts.pool", time.Duration(0),
		) * time.Second,
		IdleTimeout: spiconfig.GetOrDefault(
			config, "sink.redis.timeouts.idle", time.Duration(0),
		) * time.Minute,
	}

	if config.Sink.Redis.TLS.Enabled {
		options.TLSConfig = &tls.Config{
			InsecureSkipVerify: spiconfig.GetOrDefault(
				config, "sink.redis.tls.skipverify", false,
			),
			ClientAuth: spiconfig.GetOrDefault(
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
