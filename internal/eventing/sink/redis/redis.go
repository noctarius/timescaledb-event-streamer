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

package redis

import (
	"crypto/tls"
	"github.com/go-redis/redis"
	sinkimpl "github.com/noctarius/timescaledb-event-streamer/internal/eventing/sink"
	config "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/encoding"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"time"
)

func init() {
	sinkimpl.RegisterSink(config.Redis, newRedisSink)
}

type redisSink struct {
	client  *redis.Client
	encoder *encoding.JsonEncoder
}

func newRedisSink(
	c *config.Config,
) (sink.Sink, error) {

	options := &redis.Options{
		Network: config.GetOrDefault(
			c, config.PropertyRedisNetwork, "tcp",
		),
		Addr: config.GetOrDefault(
			c, config.PropertyRedisAddress, "localhost:6379",
		),
		Password: config.GetOrDefault(
			c, config.PropertyRedisPassword, "",
		),
		DB: config.GetOrDefault(
			c, config.PropertyRedisDatabase, 0,
		),
		MaxRetries: config.GetOrDefault(
			c, config.PropertyRedisRetriesMax, 0,
		),
		MinRetryBackoff: config.GetOrDefault(
			c, config.PropertyRedisRetriesBackoffMin, time.Duration(8),
		) * time.Microsecond,
		MaxRetryBackoff: config.GetOrDefault(
			c, config.PropertyRedisRetriesBackoffMax, time.Duration(512),
		) * time.Microsecond,
		DialTimeout: config.GetOrDefault(
			c, config.PropertyRedisTimeoutDial, time.Duration(0),
		),
		ReadTimeout: config.GetOrDefault(
			c, config.PropertyRedisTimeoutRead, time.Duration(0),
		) * time.Second,
		WriteTimeout: config.GetOrDefault(
			c, config.PropertyRedisTimeoutWrite, time.Duration(0),
		) * time.Second,
		PoolSize: config.GetOrDefault(
			c, config.PropertyRedisPoolsize, 0,
		),
		PoolTimeout: config.GetOrDefault(
			c, config.PropertyRedisTimeoutPool, time.Duration(0),
		) * time.Second,
		IdleTimeout: config.GetOrDefault(
			c, config.PropertyRedisTimeoutIdle, time.Duration(0),
		) * time.Minute,
	}

	if c.Sink.Redis.TLS.Enabled {
		options.TLSConfig = &tls.Config{
			InsecureSkipVerify: config.GetOrDefault(
				c, config.PropertyRedisTlsSkipVerify, false,
			),
			ClientAuth: config.GetOrDefault(
				c, config.PropertyRedisTlsClientAuth, tls.NoClientCert,
			),
		}
	}

	return &redisSink{
		client:  redis.NewClient(options),
		encoder: encoding.NewJsonEncoderWithConfig(c),
	}, nil
}

func (r *redisSink) Start() error {
	return nil
}

func (r *redisSink) Stop() error {
	return r.client.Close()
}

func (r *redisSink) Emit(
	_ sink.Context, _ time.Time, topicName string, key, envelope schema.Struct,
) error {

	keyData, err := r.encoder.Marshal(key)
	if err != nil {
		return err
	}
	envelopeData, err := r.encoder.Marshal(envelope)
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
