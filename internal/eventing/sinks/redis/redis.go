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
			config, spiconfig.PropertyRedisNetwork, "tcp",
		),
		Addr: spiconfig.GetOrDefault(
			config, spiconfig.PropertyRedisAddress, "localhost:6379",
		),
		Password: spiconfig.GetOrDefault(
			config, spiconfig.PropertyRedisPassword, "",
		),
		DB: spiconfig.GetOrDefault(
			config, spiconfig.PropertyRedisDatabase, 0,
		),
		MaxRetries: spiconfig.GetOrDefault(
			config, spiconfig.PropertyRedisRetriesMax, 0,
		),
		MinRetryBackoff: spiconfig.GetOrDefault(
			config, spiconfig.PropertyRedisRetriesBackoffMin, time.Duration(8),
		) * time.Microsecond,
		MaxRetryBackoff: spiconfig.GetOrDefault(
			config, spiconfig.PropertyRedisRetriesBackoffMax, time.Duration(512),
		) * time.Microsecond,
		DialTimeout: spiconfig.GetOrDefault(
			config, spiconfig.PropertyRedisTimeoutDial, time.Duration(0),
		),
		ReadTimeout: spiconfig.GetOrDefault(
			config, spiconfig.PropertyRedisTimeoutRead, time.Duration(0),
		) * time.Second,
		WriteTimeout: spiconfig.GetOrDefault(
			config, spiconfig.PropertyRedisTimeoutWrite, time.Duration(0),
		) * time.Second,
		PoolSize: spiconfig.GetOrDefault(
			config, spiconfig.PropertyRedisPoolsize, 0,
		),
		PoolTimeout: spiconfig.GetOrDefault(
			config, spiconfig.PropertyRedisTimeoutPool, time.Duration(0),
		) * time.Second,
		IdleTimeout: spiconfig.GetOrDefault(
			config, spiconfig.PropertyRedisTimeoutIdle, time.Duration(0),
		) * time.Minute,
	}

	if config.Sink.Redis.TLS.Enabled {
		options.TLSConfig = &tls.Config{
			InsecureSkipVerify: spiconfig.GetOrDefault(
				config, spiconfig.PropertyRedisTlsSkipVerify, false,
			),
			ClientAuth: spiconfig.GetOrDefault(
				config, spiconfig.PropertyRedisTlsClientAuth, tls.NoClientCert,
			),
		}
	}

	return &redisSink{
		client: redis.NewClient(options),
	}, nil
}

func (r *redisSink) Start() error {
	return nil
}

func (r *redisSink) Stop() error {
	return r.client.Close()
}

func (r *redisSink) Emit(_ sink.Context, _ time.Time, topicName string, key, envelope schema.Struct) error {
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
