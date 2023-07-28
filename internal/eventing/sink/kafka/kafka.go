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

package kafka

import (
	"crypto/tls"
	"github.com/Shopify/sarama"
	sinkimpl "github.com/noctarius/timescaledb-event-streamer/internal/eventing/sink"
	config "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/encoding"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"time"
)

func init() {
	sinkimpl.RegisterSink(config.Kafka, newKafkaSink)
}

type kafkaSink struct {
	producer sarama.SyncProducer
	encoder  *encoding.JsonEncoder
}

func newKafkaSink(
	c *config.Config,
) (sink.Sink, error) {

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.ClientID = "event-stream-prototype"
	kafkaConfig.Producer.Idempotent = config.GetOrDefault(
		c, "sink.kafka.idempotent", false,
	)
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	kafkaConfig.Producer.Retry.Max = 10

	if config.GetOrDefault(c, config.PropertyKafkaSaslEnabled, false) {
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.User = config.GetOrDefault(
			c, config.PropertyKafkaSaslUser, "",
		)
		kafkaConfig.Net.SASL.Password = config.GetOrDefault(
			c, config.PropertyKafkaSaslPassword, "",
		)
		kafkaConfig.Net.SASL.Mechanism = config.GetOrDefault[sarama.SASLMechanism](
			c, config.PropertyKafkaSaslMechanism, sarama.SASLTypePlaintext,
		)
	}

	if config.GetOrDefault(c, config.PropertyKafkaTlsEnabled, false) {
		kafkaConfig.Net.TLS.Enable = true
		kafkaConfig.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: config.GetOrDefault(
				c, config.PropertyKafkaTlsSkipVerify, false,
			),
			ClientAuth: config.GetOrDefault(
				c, config.PropertyKafkaTlsClientAuth, tls.NoClientCert,
			),
		}
	}

	producer, err := sarama.NewSyncProducer(
		config.GetOrDefault(c, config.PropertyKafkaBrokers, []string{"localhost:9092"}), kafkaConfig,
	)
	if err != nil {
		return nil, err
	}

	return &kafkaSink{
		producer: producer,
		encoder:  encoding.NewJsonEncoderWithConfig(c),
	}, nil
}

func (k *kafkaSink) Start() error {
	return nil
}

func (k *kafkaSink) Stop() error {
	return k.producer.Close()
}

func (k *kafkaSink) Emit(
	_ sink.Context, timestamp time.Time, topicName string, key, envelope schema.Struct,
) error {

	keyData, err := k.encoder.Marshal(key)
	if err != nil {
		return err
	}
	envelopeData, err := k.encoder.Marshal(envelope)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic:     topicName,
		Key:       sarama.ByteEncoder(keyData),
		Value:     sarama.ByteEncoder(envelopeData),
		Timestamp: timestamp,
	}

	_, _, err = k.producer.SendMessage(msg)
	return err
}
