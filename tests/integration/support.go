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

package integration

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	inttest "github.com/noctarius/timescaledb-event-streamer/testsupport"
	"testing"
)

type kafkaConsumer struct {
	t         *testing.T
	ready     chan bool
	collected chan bool
	envelopes []inttest.Envelope
}

func NewKafkaConsumer(
	t *testing.T,
) (*kafkaConsumer, <-chan bool) {

	kc := &kafkaConsumer{
		t:         t,
		ready:     make(chan bool, 1),
		collected: make(chan bool, 1),
		envelopes: make([]inttest.Envelope, 0),
	}
	return kc, kc.ready
}

func (k *kafkaConsumer) Envelopes() []inttest.Envelope {
	return k.envelopes
}

func (k *kafkaConsumer) Collected() <-chan bool {
	return k.collected
}

func (k *kafkaConsumer) Setup(
	_ sarama.ConsumerGroupSession,
) error {

	return nil
}

func (k *kafkaConsumer) Cleanup(
	_ sarama.ConsumerGroupSession,
) error {

	return nil
}

func (k *kafkaConsumer) ConsumeClaim(
	session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim,
) error {

	kafkaLogger, err := logging.NewLogger("Test_Kafka_Sink")
	if err != nil {
		return err
	}

	k.ready <- true
	for {
		select {
		case message := <-claim.Messages():
			envelope := inttest.Envelope{}
			if err := json.Unmarshal(message.Value, &envelope); err != nil {
				k.t.Error(err)
			}
			kafkaLogger.Infof("EVENT: %+v", envelope)
			k.envelopes = append(k.envelopes, envelope)
			if len(k.envelopes) >= 10 {
				k.collected <- true
			}
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
