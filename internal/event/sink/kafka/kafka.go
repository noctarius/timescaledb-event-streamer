package kafka

import (
	"crypto/tls"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/noctarius/event-stream-prototype/internal/configuring"
	"github.com/noctarius/event-stream-prototype/internal/event/sink"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"time"
)

type kafkaSink struct {
	producer sarama.SyncProducer
}

func NewKafkaSink(config *configuring.Config) (sink.Sink, error) {
	c := sarama.NewConfig()
	c.ClientID = "event-stream-prototype"
	c.Producer.Idempotent = configuring.GetOrDefault(
		config, "sink.kafka.idempotent", true,
	)
	c.Producer.Return.Successes = true
	c.Producer.RequiredAcks = sarama.WaitForLocal
	c.Producer.Partitioner = sarama.NewHashPartitioner

	if configuring.GetOrDefault(config, "sink.kafka.sasl.enabled", false) {
		c.Net.SASL.Enable = true
		c.Net.SASL.User = configuring.GetOrDefault(
			config, "sink.kafka.sasl.user", "",
		)
		c.Net.SASL.Password = configuring.GetOrDefault(
			config, "sink.kafka.sasl.password", "",
		)
		c.Net.SASL.Mechanism = configuring.GetOrDefault(
			config, "sink.kafka.sasl.mechanism", sarama.SASLMechanism(sarama.SASLTypePlaintext),
		)
	}

	if configuring.GetOrDefault(config, "sink.kafka.tls.enabled", false) {
		c.Net.TLS.Enable = true
		c.Net.TLS.Config.InsecureSkipVerify = configuring.GetOrDefault(
			config, "sink.kafka.tls.skipverify", false,
		)
		c.Net.TLS.Config.ClientAuth = configuring.GetOrDefault(
			config, "sink.kafka.tls.clientauth", tls.NoClientCert,
		)
	}

	producer, err := sarama.NewSyncProducer(
		configuring.GetOrDefault(config, "sink.kafka.brokers", []string{"localhost:9092"}), c,
	)
	if err != nil {
		return nil, err
	}

	return &kafkaSink{
		producer: producer,
	}, nil
}

func (k *kafkaSink) Emit(timestamp time.Time, topicName string, envelope schema.Struct) error {
	data, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic:     topicName,
		Key:       sarama.StringEncoder(topicName),
		Value:     sarama.ByteEncoder(data),
		Timestamp: timestamp,
	}

	_, _, err = k.producer.SendMessage(msg)
	return err
}
