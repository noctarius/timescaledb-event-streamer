package kafka

import (
	"crypto/tls"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/noctarius/event-stream-prototype/internal/configuration"
	"github.com/noctarius/event-stream-prototype/internal/event/sink"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"time"
)

type kafkaSink struct {
	producer sarama.SyncProducer
}

func NewKafkaSink(config *configuration.Config) (sink.Sink, error) {
	c := sarama.NewConfig()
	c.ClientID = "event-stream-prototype"
	c.Producer.Idempotent = configuration.GetOrDefault(config, "sink.kafka.idempotent", true)
	c.Producer.Return.Successes = true
	c.Producer.RequiredAcks = sarama.WaitForLocal
	c.Producer.Partitioner = sarama.NewHashPartitioner

	if configuration.GetOrDefault(config, "sink.kafka.sasl.enabled", false) {
		c.Net.SASL.Enable = true
		c.Net.SASL.User = configuration.GetOrDefault(config, "sink.kafka.sasl.user", "")
		c.Net.SASL.Password = configuration.GetOrDefault(config, "sink.kafka.sasl.password", "")
		c.Net.SASL.Mechanism = configuration.GetOrDefault(config, "sink.kafka.sasl.mechanism", sarama.SASLMechanism(sarama.SASLTypePlaintext))
	}

	if configuration.GetOrDefault(config, "sink.kafka.tls.enabled", false) {
		c.Net.TLS.Enable = true
		c.Net.TLS.Config.InsecureSkipVerify = configuration.GetOrDefault(config, "sink.kafka.tls.skipverify", false)
		c.Net.TLS.Config.ClientAuth = configuration.GetOrDefault(config, "sink.kafka.tls.clientauth", tls.NoClientCert)
	}

	producer, err := sarama.NewSyncProducer(configuration.GetOrDefault(config, "sink.kafka.brokers", []string{"localhost:9092"}), c)
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
