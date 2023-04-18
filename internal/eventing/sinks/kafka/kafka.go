package kafka

import (
	"crypto/tls"
	"encoding/json"
	"github.com/Shopify/sarama"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"time"
)

func init() {
	sink.RegisterSink(spiconfig.Kafka, newKafkaSink)
}

type kafkaSink struct {
	producer sarama.SyncProducer
}

func newKafkaSink(config *spiconfig.Config) (sink.Sink, error) {
	c := sarama.NewConfig()
	c.ClientID = "event-stream-prototype"
	c.Producer.Idempotent = spiconfig.GetOrDefault(
		config, "sink.kafka.idempotent", false,
	)
	c.Producer.Return.Successes = true
	c.Producer.RequiredAcks = sarama.WaitForLocal
	c.Producer.Retry.Max = 10

	if spiconfig.GetOrDefault(config, spiconfig.PropertyKafkaSaslEnabled, false) {
		c.Net.SASL.Enable = true
		c.Net.SASL.User = spiconfig.GetOrDefault(
			config, spiconfig.PropertyKafkaSaslUser, "",
		)
		c.Net.SASL.Password = spiconfig.GetOrDefault(
			config, spiconfig.PropertyKafkaSaslPassword, "",
		)
		c.Net.SASL.Mechanism = spiconfig.GetOrDefault[sarama.SASLMechanism](
			config, spiconfig.PropertyKafkaSaslMechanism, sarama.SASLTypePlaintext,
		)
	}

	if spiconfig.GetOrDefault(config, spiconfig.PropertyKafkaTlsEnabled, false) {
		c.Net.TLS.Enable = true
		c.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: spiconfig.GetOrDefault(
				config, spiconfig.PropertyKafkaTlsSkipVerify, false,
			),
			ClientAuth: spiconfig.GetOrDefault(
				config, spiconfig.PropertyKafkaTlsClientAuth, tls.NoClientCert,
			),
		}
	}

	producer, err := sarama.NewSyncProducer(
		spiconfig.GetOrDefault(config, spiconfig.PropertyKafkaBrokers, []string{"localhost:9092"}), c,
	)
	if err != nil {
		return nil, err
	}

	return &kafkaSink{
		producer: producer,
	}, nil
}

func (k *kafkaSink) Emit(_ sink.Context, timestamp time.Time, topicName string, key, envelope schema.Struct) error {
	keyData, err := json.Marshal(key)
	if err != nil {
		return err
	}
	envelopeData, err := json.Marshal(envelope)
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
