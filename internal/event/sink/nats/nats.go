package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/noctarius/event-stream-prototype/internal/configuration"
	"github.com/noctarius/event-stream-prototype/internal/event/sink"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"time"
)

type natsSink struct {
	jetStreamContext nats.JetStreamContext
}

func NewNatsSink(config *configuration.Config) (sink.Sink, error) {
	natsConfig := config.Sink.Nats
	switch natsConfig.Authorization {
	case configuration.UserInfo:
		return newNatsSinkWithUserInfo(
			natsConfig.Address, natsConfig.UserInfo.Username, natsConfig.UserInfo.Password)
	case configuration.Credentials:
		return newNatsSinkWithUserCredentials(
			natsConfig.Address, natsConfig.Credentials.Certificate, natsConfig.Credentials.Seeds...)
	case configuration.Jwt:
		return newNatsSinkWithUserJWT(natsConfig.Address, natsConfig.JWT.JWT, natsConfig.JWT.Seed)
	}
	return nil, fmt.Errorf("NATS AuthorizationType '%s' doesn't exist", config.Sink.Nats.Authorization)
}

func newNatsSinkWithUserInfo(address, user, password string) (sink.Sink, error) {
	return connectJetStreamContext(address, nats.UserInfo(user, password))
}

func newNatsSinkWithUserCredentials(address, userOrChainedFile string, seedFiles ...string) (sink.Sink, error) {
	return connectJetStreamContext(address, nats.UserCredentials(userOrChainedFile, seedFiles...))
}

func newNatsSinkWithUserJWT(address, jwt, seed string) (sink.Sink, error) {
	return connectJetStreamContext(address, nats.UserJWTAndSeed(jwt, seed))
}

func connectJetStreamContext(address string, options ...nats.Option) (sink.Sink, error) {
	options = append(
		options,
		nats.Name("event-stream-prototype"),
		nats.RetryOnFailedConnect(true),
		nats.ReconnectWait(time.Second*10),
		nats.ReconnectBufSize(1024*1024),
		nats.MaxReconnects(-1),
	)

	client, err := nats.Connect(address, options...)
	if err != nil {
		return nil, err
	}

	jetStreamContext, err := client.JetStream()
	if err != nil {
		return nil, err
	}

	return &natsSink{
		jetStreamContext: jetStreamContext,
	}, nil
}

func (n *natsSink) Emit(topicName string, envelope schema.Struct) bool {
	data, err := json.Marshal(envelope)
	if err != nil {
		return false
	}

	if _, err := n.jetStreamContext.Publish(topicName, data, nats.Context(context.Background())); err != nil {
		return false
	}
	return true
}
