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
	address := configuration.GetOrDefault(config, "sink.nats.address", "nats://localhost:4222")
	authorization := configuration.GetOrDefault(config, "sink.nats.authorization", "userinfo")
	switch configuration.NatsAuthorizationType(authorization) {
	case configuration.UserInfo:
		username := configuration.GetOrDefault(config, "sink.nats.userinfo.username", "")
		password := configuration.GetOrDefault(config, "sink.nats.userinfo.password", "")
		return newNatsSinkWithUserInfo(address, username, password)
	case configuration.Credentials:
		certificate := configuration.GetOrDefault(config, "sink.nats.credentials.certificate", "")
		seeds := configuration.GetOrDefault(config, "sink.nats.credentials.seeds", []string{})
		return newNatsSinkWithUserCredentials(address, certificate, seeds...)
	case configuration.Jwt:
		jwt := configuration.GetOrDefault(config, "sink.nats.jwt.jwt", "")
		seed := configuration.GetOrDefault(config, "sink.nats.jwt.seed", "")
		return newNatsSinkWithUserJWT(address, jwt, seed)
	}
	return nil, fmt.Errorf("NATS AuthorizationType '%s' doesn't exist", authorization)
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

func (n *natsSink) Emit(_ time.Time, topicName string, envelope schema.Struct) error {
	data, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	_, err = n.jetStreamContext.Publish(topicName, data, nats.Context(context.Background()))
	return err
}
