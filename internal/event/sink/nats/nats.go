package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/noctarius/event-stream-prototype/internal/configuring"
	"github.com/noctarius/event-stream-prototype/internal/event/sink"
	"github.com/noctarius/event-stream-prototype/internal/schema"
	"time"
)

type natsSink struct {
	jetStreamContext nats.JetStreamContext
}

func NewNatsSink(config *configuring.Config) (sink.Sink, error) {
	address := configuring.GetOrDefault(config, "sink.nats.address", "nats://localhost:4222")
	authorization := configuring.GetOrDefault(config, "sink.nats.authorization", "userinfo")
	switch configuring.NatsAuthorizationType(authorization) {
	case configuring.UserInfo:
		username := configuring.GetOrDefault(config, "sink.nats.userinfo.username", "")
		password := configuring.GetOrDefault(config, "sink.nats.userinfo.password", "")
		return newNatsSinkWithUserInfo(address, username, password)
	case configuring.Credentials:
		certificate := configuring.GetOrDefault(config, "sink.nats.credentials.certificate", "")
		seeds := configuring.GetOrDefault(config, "sink.nats.credentials.seeds", []string{})
		return newNatsSinkWithUserCredentials(address, certificate, seeds...)
	case configuring.Jwt:
		jwt := configuring.GetOrDefault(config, "sink.nats.jwt.jwt", "")
		seed := configuring.GetOrDefault(config, "sink.nats.jwt.seed", "")
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

func (n *natsSink) Emit(_ time.Time, topicName string, key, envelope schema.Struct) error {
	keyData, err := json.Marshal(key)
	if err != nil {
		return err
	}
	envelopeData, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	header := nats.Header{}
	header.Add("key", string(keyData))

	_, err = n.jetStreamContext.PublishMsg(
		&nats.Msg{
			Subject: topicName,
			Header:  header,
			Data:    envelopeData,
		},
		nats.Context(context.Background()),
	)
	return err
}
