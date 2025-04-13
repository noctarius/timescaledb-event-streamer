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

package nats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	sinkimpl "github.com/noctarius/timescaledb-event-streamer/internal/eventing/sink"
	config "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/encoding"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
)

type (
	natsSink struct {
		client  *nats.Conn
		encoder *encoding.JsonEncoder
	}

	jetStreamNatsSink struct {
		natsSink
		jetStreamContext nats.JetStreamContext
		publishTimeout   int
	}
)

func init() {
	sinkimpl.RegisterSink(config.NATS, newNatsSink)
}

func newNatsSink(
	c *config.Config,
) (sink.Sink, error) {
	address := config.GetOrDefault(c, config.PropertyNatsAddress, "nats://localhost:4222")
	authorization := config.GetOrDefault(c, config.PropertyNatsAuthorization, "userinfo")
	switch config.NatsAuthorizationType(authorization) {
	case config.UserInfo:
		username := config.GetOrDefault(c, config.PropertyNatsUserinfoUsername, "")
		password := config.GetOrDefault(c, config.PropertyNatsUserinfoPassword, "")
		return newNatsSinkWithUserInfo(c, address, username, password)
	case config.Credentials:
		certificate := config.GetOrDefault(c, config.PropertyNatsCredentialsCertificate, "")
		seeds := config.GetOrDefault(c, config.PropertyNatsCredentialsSeeds, []string{})
		return newNatsSinkWithUserCredentials(c, address, certificate, seeds...)
	case config.Jwt:
		jwt := config.GetOrDefault(c, config.PropertyNatsJwt, "")
		seed := config.GetOrDefault(c, config.PropertyNatsJwtSeed, "")
		return newNatsSinkWithUserJWT(c, address, jwt, seed)
	}
	return nil, fmt.Errorf("NATS AuthorizationType '%s' doesn't exist", authorization)
}

func newNatsSinkWithUserInfo(
	c *config.Config, address, user, password string,
) (sink.Sink, error) {
	return connectStreamContext(c, address, nats.UserInfo(user, password))
}

func newNatsSinkWithUserCredentials(
	c *config.Config, address, userOrChainedFile string, seedFiles ...string,
) (sink.Sink, error) {
	return connectStreamContext(c, address, nats.UserCredentials(userOrChainedFile, seedFiles...))
}

func newNatsSinkWithUserJWT(
	c *config.Config, address, jwt, seed string,
) (sink.Sink, error) {
	return connectStreamContext(c, address, nats.UserJWTAndSeed(jwt, seed))
}

func connectStreamContext(
	c *config.Config, address string, options ...nats.Option,
) (sink.Sink, error) {
	reconnectWaitTimeout := config.GetOrDefault(c, config.PropertyNatsTimeoutReconnectWait, 2)
	dialTimeout := config.GetOrDefault(c, config.PropertyNatsTimeoutDialTimeout, 2)
	options = append(
		options,
		nats.Name("event-stream-prototype"),
		nats.RetryOnFailedConnect(true),
		nats.ReconnectWait(time.Second*time.Duration(reconnectWaitTimeout)),
		nats.Timeout(time.Second*time.Duration(dialTimeout)),
		nats.ReconnectBufSize(1024*1024),
		nats.MaxReconnects(-1),
	)

	client, err := nats.Connect(address, options...)
	if err != nil {
		return nil, err
	}

	sink := natsSink{
		client:  client,
		encoder: encoding.NewJsonEncoderWithConfig(c),
	}

	mode := strings.ToLower(config.GetOrDefault(c, config.PropertyNatsMode, "jetstream"))
	if mode == "jetstream" {
		jetStreamContext, err := client.JetStream()
		if err != nil {
			return nil, err
		}
		publishTimeout := config.GetOrDefault(c, config.PropertyNatsTimeoutPublishTimeout, 2)
		return &jetStreamNatsSink{
			publishTimeout:   publishTimeout,
			natsSink:         sink,
			jetStreamContext: jetStreamContext,
		}, nil
	}

	return &sink, nil
}

func (n *natsSink) Start() error {
	return nil
}

func (n *natsSink) Stop() error {
	n.client.Close()
	return nil
}

func (n *natsSink) getMsg(topicName string, key, envelope schema.Struct) (*nats.Msg, error) {
	envelopeData, err := n.encoder.Marshal(envelope)
	if err != nil {
		return nil, err
	}

	keyData, err := n.encoder.Marshal(key)
	if err != nil {
		return nil, err
	}

	return &nats.Msg{
		Subject: topicName,
		Data:    envelopeData,
		Header: nats.Header{
			"key": []string{string(keyData)},
		},
	}, nil
}

func (n *natsSink) Emit(
	_ sink.Context, _ time.Time, topicName string, key, envelope schema.Struct,
) error {
	msg, err := n.getMsg(topicName, key, envelope)
	if err != nil {
		return err
	}
	err = n.client.PublishMsg(msg)
	return err
}

func (n *jetStreamNatsSink) Emit(
	_ sink.Context, _ time.Time, topicName string, key, envelope schema.Struct,
) error {
	msg, err := n.getMsg(topicName, key, envelope)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(n.publishTimeout)*time.Second)
	defer cancel()

	_, err = n.jetStreamContext.PublishMsg(
		msg,
		nats.Context(ctx),
	)
	return err
}
