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
	"github.com/nats-io/nats.go"
	sinkimpl "github.com/noctarius/timescaledb-event-streamer/internal/eventing/sink"
	config "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/encoding"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"time"
)

func init() {
	sinkimpl.RegisterSink(config.NATS, newNatsSink)
}

type natsSink struct {
	client           *nats.Conn
	jetStreamContext nats.JetStreamContext
	encoder          *encoding.JsonEncoder
	timeout          time.Duration
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

	return connectJetStreamContext(c, address, nats.UserInfo(user, password))
}

func newNatsSinkWithUserCredentials(
	c *config.Config, address, userOrChainedFile string, seedFiles ...string,
) (sink.Sink, error) {

	return connectJetStreamContext(c, address, nats.UserCredentials(userOrChainedFile, seedFiles...))
}

func newNatsSinkWithUserJWT(
	c *config.Config, address, jwt, seed string,
) (sink.Sink, error) {

	return connectJetStreamContext(c, address, nats.UserJWTAndSeed(jwt, seed))
}

func connectJetStreamContext(
	c *config.Config, address string, options ...nats.Option,
) (sink.Sink, error) {

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

	timeout := time.Second * 5
	if c.Sink.Nats.Timeout != 0 {
		timeout = time.Second * time.Duration(c.Sink.Nats.Timeout)
	}

	return &natsSink{
		client:           client,
		jetStreamContext: jetStreamContext,
		encoder:          encoding.NewJsonEncoderWithConfig(c),
		timeout:          timeout,
	}, nil
}

func (n *natsSink) Start() error {
	return nil
}

func (n *natsSink) Stop() error {
	n.client.Close()
	return nil
}

func (n *natsSink) Emit(
	_ sink.Context, _ time.Time, topicName string, key, envelope schema.Struct,
) error {

	keyData, err := n.encoder.Marshal(key)
	if err != nil {
		return err
	}
	envelopeData, err := n.encoder.Marshal(envelope)
	if err != nil {
		return err
	}

	header := nats.Header{}
	header.Add("key", string(keyData))

	ctx, cancel := context.WithTimeout(context.Background(), n.timeout)
	defer cancel()

	_, err = n.jetStreamContext.PublishMsg(
		&nats.Msg{
			Subject: topicName,
			Header:  header,
			Data:    envelopeData,
		},
		nats.Context(ctx),
	)
	return err
}
