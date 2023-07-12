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
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema/schemamodel"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"time"
)

func init() {
	sink.RegisterSink(spiconfig.NATS, newNatsSink)
}

type natsSink struct {
	client           *nats.Conn
	jetStreamContext nats.JetStreamContext
}

func newNatsSink(config *spiconfig.Config) (sink.Sink, error) {
	address := spiconfig.GetOrDefault(config, spiconfig.PropertyNatsAddress, "nats://localhost:4222")
	authorization := spiconfig.GetOrDefault(config, spiconfig.PropertyNatsAuthorization, "userinfo")
	switch spiconfig.NatsAuthorizationType(authorization) {
	case spiconfig.UserInfo:
		username := spiconfig.GetOrDefault(config, spiconfig.PropertyNatsUserinfoUsername, "")
		password := spiconfig.GetOrDefault(config, spiconfig.PropertyNatsUserinfoPassword, "")
		return newNatsSinkWithUserInfo(address, username, password)
	case spiconfig.Credentials:
		certificate := spiconfig.GetOrDefault(config, spiconfig.PropertyNatsCredentialsCertificate, "")
		seeds := spiconfig.GetOrDefault(config, spiconfig.PropertyNatsCredentialsSeeds, []string{})
		return newNatsSinkWithUserCredentials(address, certificate, seeds...)
	case spiconfig.Jwt:
		jwt := spiconfig.GetOrDefault(config, spiconfig.PropertyNatsJwt, "")
		seed := spiconfig.GetOrDefault(config, spiconfig.PropertyNatsJwtSeed, "")
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
		client:           client,
		jetStreamContext: jetStreamContext,
	}, nil
}

func (n *natsSink) Start() error {
	return nil
}

func (n *natsSink) Stop() error {
	n.client.Close()
	return nil
}

func (n *natsSink) Emit(_ sink.Context, _ time.Time, topicName string, key, envelope schemamodel.Struct) error {
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
