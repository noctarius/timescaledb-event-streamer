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

package http

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	sinkimpl "github.com/noctarius/timescaledb-event-streamer/internal/eventing/sink"
	config "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/encoding"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"net/http"
	"time"
)

func init() {
	sinkimpl.RegisterSink(config.Http, newHttpSink)
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

type httpSink struct {
	client  *http.Client
	encoder *encoding.JsonEncoder
	address *string
	headers *http.Header
}

func newHttpSink(
	c *config.Config,
) (sink.Sink, error) {

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if config.GetOrDefault(c, config.PropertyHttpTlsEnabled, false) {
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: config.GetOrDefault(
				c, config.PropertyHttpTlsSkipVerify, false,
			),
			ClientAuth: config.GetOrDefault(
				c, config.PropertyHttpTlsClientAuth, tls.NoClientCert,
			),
		}
	}

	address := config.GetOrDefault(c, config.PropertyHttpUrl, "http://localhost:80")
	headers := make(http.Header)

	authenticationType := config.GetOrDefault(c, config.PropertyHttpAuthenticationType, "none")
	switch config.HttpAuthenticationType(authenticationType) {
	case config.BasicAuthentication:
		{
			headers.Add("Authorization",
				fmt.Sprintf("Basic %s",
					basicAuth(config.GetOrDefault(c, config.PropertyHttpBasicAuthenticationUsername, ""),
						config.GetOrDefault(c, config.PropertyHttpBasicAuthenticationPassword, ""),
					),
				),
			)
		}
	case config.HeaderAuthentication:
		{
			headers.Add(config.GetOrDefault(c, config.PropertyHttpHeaderAuthenticationHeaderName, ""),
				config.GetOrDefault(c, config.PropertyHttpHeaderAuthenticationHeaderValue, ""),
			)
		}
	case config.NoneAuthentication:
		{
		}
	default:
		{
			return nil, fmt.Errorf("http AuthenticationType '%s' doesn't exist", authenticationType)
		}
	}

	return &httpSink{
		client:  &http.Client{Transport: transport},
		encoder: encoding.NewJsonEncoderWithConfig(c),
		address: &address,
		headers: &headers,
	}, nil
}

func (h *httpSink) Start() error {
	return nil
}

func (h *httpSink) Stop() error {
	h.client.CloseIdleConnections()
	return nil
}

func (h *httpSink) Emit(
	_ sink.Context, _ time.Time, topicName string, key, envelope schema.Struct,
) error {
	delete(envelope, "schema")
	payload, err := h.encoder.Marshal(envelope)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", *h.address, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}

	req.Header = *h.headers
	_, err = h.client.Do(req)
	return err
}
