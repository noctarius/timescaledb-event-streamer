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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/noctarius/timescaledb-event-streamer/internal/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/testsupport"
	"github.com/noctarius/timescaledb-event-streamer/testsupport/testrunner"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"net"
	"net/http"
	"testing"
	"time"
)

type HttpIntegrationTestSuite struct {
	testrunner.TestRunner
}

func TestHttpIntegrationTestSuite(
	t *testing.T,
) {

	suite.Run(t, new(HttpIntegrationTestSuite))
}

func (hits *HttpIntegrationTestSuite) Test_Http_Sink() {
	topicPrefix := lo.RandomString(10, lo.LowerCaseLettersCharset)

	httpLogger, err := logging.NewLogger("Test_Http_Sink")
	if err != nil {
		hits.T().Error(err)
	}

	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		hits.T().Error(err)
	}

	server := &http.Server{}

	hits.RunTest(
		func(ctx testrunner.Context) error {
			collected := make(chan bool, 1)
			envelopes := make([]testsupport.Envelope, 0)

			http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
				decoder := json.NewDecoder(req.Body)
				envelope := testsupport.Envelope{}
				err := decoder.Decode(&envelope)
				if err != nil {
					httpLogger.Fatalf("Could not deserialize envelope!")
				}
				httpLogger.Debugf("EVENT: %+v", envelope)
				envelopes = append(envelopes, envelope)
				if len(envelopes) >= 10 {
					collected <- true
				}
			})

			go func() {
				if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
					httpLogger.Fatalf("Unable to listen and serve on %s", listener.Addr())
					hits.T().Error(err)
				}
			}()

			if _, err := ctx.Exec(context.Background(),
				fmt.Sprintf(
					"INSERT INTO \"%s\" SELECT ts, ROW_NUMBER() OVER (ORDER BY ts) AS val FROM GENERATE_SERIES('2023-03-25 00:00:00'::TIMESTAMPTZ, '2023-03-25 00:09:59'::TIMESTAMPTZ, INTERVAL '1 minute') t(ts)",
					testrunner.GetAttribute[string](ctx, "tableName"),
				),
			); err != nil {
				return err
			}

			<-collected

			for i, envelope := range envelopes {
				assert.Equal(hits.T(), i+1, int(envelope.Payload.After["val"].(float64)))
			}
			return nil
		},

		testrunner.WithSetup(func(setupContext testrunner.SetupContext) error {
			sn, tn, err := setupContext.CreateHypertable("ts", time.Hour*24,
				testsupport.NewColumn("ts", "timestamptz", false, true, nil),
				testsupport.NewColumn("val", "integer", false, false, nil),
			)
			if err != nil {
				return err
			}
			testrunner.Attribute(setupContext, "schemaName", sn)
			testrunner.Attribute(setupContext, "tableName", tn)

			setupContext.AddSystemConfigConfigurator(func(config *sysconfig.SystemConfig) {
				config.Topic.Prefix = topicPrefix
				config.Sink.Type = spiconfig.Http
				config.Sink.Http = spiconfig.HttpConfig{
					Url: fmt.Sprintf("http://%s/", listener.Addr()),
				}
			})

			return nil
		}),

		testrunner.WithTearDown(func(ctx testrunner.Context) error {
			return server.Shutdown(context.Background())
		}),
	)
}
