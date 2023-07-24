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

package stdout

import (
	"encoding/json"
	"fmt"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"os"
	"time"
)

func init() {
	sink.RegisterSink(spiconfig.Stdout, newStdoutSink)
}

func newStdoutSink(
	_ *spiconfig.Config,
) (sink.Sink, error) {

	return sink.SinkFunc(
		func(
			_ sink.Context, _ time.Time, _ string, _, envelope schema.Struct,
		) error {

			delete(envelope, "schema")
			data, err := json.Marshal(envelope)
			if err != nil {
				return err
			}
			_, err = os.Stdout.WriteString(fmt.Sprintf("%s\n", string(data)))
			return err
		},
	), nil
}
