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

package eventfiltering

import (
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema/schemamodel"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEventFilter_Evaluate(t *testing.T) {
	filterDefinitions := map[string]spiconfig.EventFilterConfig{
		"test": {
			Condition: "value.op == \"c\"",
		},
	}

	filter, err := NewEventFilter(filterDefinitions)
	if err != nil {
		t.FailNow()
	}

	success, err := filter.Evaluate(nil, schemamodel.Struct{
		schemamodel.FieldNamePayload: schemamodel.Struct{},
		schemamodel.FieldNameSchema:  schemamodel.Struct{},
	}, schemamodel.Struct{
		schemamodel.FieldNamePayload: schemamodel.Struct{
			schemamodel.FieldNameOperation: string(schema.OP_CREATE),
		},
		schemamodel.FieldNameSchema: schemamodel.Struct{},
	})

	if err != nil {
		t.FailNow()
	}

	assert.True(t, success)
}
