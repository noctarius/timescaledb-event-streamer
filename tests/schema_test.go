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

package tests

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/testsupport/testrunner"
	"github.com/stretchr/testify/suite"
	"testing"
)

var schemaTestTable = []SchemaTestCase{
	{
		name:       "Basic Type",
		pgTypeName: "boolean",
		value:      true,
		schemaFactory: func(config config.Config) schema.Struct {
			return nil
		},
	},
}

type SchemaTestCase struct {
	name          string
	pgTypeName    string
	value         any
	schemaFactory func(config config.Config) schema.Struct
}

type SchemaTestSuite struct {
	testrunner.TestRunner
}

func TestSchemaTestSuite(
	t *testing.T,
) {

	suite.Run(t, new(SchemaTestSuite))
}

func (sts *SchemaTestSuite) Test_Source_Schema() {

}

func extractSchemaField(
	raw schema.Struct, schemaFieldName string,
) schema.Struct {

	rawSchema := raw["schema"].(map[string]any)
	rawFields := rawSchema["fields"].([]map[string]any)
	for _, rawField := range rawFields {
		if rawField["name"].(string) == schemaFieldName {
			return rawField
		}
	}
	return nil
}
