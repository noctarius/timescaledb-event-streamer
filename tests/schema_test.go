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

func TestSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(SchemaTestSuite))
}

func (sts *SchemaTestSuite) Test_Source_Schema() {

}

func extractSchemaField(raw schema.Struct, schemaFieldName string) schema.Struct {
	rawSchema := raw["schema"].(map[string]any)
	rawFields := rawSchema["fields"].([]map[string]any)
	for _, rawField := range rawFields {
		if rawField["name"].(string) == schemaFieldName {
			return rawField
		}
	}
	return nil
}
