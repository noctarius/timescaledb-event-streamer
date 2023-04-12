package eventfiltering

import (
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEventFilter_Evaluate(t *testing.T) {
	filterDefinitions := map[string]spiconfig.EventFilterConfig{
		"test": {
			Condition: "value.op == \"c\"",
		},
	}

	filter, err := NewSinkEventFilter(filterDefinitions)
	if err != nil {
		t.FailNow()
	}

	success, err := filter.Evaluate(nil, schema.Struct{
		"payload": schema.Struct{},
		"schema":  schema.Struct{},
	}, schema.Struct{
		"payload": schema.Struct{
			"op": string(schema.OP_CREATE),
		},
		"schema": schema.Struct{},
	})

	if err != nil {
		t.FailNow()
	}

	assert.True(t, success)
}
