package sink

import (
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"time"
)

const (
	sinkContextStateName = "SinkContextState"
)

type sinkManager struct {
	stateStorageManager statestorage.Manager
	sinkContext         *sinkContext
	sink                sink.Sink
}

func NewSinkManager(
	stateStorageManager statestorage.Manager, sink sink.Sink,
) sink.Manager {

	return &sinkManager{
		stateStorageManager: stateStorageManager,
		sinkContext:         newSinkContext(),
		sink:                sink,
	}
}

func (sm *sinkManager) Start() error {
	if encodedSinkContextState, present := sm.stateStorageManager.EncodedState(sinkContextStateName); present {
		return sm.sinkContext.UnmarshalBinary(encodedSinkContextState)
	}
	return sm.sink.Start()
}

func (sm *sinkManager) Stop() error {
	if err := sm.stateStorageManager.StateEncoder(
		sinkContextStateName, statestorage.StateEncoderFunc(sm.sinkContext.MarshalBinary),
	); err != nil {
		return errors.Wrap(err, 0)
	}
	return sm.sink.Stop()
}

func (sm *sinkManager) Emit(
	timestamp time.Time, topicName string, key, envelope schema.Struct,
) error {

	return sm.sink.Emit(sm.sinkContext, timestamp, topicName, key, envelope)
}
