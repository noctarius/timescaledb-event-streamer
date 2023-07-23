package stream

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/schema"
	"github.com/noctarius/timescaledb-event-streamer/spi/sink"
	"sync"
)

const (
	messageStreamName = "::internal::message::stream::"
)

type Provider = func(
	nameGenerator schema.NameGenerator, typeManager pgtypes.TypeManager, sinkManager sink.Manager,
) (Manager, error)

type Manager interface {
	Start() error
	Stop() error
	GetStream(table schema.TableAlike) (stream Stream, present bool)
	GetOrCreateStream(table schema.TableAlike) Stream
}

type streamManager struct {
	nameGenerator schema.NameGenerator
	typeManager   pgtypes.TypeManager
	sinkManager   sink.Manager

	streamsMutex sync.Mutex
	streams      map[string]Stream
}

func NewStreamManager(
	nameGenerator schema.NameGenerator, typeManager pgtypes.TypeManager, sinkManager sink.Manager,
) (Manager, error) {
	return &streamManager{
		nameGenerator: nameGenerator,
		typeManager:   typeManager,
		sinkManager:   sinkManager,

		streamsMutex: sync.Mutex{},
		streams:      make(map[string]Stream),
	}, nil
}

func (s *streamManager) Start() error {
	return s.sinkManager.Start()
}

func (s *streamManager) Stop() error {
	return s.sinkManager.Stop()
}

func (s *streamManager) GetStream(table schema.TableAlike) (stream Stream, present bool) {
	s.streamsMutex.Lock()
	defer s.streamsMutex.Unlock()
	return s.getStream(table)
}

func (s *streamManager) GetOrCreateStream(table schema.TableAlike) Stream {
	s.streamsMutex.Lock()
	defer s.streamsMutex.Unlock()
	stream, present := s.getStream(table)
	if present {
		return stream
	}
	return s.createStream(table)
}

func (s *streamManager) getStream(table schema.TableAlike) (stream Stream, present bool) {
	streamName := messageStreamName
	if table != nil {
		streamName = table.CanonicalName()
	}
	stream, present = s.streams[streamName]
	return stream, present
}

func (s *streamManager) createStream(table schema.TableAlike) Stream {
	stream := Stream(nil)
	streamName := messageStreamName
	if table == nil {
		stream = NewMessageStream(s.nameGenerator, s.sinkManager)
	} else {
		stream = NewTableStream(s.nameGenerator, s.typeManager, s.sinkManager, table)
		streamName = table.CanonicalName()
	}
	s.streams[streamName] = stream
	return stream
}
