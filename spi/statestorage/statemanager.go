package statestorage

import (
	"encoding"
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/spi/watermark"
)

const (
	stateContextName = "snapshotContext"
)

type Manager interface {
	Start() error
	Stop() error
	Get() (map[string]*Offset, error)
	Set(
		key string, value *Offset,
	) error
	StateEncoder(
		name string, encoder encoding.BinaryMarshaler,
	) error
	StateDecoder(
		name string, decoder encoding.BinaryUnmarshaler,
	) (present bool, err error)
	SetEncodedState(
		name string, encodedState []byte,
	)
	EncodedState(
		name string,
	) (encodedState []byte, present bool)
	SnapshotContext() (*watermark.SnapshotContext, error)
	SnapshotContextTransaction(
		snapshotName string, createIfNotExists bool, transaction func(snapshotContext *watermark.SnapshotContext) error,
	) error
}

type stateManager struct {
	stateStorage Storage
}

func NewStateStorageManager(
	stateStorage Storage,
) Manager {

	return &stateManager{
		stateStorage: stateStorage,
	}
}

func (sm *stateManager) Start() error {
	return sm.stateStorage.Start()
}

func (sm *stateManager) Stop() error {
	return sm.stateStorage.Stop()
}

func (sm *stateManager) Get() (map[string]*Offset, error) {
	return sm.stateStorage.Get()
}

func (sm *stateManager) Set(
	key string, value *Offset,
) error {

	return sm.stateStorage.Set(key, value)
}

func (sm *stateManager) StateEncoder(
	name string, encoder encoding.BinaryMarshaler,
) error {

	return sm.stateStorage.StateEncoder(name, encoder)
}

func (sm *stateManager) StateDecoder(
	name string, decoder encoding.BinaryUnmarshaler,
) (present bool, err error) {

	return sm.stateStorage.StateDecoder(name, decoder)
}

func (sm *stateManager) SetEncodedState(
	name string, encodedState []byte,
) {

	sm.stateStorage.SetEncodedState(name, encodedState)
}

func (sm *stateManager) EncodedState(
	name string,
) (encodedState []byte, present bool) {

	return sm.stateStorage.EncodedState(name)
}

func (sm *stateManager) SnapshotContext() (*watermark.SnapshotContext, error) {
	snapshotContext := &watermark.SnapshotContext{}
	present, err := sm.StateDecoder(stateContextName, snapshotContext)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}
	if !present {
		return nil, nil
	}
	return snapshotContext, nil
}

func (sm *stateManager) SnapshotContextTransaction(
	snapshotName string, createIfNotExists bool, transaction func(snapshotContext *watermark.SnapshotContext) error,
) error {

	retrieval := func() (*watermark.SnapshotContext, error) {
		return sm.SnapshotContext()
	}

	if createIfNotExists {
		retrieval = func() (*watermark.SnapshotContext, error) {
			return sm.getOrCreateSnapshotContext(snapshotName)
		}
	}

	snapshotContext, err := retrieval()
	if err != nil {
		return err
	}

	if snapshotContext == nil && !createIfNotExists {
		return errors.Errorf("No such snapshot context found")
	}

	if err := transaction(snapshotContext); err != nil {
		return err
	}

	if err := sm.setSnapshotContext(snapshotContext); err != nil {
		return err
	}
	return nil
}

func (sm *stateManager) setSnapshotContext(
	snapshotContext *watermark.SnapshotContext,
) error {

	return sm.StateEncoder(stateContextName, snapshotContext)
}

func (sm *stateManager) getOrCreateSnapshotContext(
	snapshotName string,
) (*watermark.SnapshotContext, error) {

	snapshotContext, err := sm.SnapshotContext()
	if err != nil {
		return nil, err
	}

	// Exists -> done
	if snapshotContext != nil {
		return snapshotContext, nil
	}

	// New snapshot context
	snapshotContext = watermark.NewSnapshotContext(snapshotName)

	// Register new snapshot context
	if err := sm.setSnapshotContext(snapshotContext); err != nil {
		return nil, err
	}

	return snapshotContext, nil
}
