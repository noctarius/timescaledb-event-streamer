package snapshotting

import (
	"github.com/go-errors/errors"
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
)

const stateContextName = "snapshotContext"

func GetSnapshotContext(replicationContext *context.ReplicationContext) (*SnapshotContext, error) {
	snapshotContext := &SnapshotContext{}
	present, err := replicationContext.StateDecoder(stateContextName, snapshotContext)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}
	if !present {
		return nil, nil
	}
	return snapshotContext, nil
}

func SnapshotContextTransaction(replicationContext *context.ReplicationContext, snapshotName string,
	createIfNotExists bool, transaction func(snapshotContext *SnapshotContext) error) error {

	retrieval := func() (*SnapshotContext, error) {
		return GetSnapshotContext(replicationContext)
	}

	if createIfNotExists {
		retrieval = func() (*SnapshotContext, error) {
			return getOrCreateSnapshotContext(replicationContext, snapshotName)
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

	if err := SetSnapshotContext(replicationContext, snapshotContext); err != nil {
		return err
	}
	return nil
}

func SetSnapshotContext(replicationContext *context.ReplicationContext, snapshotContext *SnapshotContext) error {
	return replicationContext.StateEncoder(stateContextName, snapshotContext)
}

func getOrCreateSnapshotContext(
	replicationContext *context.ReplicationContext, snapshotName string) (*SnapshotContext, error) {

	snapshotContext, err := GetSnapshotContext(replicationContext)
	if err != nil {
		return nil, err
	}

	// Exists -> done
	if snapshotContext != nil {
		return snapshotContext, nil
	}

	// New snapshot context
	snapshotContext = &SnapshotContext{
		snapshotName: snapshotName,
		complete:     false,
		watermarks:   make(map[string]*Watermark),
	}

	// Register new snapshot context
	if err := SetSnapshotContext(replicationContext, snapshotContext); err != nil {
		return nil, err
	}

	return snapshotContext, nil
}
