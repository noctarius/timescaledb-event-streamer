package context

import (
	"encoding"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/noctarius/timescaledb-event-streamer/spi/watermark"
)

type PublicationManager interface {
	PublicationName() string
	PublicationCreate() bool
	PublicationAutoDrop() bool
	CreatePublication() (bool, error)
	ExistsPublication() (bool, error)
	DropPublication() error
	ReadPublishedTables() ([]systemcatalog.SystemEntity, error)
	ExistsTableInPublication(entity systemcatalog.SystemEntity) (found bool, err error)
	AttachTablesToPublication(entities ...systemcatalog.SystemEntity) error
	DetachTablesFromPublication(entities ...systemcatalog.SystemEntity) error
}

type StateManager interface {
	StateEncoder(name string, encoder encoding.BinaryMarshaler) error
	StateDecoder(name string, decoder encoding.BinaryUnmarshaler) (present bool, err error)
	SetEncodedState(name string, encodedState []byte)
	EncodedState(name string) (encodedState []byte, present bool)
	SnapshotContext() (*watermark.SnapshotContext, error)
	SnapshotContextTransaction(snapshotName string, createIfNotExists bool,
		transaction func(snapshotContext *watermark.SnapshotContext) error) error
}
