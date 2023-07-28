package publication

import "github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"

type PublicationManager interface {
	PublicationName() string
	PublicationCreate() bool
	PublicationAutoDrop() bool
	CreatePublication() (bool, error)
	ExistsPublication() (bool, error)
	DropPublication() error
	ReadPublishedTables() (
		[]systemcatalog.SystemEntity, error,
	)
	ExistsTableInPublication(
		entity systemcatalog.SystemEntity,
	) (found bool, err error)
	AttachTablesToPublication(
		entities ...systemcatalog.SystemEntity,
	) error
	DetachTablesFromPublication(
		entities ...systemcatalog.SystemEntity,
	) error
}
