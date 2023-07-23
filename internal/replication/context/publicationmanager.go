package context

import "github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"

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

type publicationManager struct {
	replicationContext *replicationContext
}

func (pm *publicationManager) PublicationName() string {
	return pm.replicationContext.publicationName
}

func (pm *publicationManager) PublicationCreate() bool {
	return pm.replicationContext.publicationCreate
}

func (pm *publicationManager) PublicationAutoDrop() bool {
	return pm.replicationContext.publicationAutoDrop
}

func (pm *publicationManager) ExistsTableInPublication(entity systemcatalog.SystemEntity) (found bool, err error) {
	return pm.replicationContext.sideChannel.ExistsTableInPublication(
		pm.PublicationName(), entity.SchemaName(), entity.TableName(),
	)
}

func (pm *publicationManager) AttachTablesToPublication(entities ...systemcatalog.SystemEntity) error {
	return pm.replicationContext.sideChannel.AttachTablesToPublication(pm.PublicationName(), entities...)
}

func (pm *publicationManager) DetachTablesFromPublication(entities ...systemcatalog.SystemEntity) error {
	return pm.replicationContext.sideChannel.DetachTablesFromPublication(pm.PublicationName(), entities...)
}

func (pm *publicationManager) ReadPublishedTables() ([]systemcatalog.SystemEntity, error) {
	return pm.replicationContext.sideChannel.ReadPublishedTables(pm.PublicationName())
}

func (pm *publicationManager) CreatePublication() (bool, error) {
	return pm.replicationContext.sideChannel.CreatePublication(pm.PublicationName())
}

func (pm *publicationManager) ExistsPublication() (bool, error) {
	return pm.replicationContext.sideChannel.ExistsPublication(pm.PublicationName())
}

func (pm *publicationManager) DropPublication() error {
	return pm.replicationContext.sideChannel.DropPublication(pm.PublicationName())
}
