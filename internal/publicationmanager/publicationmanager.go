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

package publicationmanager

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/publication"
	"github.com/noctarius/timescaledb-event-streamer/spi/sidechannel"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
)

type publicationManager struct {
	sideChannel sidechannel.SideChannel

	publicationName     string
	publicationCreate   bool
	publicationAutoDrop bool
}

func NewPublicationManager(
	c *config.Config, sideChannel sidechannel.SideChannel,
) publication.PublicationManager {

	publicationName := config.GetOrDefault(
		c, config.PropertyPostgresqlPublicationName, "",
	)
	publicationCreate := config.GetOrDefault(
		c, config.PropertyPostgresqlPublicationCreate, true,
	)
	publicationAutoDrop := config.GetOrDefault(
		c, config.PropertyPostgresqlPublicationAutoDrop, true,
	)

	return &publicationManager{
		sideChannel: sideChannel,

		publicationName:     publicationName,
		publicationCreate:   publicationCreate,
		publicationAutoDrop: publicationAutoDrop,
	}
}

func (pm *publicationManager) PublicationName() string {
	return pm.publicationName
}

func (pm *publicationManager) PublicationCreate() bool {
	return pm.publicationCreate
}

func (pm *publicationManager) PublicationAutoDrop() bool {
	return pm.publicationAutoDrop
}

func (pm *publicationManager) ExistsTableInPublication(
	entity systemcatalog.SystemEntity,
) (found bool, err error) {

	return pm.sideChannel.ExistsTableInPublication(
		pm.PublicationName(), entity.SchemaName(), entity.TableName(),
	)
}

func (pm *publicationManager) AttachTablesToPublication(
	entities ...systemcatalog.SystemEntity,
) error {

	return pm.sideChannel.AttachTablesToPublication(pm.PublicationName(), entities...)
}

func (pm *publicationManager) DetachTablesFromPublication(
	entities ...systemcatalog.SystemEntity,
) error {

	return pm.sideChannel.DetachTablesFromPublication(pm.PublicationName(), entities...)
}

func (pm *publicationManager) ReadPublishedTables() ([]systemcatalog.SystemEntity, error) {
	return pm.sideChannel.ReadPublishedTables(pm.PublicationName())
}

func (pm *publicationManager) CreatePublication() (bool, error) {
	return pm.sideChannel.CreatePublication(pm.PublicationName())
}

func (pm *publicationManager) ExistsPublication() (bool, error) {
	return pm.sideChannel.ExistsPublication(pm.PublicationName())
}

func (pm *publicationManager) DropPublication() error {
	return pm.sideChannel.DropPublication(pm.PublicationName())
}
