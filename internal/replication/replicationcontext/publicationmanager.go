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

package replicationcontext

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

func (pm *publicationManager) ExistsTableInPublication(
	entity systemcatalog.SystemEntity,
) (found bool, err error) {

	return pm.replicationContext.sideChannel.ExistsTableInPublication(
		pm.PublicationName(), entity.SchemaName(), entity.TableName(),
	)
}

func (pm *publicationManager) AttachTablesToPublication(
	entities ...systemcatalog.SystemEntity,
) error {

	return pm.replicationContext.sideChannel.AttachTablesToPublication(pm.PublicationName(), entities...)
}

func (pm *publicationManager) DetachTablesFromPublication(
	entities ...systemcatalog.SystemEntity,
) error {

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
