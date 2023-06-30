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

package logicalreplicationresolver

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/replication/context"
	"github.com/noctarius/timescaledb-event-streamer/internal/systemcatalog"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/eventhandlers"
	"time"
)

func NewResolver(config *spiconfig.Config, replicationContext *context.ReplicationContext,
	systemCatalog *systemcatalog.SystemCatalog) (eventhandlers.BaseReplicationEventHandler, error) {

	enabled := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlTxwindowEnabled, true,
	)
	timeout := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlTxwindowTimeout, time.Duration(60),
	) * time.Second
	maxSize := spiconfig.GetOrDefault(
		config, spiconfig.PropertyPostgresqlTxwindowMaxsize, uint(10000),
	)

	resolver, err := newLogicalReplicationResolver(config, replicationContext, systemCatalog)
	if err != nil {
		return nil, err
	}

	if enabled && maxSize > 0 {
		return newTransactionTracker(timeout, maxSize, replicationContext, systemCatalog, resolver)
	}
	return resolver, nil
}
