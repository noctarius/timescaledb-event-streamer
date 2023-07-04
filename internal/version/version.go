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

package version

import "github.com/noctarius/timescaledb-event-streamer/spi/version"

const (
	TSDB_MIN_VERSION version.TimescaleVersion = 21000
	TSDB_212_VERSION version.TimescaleVersion = 21200
	PG_MIN_VERSION   version.PostgresVersion  = 130000
	PG_14_VERSION    version.PostgresVersion  = 140000
)

var (
	BinName    = "timescaledb-event-streamer"
	Version    = "0.0.9-dev"
	CommitHash = "unknown"
	Branch     = "unknown"
)

var (
	PostgresqlVersion  version.PostgresVersion
	TimescaleDbVersion version.TimescaleVersion
)
