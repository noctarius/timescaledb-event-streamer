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

import (
	"fmt"
	"github.com/go-errors/errors"
	"regexp"
	"strconv"
)

var (
	postgresqlVersionRegex  = regexp.MustCompile(`^((1[0-9])\.([0-9]+))?`)
	timescaledbVersionRegex = regexp.MustCompile(`([0-9]+)\.([0-9]+)(\.([0-9]+))?`)
)

const (
	TSDB_MIN_VERSION TimescaleVersion = 21000
	TSDB_212_VERSION TimescaleVersion = 21200
	PG_MIN_VERSION   PostgresVersion  = 130000
	PG_14_VERSION    PostgresVersion  = 140000
	PG_15_VERSION    PostgresVersion  = 150000
)

var (
	BinName    = "timescaledb-event-streamer"
	Version    = "0.3.1"
	CommitHash = "unknown"
	Branch     = "unknown"
)

// PostgresVersion represents the parsed and comparable
// version number of the connected PostgreSQL server
type PostgresVersion uint

// Major returns the major version
func (pv PostgresVersion) Major() uint {
	return uint(pv) / 10000
}

// Minor returns the minor version
func (pv PostgresVersion) Minor() uint {
	return uint(pv) % (100)
}

// String returns the string representation of the PostgreSQL
// server version as in >>major.minor<<
func (pv PostgresVersion) String() string {
	return fmt.Sprintf("%d.%d", pv.Major(), pv.Minor())
}

// Compare returns a negative value if the current version
// is lower than other, returns 0 if the versions match,
// otherwise it returns a value larger than 0.
func (pv PostgresVersion) Compare(other PostgresVersion) int {
	if pv < other {
		return -1
	}
	if other > pv {
		return 1
	}
	return 0
}

// ParsePostgresVersion parses a version string retrieved from
// a PostgreSQL server and returns a PostgresVersion instance
func ParsePostgresVersion(version string) (PostgresVersion, error) {
	matches := postgresqlVersionRegex.FindStringSubmatch(version)
	if len(matches) < 3 {
		return 0, errors.Errorf("failed to extract postgresql version")
	}

	v, err := strconv.ParseInt(matches[2], 10, 32)
	if err != nil {
		return 0, err
	}
	major := uint(v)

	v, err = strconv.ParseInt(matches[3], 10, 32)
	if err != nil {
		return 0, err
	}
	minor := uint(v)

	return PostgresVersion((major * 10000) + minor), nil
}

// TimescaleVersion represents the parsed and comparable
// version number of the TimescaleDB extension loaded in
// the connected PostgreSQL server
type TimescaleVersion uint

// Major returns the major version
func (tv TimescaleVersion) Major() uint {
	return uint(tv) / 10000
}

// Minor returns the minor version
func (tv TimescaleVersion) Minor() uint {
	return (uint(tv) / 100) % 100
}

// Release returns the release version
func (tv TimescaleVersion) Release() uint {
	return uint(tv) % (100)
}

// String returns the string representation of the TimescaleDB
// extension version as in >>major.minor.release<<
func (tv TimescaleVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", tv.Major(), tv.Minor(), tv.Release())
}

// Compare returns a negative value if the current version
// is lower than other, returns 0 if the versions match,
// otherwise it returns a value larger than 0.
func (tv TimescaleVersion) Compare(other TimescaleVersion) int {
	if tv < other {
		return -1
	}
	if other > tv {
		return 1
	}
	return 0
}

// ParseTimescaleVersion parses a TimescaleDB extension version
// string retrieved from a PostgreSQL server and returns a
// TimescaleVersion instance
func ParseTimescaleVersion(version string) (TimescaleVersion, error) {
	matches := timescaledbVersionRegex.FindStringSubmatch(version)
	if len(matches) < 3 {
		return 0, errors.Errorf("failed to extract timescale version")
	}

	v, err := strconv.ParseInt(matches[1], 10, 32)
	if err != nil {
		return 0, err
	}
	major := uint(v)

	v, err = strconv.ParseInt(matches[2], 10, 32)
	if err != nil {
		return 0, err
	}
	minor := uint(v)

	release := uint(0)
	if len(matches) == 5 {
		v, err = strconv.ParseInt(matches[4], 10, 32)
		if err != nil {
			return 0, err
		}
		release = uint(v)
	}

	return TimescaleVersion((major * 10000) + (minor * 100) + release), nil
}
