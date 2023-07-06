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

package containers

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

const (
	databaseName   = "tsdb"
	databaseSchema = "tsdb"
	postgresUser   = "postgres"
	postgresPass   = "postgres"
	tsdbUser       = "tsdb"
	tsdbPass       = "tsdb"
	replUser       = "repl_user"
	replPass       = "repl_user"
)

var initialPublicationFunction = `FAILED TO READ SQL FILE`

func init() {
	if _, filename, _, ok := runtime.Caller(0); ok {
		base := filepath.Dir(filename)
		dir := filepath.Clean(filepath.Join(base, "./../../../"))
		f, err := os.Open(filepath.Join(dir, "create_timescaledb_catalog_publication.sql"))
		if err != nil {
			panic("failed to open file to load the publication sql file")
		}
		d, err := io.ReadAll(f)
		if err != nil {
			panic("failed to read file to load the publication sql file")
		}
		initialPublicationFunction = string(d)
	} else {
		panic("failed getting the initial package name to load the publication sql file")
	}
}

type ConfigProvider struct {
	host string
	port int
}

func (c *ConfigProvider) ReplicationConnConfig() (*pgx.ConnConfig, error) {
	return pgx.ParseConfig(
		fmt.Sprintf("postgres://%s:%s@%s:%d/%s", replUser, replPass, c.host, c.port, databaseName),
	)
}

func (c *ConfigProvider) UserConnConfig() (*pgxpool.Config, error) {
	return pgxpool.ParseConfig(
		fmt.Sprintf("postgres://%s:%s@%s:%d/%s", tsdbUser, tsdbPass, c.host, c.port, databaseName),
	)
}

func SetupTimescaleContainer() (testcontainers.Container, *ConfigProvider, error) {
	containerRequest := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb:latest-pg15",
		ExposedPorts: []string{"5432/tcp"},
		Cmd:          []string{"-c", "fsync=off", "-c", "wal_level=logical"},
		WaitingFor:   wait.ForListeningPort("5432/tcp"),
		Env: map[string]string{
			"POSTGRES_DB":       databaseName,
			"POSTGRES_PASSWORD": postgresPass,
			"POSTGRES_USER":     postgresUser,
		},
	}

	logger, err := logging.NewLogger("testcontainers")
	if err != nil {
		return nil, nil, err
	}
	timescaledbLogger, err := logging.NewLogger("testcontainers-timescaledb")
	if err != nil {
		return nil, nil, err
	}

	container, err := testcontainers.GenericContainer(
		context.Background(),
		testcontainers.GenericContainerRequest{
			ContainerRequest: containerRequest,
			Started:          true,
			Logger:           logger,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	// Collect logs
	//container.FollowOutput(&logConsumer{})
	//container.StartLogProducer(context.Background())

	host, err := container.Host(context.Background())
	if err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	port, err := container.MappedPort(context.Background(), "5432/tcp")
	if err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		postgresUser, postgresPass, host, port.Int(), databaseName)

	config, err := pgx.ParseConfig(connString)
	if err != nil {
		container.Terminate(context.Background())
		return nil, nil, err
	}

	var conn *pgx.Conn
	for i := 0; ; i++ {
		conn, err = pgx.ConnectConfig(context.Background(), config)
		if err != nil {
			if i == 9 {
				_ = container.Terminate(context.Background())
				return nil, nil, err
			} else {
				time.Sleep(time.Second)
			}
		} else {
			break
		}
	}

	exec := func(query string) error {
		if _, err := conn.Exec(context.Background(), query); err != nil {
			conn.Close(context.Background())
			container.Terminate(context.Background())
			return err
		}
		return nil
	}

	// Disable timescale plugin
	timescaledbLogger.Verbosef("Drop timescaledb extension")
	if err := exec("DROP EXTENSION IF EXISTS timescaledb"); err != nil {
		return nil, nil, err
	}

	// Create default user role
	timescaledbLogger.Verbosef("Create default user login")
	if err := exec(
		fmt.Sprintf("CREATE ROLE %s LOGIN ENCRYPTED PASSWORD '%s'", tsdbUser, tsdbPass),
	); err != nil {
		return nil, nil, err
	}
	timescaledbLogger.Verbosef("Grant permissions to default user")
	if err := exec(
		fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s", databaseName, tsdbUser),
	); err != nil {
		return nil, nil, err
	}
	timescaledbLogger.Verbosef("Create %s schema", databaseSchema)
	if err := exec(
		fmt.Sprintf("CREATE SCHEMA %s", databaseSchema),
	); err != nil {
		return nil, nil, err
	}
	timescaledbLogger.Verbosef("Grant schema permissions to default user")
	if err := exec(
		fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", databaseSchema, tsdbUser),
	); err != nil {
		return nil, nil, err
	}
	timescaledbLogger.Verbosef("Set default schema for default user")
	if err := exec(
		fmt.Sprintf("ALTER ROLE %s SET search_path TO %s, public", tsdbUser, databaseSchema),
	); err != nil {
		return nil, nil, err
	}

	// Create replication user and adjust permissions
	timescaledbLogger.Verbosef("Create replication user login")
	if err := exec(
		fmt.Sprintf("CREATE ROLE %s LOGIN REPLICATION ENCRYPTED PASSWORD '%s'", replUser, replPass),
	); err != nil {
		return nil, nil, err
	}
	timescaledbLogger.Verbosef("Grant user permissions for replication user")
	if err := exec(
		fmt.Sprintf("GRANT %s TO %s", tsdbUser, replUser),
	); err != nil {
		return nil, nil, err
	}
	timescaledbLogger.Verbosef("Create timescaledb extension")
	if err := exec("CREATE EXTENSION IF NOT EXISTS timescaledb"); err != nil {
		return nil, nil, err
	}
	timescaledbLogger.Verbosef("Drop existing publication")

	// Create initial publication function
	timescaledbLogger.Verbosef("Create publication initiator function")
	if err := exec(initialPublicationFunction); err != nil {
		return nil, nil, err
	}

	// Close database setup connection when done
	conn.Close(context.Background())

	return container, &ConfigProvider{host, port.Int()}, nil
}
