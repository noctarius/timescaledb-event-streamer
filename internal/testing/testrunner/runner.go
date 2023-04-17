package testrunner

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/noctarius/timescaledb-event-streamer/internal"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting"
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/noctarius/timescaledb-event-streamer/internal/sysconfig"
	inttest "github.com/noctarius/timescaledb-event-streamer/internal/testing"
	"github.com/noctarius/timescaledb-event-streamer/internal/testing/containers"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"time"
)

type Context interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
	CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
	Begin(ctx context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Ping(ctx context.Context) error
	CreateHypertable(timeDimension string, chunkSize time.Duration, columns ...systemcatalog.Column) (string, string, error)
	PauseReplicator() error
	ResumeReplicator() error
	attribute(key string, value any)
	getAttribute(key string) any
}

type SetupContext interface {
	Context
	AddSystemConfigConfigurator(fn func(config *sysconfig.SystemConfig))
}

func Attribute[V any](context Context, key string, value V) {
	context.attribute(key, value)
}

func GetAttribute[V any](context Context, key string) V {
	return context.getAttribute(key).(V)
}

type testContext struct {
	pool        *pgxpool.Pool
	streamer    *internal.Streamer
	hypertables []string
	attributes  map[string]any

	streamerRunning bool

	setupFunctions            []func(setupContext SetupContext) error
	tearDownFunction          []func(Context) error
	systemConfigConfigurators []func(config *sysconfig.SystemConfig)
}

func (t *testContext) PauseReplicator() error {
	if !t.streamerRunning {
		return nil
	}
	if err := t.streamer.Stop(); err != nil {
		return err
	}
	t.streamerRunning = false
	return nil
}

func (t *testContext) ResumeReplicator() error {
	if t.streamerRunning {
		return nil
	}
	if err := t.streamer.Start(); err != nil {
		return err
	}
	t.streamerRunning = true
	return nil
}

func (t *testContext) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.pool.Exec(ctx, sql, args...)
}

func (t *testContext) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.pool.Query(ctx, sql, args...)
}

func (t *testContext) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.pool.QueryRow(ctx, sql, args...)
}

func (t *testContext) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	return t.pool.SendBatch(ctx, batch)
}

func (t *testContext) CopyFrom(ctx context.Context, identifier pgx.Identifier,
	strings []string, source pgx.CopyFromSource) (int64, error) {

	return t.pool.CopyFrom(ctx, identifier, strings, source)
}

func (t *testContext) Begin(ctx context.Context) (pgx.Tx, error) {
	return t.pool.Begin(ctx)
}

func (t *testContext) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return t.pool.BeginTx(ctx, txOptions)
}

func (t *testContext) Ping(ctx context.Context) error {
	return t.pool.Ping(ctx)
}

func (t *testContext) CreateHypertable(timeDimension string,
	chunkSize time.Duration, columns ...systemcatalog.Column) (string, string, error) {

	schemaName, tableName, err := inttest.CreateHypertable(t.pool, timeDimension, chunkSize, columns...)
	if err != nil {
		return "", "", err
	}
	t.hypertables = append(t.hypertables, systemcatalog.MakeRelationKey(schemaName, tableName))
	return schemaName, tableName, nil
}

func (t *testContext) AddSystemConfigConfigurator(fn func(config *sysconfig.SystemConfig)) {
	t.systemConfigConfigurators = append(t.systemConfigConfigurators, fn)
}

type TestRunner struct {
	suite.Suite

	container     testcontainers.Container
	userConfig    *pgxpool.Config
	replPgxConfig *pgx.ConnConfig
	logger        *logging.Logger

	withCaller bool
}

func (t *testContext) attribute(key string, value any) {
	t.attributes[key] = value
}

func (t *testContext) getAttribute(key string) any {
	return t.attributes[key]
}

type testConfigurator func(context *testContext)

func WithSetup(fn func(setupContext SetupContext) error) testConfigurator {
	return func(context *testContext) {
		context.setupFunctions = append(context.setupFunctions, fn)
	}
}

func WithTearDown(fn func(context Context) error) testConfigurator {
	return func(context *testContext) {
		context.tearDownFunction = append(context.tearDownFunction, fn)
	}
}

func (tr *TestRunner) SetupSuite() {
	tr.withCaller = logging.WithCaller
	logging.WithCaller = true

	c := &spiconfig.Config{
		Logging: spiconfig.LoggerConfig{
			Level: "debug",
			Outputs: spiconfig.LoggerOutputConfig{
				Console: spiconfig.LoggerConsoleConfig{
					Enabled: supporting.AddrOf(true),
				},
			},
		},
	}

	if err := logging.InitializeLogging(c, false); err != nil {
		tr.T().Error(err)
	}

	logger, err := logging.NewLogger("TestRunner")
	if err != nil {
		tr.T().Error(err)
	}

	tr.logger = logger

	container, configProvider, err := containers.SetupTimescaleContainer()
	if err != nil {
		tr.logger.Fatalf("failed setting up container: %+v", err)
	}
	tr.container = container

	userConfig, err := configProvider.UserConnConfig()
	if err != nil {
		tr.logger.Fatalf("failed setting up user connection config: %+v", err)
	}
	tr.userConfig = userConfig

	replPgxConfig, err := configProvider.ReplicationConnConfig()
	if err != nil {
		tr.logger.Fatalf("failed setting up replication connection config: %+v", err)
	}
	tr.replPgxConfig = replPgxConfig
}

func (tr *TestRunner) TearDownSuite() {
	if tr.container != nil {
		tr.container.Terminate(context.Background())
	}
	logging.WithCaller = tr.withCaller
}

func (tr *TestRunner) RunTest(testFn func(context Context) error, configurators ...testConfigurator) {
	pool, err := pgxpool.NewWithConfig(context.Background(), tr.userConfig)
	if err != nil {
		tr.T().Fatalf("failed to create connection pool: %+v", err)
		return
	}
	defer pool.Close()

	tc := &testContext{
		pool:        pool,
		hypertables: make([]string, 0),
		attributes:  make(map[string]any, 0),
	}

	for _, configurator := range configurators {
		configurator(tc)
	}

	for _, setupFn := range tc.setupFunctions {
		if err := setupFn(tc); err != nil {
			tr.T().Fatalf("failed to setup test: %+v", err)
			return
		}
	}

	replConfig := &spiconfig.Config{
		PostgreSQL: spiconfig.PostgreSQLConfig{
			Publication: spiconfig.PublicationConfig{
				Name: supporting.RandomTextString(10),
			},
		},
		TimescaleDB: spiconfig.TimescaleDBConfig{
			Hypertables: spiconfig.HypertablesConfig{
				Includes: tc.hypertables,
			},
		},
	}
	systemConfig := sysconfig.NewSystemConfig(replConfig)
	systemConfig.PgxConfig = tr.replPgxConfig

	for _, configurator := range tc.systemConfigConfigurators {
		configurator(systemConfig)
	}

	streamer, e := internal.NewStreamer(systemConfig)
	if e != nil {
		tr.T().Fatalf("failed to create streamer with exitCode: %d and error: %+v", e.ExitCode(), e.Error())
		return
	}
	tc.streamer = streamer

	if err := tc.ResumeReplicator(); err != nil {
		tr.T().Fatalf("failed to start streamer: %+v", err)
		return
	}

	defer func() {
		for _, tearDownFn := range tc.tearDownFunction {
			if err := tearDownFn(tc); err != nil {
				tr.T().Fatalf("failed to tear down test: %+v", err)
				return
			}
		}

		if err := tc.PauseReplicator(); err != nil {
			tr.T().Fatalf("failed to stop streamer: %+v", err)
		}
	}()

	if err := testFn(tc); err != nil {
		tr.T().Fatalf("failure in test: %+v", err)
		return
	}
}

type ContainerLogForwarder struct {
	Logger *logging.Logger
}

func (c *ContainerLogForwarder) Accept(log testcontainers.Log) {
	switch log.LogType {
	case testcontainers.StdoutLog:
		c.Logger.Infof(string(log.Content))
	case testcontainers.StderrLog:
		c.Logger.Errorf(string(log.Content))
	}
}
