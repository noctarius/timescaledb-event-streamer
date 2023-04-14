package version

import "github.com/noctarius/timescaledb-event-streamer/spi/version"

const (
	TSDB_MIN_VERSION version.TimescaleVersion = 21000
	TSDB_211_VERSION version.TimescaleVersion = 21100
	PG_MIN_VERSION   version.PostgresVersion  = 130000
	PG_14_VERSION    version.PostgresVersion  = 140000
)

var (
	BinName    = "timescaledb-event-streamer"
	Version    = "0.0.1-dev"
	CommitHash = "unknown"
	Branch     = "unknown"
)

var (
	PostgresqlVersion  version.PostgresVersion
	TimescaleDbVersion version.TimescaleVersion
)
