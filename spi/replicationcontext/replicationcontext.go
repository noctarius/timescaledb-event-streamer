package replicationcontext

import (
	"context"
	"github.com/jackc/pgx/v5/pgconn"
	spiconfig "github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/pgtypes"
	"github.com/noctarius/timescaledb-event-streamer/spi/statestorage"
	"github.com/noctarius/timescaledb-event-streamer/spi/version"
)

type ReplicationContext interface {
	StartReplicationContext() error
	StopReplicationContext() error
	NewReplicationChannelConnection(
		ctx context.Context,
	) (*pgconn.PgConn, error)

	Offset() (*statestorage.Offset, error)
	SetLastTransactionId(
		xid uint32,
	)
	LastTransactionId() uint32
	SetLastBeginLSN(
		lsn pgtypes.LSN,
	)
	LastBeginLSN() pgtypes.LSN
	SetLastCommitLSN(
		lsn pgtypes.LSN,
	)
	LastCommitLSN() pgtypes.LSN
	AcknowledgeReceived(
		xld pgtypes.XLogData,
	)
	LastReceivedLSN() pgtypes.LSN
	AcknowledgeProcessed(
		xld pgtypes.XLogData, processedLSN *pgtypes.LSN,
	) error
	LastProcessedLSN() pgtypes.LSN
	SetPositionLSNs(
		receivedLSN, processedLSN pgtypes.LSN,
	)

	InitialSnapshotMode() spiconfig.InitialSnapshotMode
	DatabaseUsername() string
	ReplicationSlotName() string
	ReplicationSlotCreate() bool
	ReplicationSlotAutoDrop() bool
	WALLevel() string
	SystemId() string
	Timeline() int32
	DatabaseName() string

	PostgresVersion() version.PostgresVersion
	TimescaleVersion() version.TimescaleVersion
	IsMinimumPostgresVersion() bool
	IsPG14GE() bool
	IsMinimumTimescaleVersion() bool
	IsTSDB212GE() bool
	IsLogicalReplicationEnabled() bool

	ExistsReplicationSlot(
		slotName string,
	) (found bool, err error)
	ReadReplicationSlot(
		slotName string,
	) (pluginName, slotType string, restartLsn, confirmedFlushLsn pgtypes.LSN, err error)
}
