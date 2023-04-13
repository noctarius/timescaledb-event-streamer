package containers

import (
	"github.com/noctarius/timescaledb-event-streamer/internal/supporting/logging"
	"github.com/testcontainers/testcontainers-go"
)

type logConsumer struct {
	logger *logging.Logger
}

func newLogConsumer(logger *logging.Logger) *logConsumer {
	return &logConsumer{
		logger: logger,
	}
}

func (l *logConsumer) Accept(log testcontainers.Log) {
	if log.LogType == testcontainers.StderrLog {
		l.logger.Errorln(string(log.Content))
	} else {
		l.logger.Println(string(log.Content))
	}
}
