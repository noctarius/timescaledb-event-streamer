package namingstrategy

import (
	"github.com/noctarius/timescaledb-event-streamer/spi/config"
	"github.com/noctarius/timescaledb-event-streamer/spi/systemcatalog"
	"strings"
)

type Provider = func(config *config.Config) (NamingStrategy, error)

// NamingStrategy represents a strategy to generate
// topic names for event topics, schema topics, as
// well as message topics
type NamingStrategy interface {
	// EventTopicName generates a event topic name for the given hypertable
	EventTopicName(topicPrefix string, hypertable *systemcatalog.Hypertable) string
	// SchemaTopicName generates a schema topic name for the given hypertable
	SchemaTopicName(topicPrefix string, hypertable *systemcatalog.Hypertable) string
	// MessageTopicName generates a message topic name for a replication message
	MessageTopicName(topicPrefix string) string
}

// SanitizeTopicName is a helper to sanitize topic
// names to be as compatible as possible
func SanitizeTopicName(topicName string) (topic string, changed bool) {
	runes := []rune(topicName)

	builder := strings.Builder{}
	for i := 0; i < len(topicName); i++ {
		if isValidCharacter(runes[i]) {
			builder.WriteRune(runes[i])
		} else {
			changed = true
			builder.WriteRune('_')
		}
	}
	return builder.String(), changed
}

func isValidCharacter(r rune) bool {
	return r == '.' ||
		r == '_' ||
		r == '-' ||
		(r >= 'A' && r <= 'Z') ||
		(r >= 'a' && r <= 'z') ||
		(r >= '0' && r <= '9')
}
