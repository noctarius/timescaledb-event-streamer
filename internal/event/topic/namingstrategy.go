package topic

import (
	"github.com/noctarius/event-stream-prototype/internal/systemcatalog/model"
	"strings"
)

type NamingStrategy interface {
	EventTopicName(topicPrefix string, hypertable *model.Hypertable) string
	SchemaTopicName(topicPrefix string, hypertable *model.Hypertable) string
	MessageTopicName(topicPrefix string) string
}

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
