package decoding

import "github.com/jackc/pglogrepl"

type BeginMessage pglogrepl.BeginMessage

type CommitMessage pglogrepl.CommitMessage

type OriginMessage pglogrepl.OriginMessage

type RelationMessage pglogrepl.RelationMessage

type TypeMessage pglogrepl.TypeMessage

type TruncateMessage pglogrepl.TruncateMessage

type InsertMessage struct {
	*pglogrepl.InsertMessage
	NewValues map[string]any
}

type UpdateMessage struct {
	*pglogrepl.UpdateMessage
	OldValues map[string]any
	NewValues map[string]any
}

type DeleteMessage struct {
	*pglogrepl.DeleteMessage
	OldValues map[string]any
}
