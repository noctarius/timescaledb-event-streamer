package pgtypes

type ReplicaIdentity string

const (
	NOTHING ReplicaIdentity = "n"
	FULL    ReplicaIdentity = "f"
	DEFAULT ReplicaIdentity = "d"
	INDEX   ReplicaIdentity = "i"
	UNKNOWN ReplicaIdentity = ""
)

// Description returns a description of this REPLICA IDENTITY
// Values are in sync with debezium project:
// https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/connection/ServerInfo.java
func (ri ReplicaIdentity) Description() string {
	switch ri {
	case NOTHING:
		return "UPDATE and DELETE events will not contain any old values"
	case FULL:
		return "UPDATE AND DELETE events will contain the previous values of all the columns"
	case DEFAULT:
		return "UPDATE and DELETE events will contain previous values only for PK columns"
	case INDEX:
		return "UPDATE and DELETE events will contain previous values only for columns present in the REPLICA IDENTITY index"
	}
	return "Unknown REPLICA IDENTITY"
}

func AsReplicaIdentity(val string) ReplicaIdentity {
	switch val {
	case string(NOTHING):
		return NOTHING
	case string(FULL):
		return FULL
	case string(DEFAULT):
		return DEFAULT
	case string(INDEX):
		return INDEX
	}
	return UNKNOWN
}
