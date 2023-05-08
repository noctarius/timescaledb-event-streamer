package systemcatalog

// Index represents either a (compound) primary key index
// or replica identity index in the database and attached
// to a hypertable (and its chunks)
type Index struct {
	name            string
	columns         []*Column
	primaryKey      bool
	replicaIdentity bool
}

func newIndex(name string, column []*Column, primaryKey bool, replicaIdentity bool) *Index {
	return &Index{
		name:            name,
		columns:         column,
		primaryKey:      primaryKey,
		replicaIdentity: replicaIdentity,
	}
}

// Name returns the index name
func (i *Index) Name() string {
	return i.name
}

// PrimaryKey returns true if the index represents a
// primary key, otherwise false
func (i *Index) PrimaryKey() bool {
	return i.primaryKey
}

// ReplicaIdentity returns true if the index represents a
// replica identity index, otherwise false
func (i *Index) ReplicaIdentity() bool {
	return i.replicaIdentity
}

// Columns returns an array of colum instances representing
// the columns of the index (in order of definition)
func (i *Index) Columns() []*Column {
	return i.columns
}
