package systemcatalog

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

func (i *Index) Name() string {
	return i.name
}

func (i *Index) PrimaryKey() bool {
	return i.primaryKey
}

func (i *Index) ReplicaIdentity() bool {
	return i.replicaIdentity
}

func (i *Index) Columns() []*Column {
	return i.columns
}
