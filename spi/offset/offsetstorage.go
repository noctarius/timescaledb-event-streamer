package offset

type Storage interface {
	Start() error
	Stop() error
	Save() error
	Load() error
	Get() (map[string]*Offset, error)
	Set(key string, value *Offset) error
}
