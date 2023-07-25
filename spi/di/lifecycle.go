package lifecycle

type Lifecycle interface {
	PostConstruct() error
	Start() error
	Stop() error
}
