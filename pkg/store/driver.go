package store

// Driver ...
type Driver interface {
	AsyncReaderStore
	SyncReaderStore
	WriteOnlyStore

	StreamExists(name string) (bool, error)
	CreateStream(name string) (*Stream, error)
	DeleteStream(name string) error
}
