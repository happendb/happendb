package store

// Driver ...
type Driver interface {
	AsyncReaderStore
	SyncReaderStore
	WriteOnlyStore

	CreateStream(streamName string) (*Stream, error)
	HasStream(streamName string) (bool, error)
}
