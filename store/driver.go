package store

type Driver interface {
	AsyncReader
	SyncReader
	Writer

	StreamExists(name string) bool
	GetStream(name string) *Stream
	CreateStream(name string) (*Stream, error)
	DeleteStream(name string) error
}
