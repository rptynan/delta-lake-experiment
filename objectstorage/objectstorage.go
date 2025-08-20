package objectstorage

type ObjectStorage interface {
	PutIfAbsent(name string, bytes []byte) error
	ListPrefix(prefix string) ([]string, error)
	Read(name string) ([]byte, error)
}
