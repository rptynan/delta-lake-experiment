package objectstorage

type ObjectStorage interface {
	PutIfAbsent(name string, bytes []byte) error
	listPrefix(prefix string) ([]string, error)
	read(name string) ([]byte, error)
}
