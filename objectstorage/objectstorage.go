package objectstorage

type ObjectStorage interface {
	PutIfAbsent(name string, bytes []byte) error
	// Must return the list of files in ascending order
	ListPrefixOrdered(prefix string) ([]string, error)
	Read(name string) ([]byte, error)
}
