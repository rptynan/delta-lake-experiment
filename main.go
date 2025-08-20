package main

import (
	"fmt"

	"github.com/rptynan/delta-lake/objectstorage"
)

func main() {
	fmt.Println("hello world")

	var client objectstorage.ObjectStorage = objectstorage.NewFileObjectStorage("./tmp")

	client.PutIfAbsent("foo", []byte{0, 1, 2})
}
