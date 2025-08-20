package main

import (
	"fmt"

	"github.com/rptynan/delta-lake/deltalakeclient"
	"github.com/rptynan/delta-lake/objectstorage"
)

// Random TODOs
// - If you call flushRows with less than DATAOBJECT_SIZE in the unflushed rows, you'll save lots of nulls to disk.

func main() {
	fmt.Println("hello world")

	var objectstorage objectstorage.ObjectStorage = objectstorage.NewFileObjectStorage("./tmp")

	client := deltalakeclient.NewClient(objectstorage)

	err := client.NewTx()
	if err != nil {
		panic(err)
	}
	err = client.CreateTable("users", []string{"id", "name", "email"})
	if err != nil {
		panic(err)
	}
	err = client.WriteRow("users", []any{123, "bob", "bob@thebuilder.com"})
	if err != nil {
		panic(err)
	}
	err = client.CommitTx()
	if err != nil {
		panic(err)
	}

	fmt.Println("done")
}
