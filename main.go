package main

import (
	"fmt"

	"github.com/rptynan/delta-lake/deltalakeclient"
	"github.com/rptynan/delta-lake/objectstorage"
)

func assertErr(err error) {
	if err != nil {
		panic(err)
	}
}

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
	scanIt, err := client.Scan("users")
	assertErr(err)
	// fmt.Println(scanIt)
	fmt.Println(scanIt.Next())
	fmt.Println(scanIt.Next())

	err = client.CommitTx()
	if err != nil {
		panic(err)
	}

	// Read committed data in new tx
	err = client.NewTx()
	assertErr(err)

	scanIt, err = client.Scan("users")
	assertErr(err)
	// fmt.Println(scanIt)
	fmt.Println(scanIt.Next())
	fmt.Println(scanIt.Next())

	fmt.Println("done")
}
