package deltalakeclient

import (
	"fmt"

	"github.com/rptynan/delta-lake/objectstorage"
)

// How many rows to accumulate before flushing
// TODO make this configurable, currently set to 10 for easy debugging
// const DATAOBJECT_SIZE int = 64 * 1024
const DATAOBJECT_SIZE int = 10

type DeltaLakeClient struct {
	os objectstorage.ObjectStorage
	// Current transaction, if any. Only one transaction per client at a time. All
	// reads and writes must be within a transaction.
	tx *transaction
}

func NewClient(os objectstorage.ObjectStorage) DeltaLakeClient {
	return DeltaLakeClient{os, nil}
}

var (
	errExistingTx   = fmt.Errorf("Existing Transaction")
	errNoTx         = fmt.Errorf("No Transaction")
	errTableExists  = fmt.Errorf("Table Exists")
	errNoTable      = fmt.Errorf("No Such Table")
	errTypeMismatch = fmt.Errorf("Type mismatch")
)
