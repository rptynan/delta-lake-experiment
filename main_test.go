package main

import (
	"os"
	"testing"

	"github.com/rptynan/delta-lake/deltalakeclient"
	"github.com/rptynan/delta-lake/objectstorage"
	"github.com/rptynan/delta-lake/utils"
)

func TestConcurrentTableWriters(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-database")
	if err != nil {
		panic(err)
	}

	defer os.Remove(dir)

	fos := objectstorage.NewFileObjectStorage(dir)
	c1Writer := deltalakeclient.NewClient(fos)
	c2Writer := deltalakeclient.NewClient(fos)

	// Have c2Writer start up a transaction.
	err = c2Writer.NewTx()
	utils.AssertEq(err, nil, "could not start first c2 tx")
	utils.Debug("[c2] new tx")

	// But then have c1Writer start a transaction and commit it first.
	err = c1Writer.NewTx()
	utils.AssertEq(err, nil, "could not start first c1 tx")
	utils.Debug("[c1] new tx")
	err = c1Writer.CreateTable("x", []string{"a", "b"})
	utils.AssertEq(err, nil, "could not create x")
	utils.Debug("[c1] Created table")
	err = c1Writer.WriteRow("x", []any{"Joey", 1})
	utils.AssertEq(err, nil, "could not write first row")
	utils.Debug("[c1] Wrote row")
	err = c1Writer.WriteRow("x", []any{"Yue", 2})
	utils.AssertEq(err, nil, "could not write second row")
	utils.Debug("[c1] Wrote row")
	err = c1Writer.CommitTx()
	utils.AssertEq(err, nil, "could not commit tx")
	utils.Debug("[c1] Committed tx")

	// Now go back to c2 and write data.
	err = c2Writer.CreateTable("x", []string{"a", "b"})
	utils.AssertEq(err, nil, "could not create x")
	utils.Debug("[c2] Created table")
	err = c2Writer.WriteRow("x", []any{"Holly", 1})
	utils.AssertEq(err, nil, "could not write first row")
	utils.Debug("[c2] Wrote row")

	err = c2Writer.CommitTx()
	utils.Assert(err != nil, "concurrent commit must fail")
	utils.Debug("[c2] tx not committed")
}

func TestConcurrentReaderWithWriterReadsSnapshot(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-database")
	if err != nil {
		panic(err)
	}

	defer os.Remove(dir)

	fos := objectstorage.NewFileObjectStorage(dir)
	c1Writer := deltalakeclient.NewClient(fos)
	c2Reader := deltalakeclient.NewClient(fos)

	// First create some data and commit the transaction.
	err = c1Writer.NewTx()
	utils.AssertEq(err, nil, "could not start first c1 tx")
	utils.Debug("[c1Writer] Started tx")
	err = c1Writer.CreateTable("x", []string{"a", "b"})
	utils.AssertEq(err, nil, "could not create x")
	utils.Debug("[c1Writer] Created table")
	err = c1Writer.WriteRow("x", []any{"Joey", 1})
	utils.AssertEq(err, nil, "could not write first row")
	utils.Debug("[c1Writer] Wrote row")
	err = c1Writer.WriteRow("x", []any{"Yue", 2})
	utils.AssertEq(err, nil, "could not write second row")
	utils.Debug("[c1Writer] Wrote row")
	err = c1Writer.CommitTx()
	utils.AssertEq(err, nil, "could not commit tx")
	utils.Debug("[c1Writer] Committed tx")

	// Now start a new transaction for more edits.
	err = c1Writer.NewTx()
	utils.AssertEq(err, nil, "could not start second c1 tx")
	utils.Debug("[c1Writer] Starting new write tx")

	// Before we commit this second write-transaction, start a
	// read transaction.
	err = c2Reader.NewTx()
	utils.AssertEq(err, nil, "could not start c2 tx")
	utils.Debug("[c2Reader] Started tx")

	// Write and commit rows in c1.
	err = c1Writer.WriteRow("x", []any{"Ada", 3})
	utils.AssertEq(err, nil, "could not write third row")
	utils.Debug("[c1Writer] Wrote third row")

	// Scan x in read-only transaction
	it, err := c2Reader.Scan("x")
	utils.AssertEq(err, nil, "could not scan x")
	utils.Debug("[c2Reader] Started scanning")
	seen := 0
	for {
		row, err := it.Next()
		utils.AssertEq(err, nil, "could not iterate x scan")

		if row == nil {
			utils.Debug("[c2Reader] Done scanning")
			break
		}

		utils.Debug("[c2Reader] Got row in reader tx", row)
		if seen == 0 {
			utils.AssertEq(row[0], "Joey", "row mismatch in c1")
			utils.AssertEq(row[1], 1.0, "row mismatch in c1")
		} else {
			utils.AssertEq(row[0], "Yue", "row mismatch in c1")
			utils.AssertEq(row[1], 2.0, "row mismatch in c1")
		}

		seen++
	}
	utils.AssertEq(seen, 2, "expected two rows")

	// Scan x in c1 write transaction
	it, err = c1Writer.Scan("x")
	utils.AssertEq(err, nil, "could not scan x in c1")
	utils.Debug("[c1Writer] Started scanning")
	seen = 0
	for {
		row, err := it.Next()
		utils.AssertEq(err, nil, "could not iterate x scan in c1")

		if row == nil {
			utils.Debug("[c1Writer] Done scanning")
			break
		}

		utils.Debug("[c1Writer] Got row in tx", row)

		if seen == 0 {
			utils.AssertEq(row[0], "Ada", "row mismatch in c1")
			// Since this hasn't been serialized to JSON, it's still an int not a float.
			utils.AssertEq(row[1], 3, "row mismatch in c1")
		} else if seen == 1 {
			utils.AssertEq(row[0], "Joey", "row mismatch in c1")
			utils.AssertEq(row[1], 1.0, "row mismatch in c1")
		} else {
			utils.AssertEq(row[0], "Yue", "row mismatch in c1")
			utils.AssertEq(row[1], 2.0, "row mismatch in c1")
		}

		seen++
	}
	utils.AssertEq(seen, 3, "expected three rows")

	// Writer committing should succeed.
	err = c1Writer.CommitTx()
	utils.AssertEq(err, nil, "could not commit second tx")
	utils.Debug("[c1Writer] Committed tx")

	// Reader committing should succeed.
	err = c2Reader.CommitTx()
	utils.AssertEq(err, nil, "could not commit read-only tx")
	utils.Debug("[c2Reader] Committed tx")
}

func scanAllRows(c deltalakeclient.DeltaLakeClient) [][]any {
	// Scan x in read-only transaction
	it, err := c.Scan("x")
	utils.AssertEq(err, nil, "could not scan")

	var result [][]any
	for {
		row, err := it.Next()
		utils.AssertEq(err, nil, "could not iterate scan")

		if row == nil {
			break
		}

		result = append(result, row)
	}

	return result
}

func TestDeletes(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-database")
	if err != nil {
		panic(err)
	}

	defer os.Remove(dir)

	fos := objectstorage.NewFileObjectStorage(dir)
	c1Writer := deltalakeclient.NewClient(fos)

	// Setup data
	err = c1Writer.NewTx()
	utils.AssertEq(err, nil, "could not start first c1 tx")
	utils.Debug("[c1] new tx")
	err = c1Writer.CreateTable("x", []string{"a", "b"})
	utils.AssertEq(err, nil, "could not create x")
	utils.Debug("[c1] Created table")
	err = c1Writer.WriteRow("x", []any{"Joey", 1})
	utils.AssertEq(err, nil, "could not write row")
	err = c1Writer.WriteRow("x", []any{"Yue", 2})
	utils.AssertEq(err, nil, "could not write row")
	err = c1Writer.WriteRow("x", []any{"Alice", 3})
	utils.AssertEq(err, nil, "could not write row")
	utils.Debug("[c1] Wrote rows")

	// Delete rows and check
	err = c1Writer.DeleteRows("x", "b", deltalakeclient.QueryRange{Start: 2, End: 2})
	utils.AssertEq(err, nil, "could not delete")
	utils.Debug("[c1] Deleted row")

	rows := scanAllRows(c1Writer)
	utils.Debug(rows)
	utils.AssertEq(len(rows), 2, "result length wrong")
	utils.AssertEq(rows[0][0], "Joey", "result wrong")
	utils.AssertEq(rows[1][0], "Alice", "result wrong")

	// Try same after committing the row to be deleted
	err = c1Writer.CommitTx()
	utils.AssertEq(err, nil, "could not commit tx")
	utils.Debug("[c1] Committed tx")
	err = c1Writer.NewTx()
	utils.AssertEq(err, nil, "could not start second c1 tx")
	utils.Debug("[c1] new tx")

	err = c1Writer.DeleteRows("x", "b", deltalakeclient.QueryRange{Start: 2, End: 4})
	utils.AssertEq(err, nil, "could not delete")
	utils.Debug("[c1] Deleted row")

	rows = scanAllRows(c1Writer)
	utils.Debug(rows)
	utils.AssertEq(len(rows), 1, "result length wrong")
	utils.AssertEq(rows[0][0], "Joey", "result wrong")

	// And lets flush all those rows too just to make sure
	c1Writer.CommitTx()
	c1Writer.NewTx()

	rows = scanAllRows(c1Writer)
	utils.Debug(rows)
	utils.AssertEq(len(rows), 1, "result length wrong")
	utils.AssertEq(rows[0][0], "Joey", "result wrong")
}
