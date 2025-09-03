package deltalakeclient

import (
	"encoding/json"
	"fmt"
)

type dataobjectAction struct {
	Name  string
	Table string
}

type changeMetadataAction struct {
	Table   string
	Columns []string
}

// an enum, only one field will be non-nil
type Action struct {
	AddDataobject    *dataobjectAction
	DeleteDataobject *dataobjectAction
	ChangeMetadata   *changeMetadataAction
}

type transaction struct {
	Id int

	// Both below are mapping table name to a list of actions on the table.
	// previousActions is a populated (when we start a new transaction) by reading
	// through all the existing log files.
	previousActions map[string][]Action
	// Actions is the set of actions for the current transaction before commit.
	Actions map[string][]Action

	// Mapping tables to column names.
	// Add ChangeMetadataActions in either previousActions or Actions, will cause
	// the columns here to be updated.
	tables map[string][]string

	// Mapping table name to unflushed/in-memory rows. When rows are flushed, the
	// dataobject that contains them is added to `tx.actions` above and
	// `tx.unflushedDataPointer[table]` is reset to `0`.
	unflushedData        map[string]*[DATAOBJECT_SIZE][]any
	unflushedDataPointer map[string]int
}

func (d *DeltaLakeClient) NewTx() error {
	if d.tx != nil {
		return errExistingTx
	}

	logPrefix := "_log_"
	txLogFilenames, err := d.os.ListPrefix(logPrefix)
	if err != nil {
		return err
	}

	tx := &transaction{}
	tx.previousActions = map[string][]Action{}
	tx.Actions = map[string][]Action{}
	tx.tables = map[string][]string{}
	tx.unflushedData = map[string]*[DATAOBJECT_SIZE][]any{}
	tx.unflushedDataPointer = map[string]int{}

	for _, txLogFilename := range txLogFilenames {
		bytes, err := d.os.Read(txLogFilename)
		if err != nil {
			return err
		}

		var oldTx transaction
		err = json.Unmarshal(bytes, &oldTx)
		if err != nil {
			return err
		}
		// Transaction metadata files are sorted
		// lexicographically so that the most recent
		// transaction (i.e. the one with the largest
		// transaction id) will be last and tx.Id will end up
		// 1 greater than the most recent transaction ID we
		// see on disk.
		tx.Id = oldTx.Id + 1

		for table, actions := range oldTx.Actions {
			for _, action := range actions {
				if action.AddDataobject != nil || action.DeleteDataobject != nil {
					tx.previousActions[table] = append(tx.previousActions[table], action)
				} else if action.ChangeMetadata != nil {
					// Store the latest version of
					// each table in memory for
					// easy lookup.
					mtd := action.ChangeMetadata
					tx.tables[table] = mtd.Columns
				} else {
					panic(fmt.Sprintf("unsupported action: %v", action))
				}
			}
		}
	}

	d.tx = tx
	return nil
}

func (d *DeltaLakeClient) CommitTx() error {
	if d.tx == nil {
		return errNoTx
	}

	// Flush any outstanding data
	for table := range d.tx.tables {
		err := d.flushRows(table)
		if err != nil {
			d.tx = nil
			return err
		}
	}

	wrote := false
	for _, actions := range d.tx.Actions {
		if len(actions) > 0 {
			wrote = true
			break
		}
	}
	// Read-only transaction, no need to do a concurrency check.
	if !wrote {
		d.tx = nil
		return nil
	}

	filename := fmt.Sprintf("_log_%020d", d.tx.Id)
	// We won't store previous actions, they will be recovered on
	// new transactions. So unset them. Honestly not totally
	// clear why.
	d.tx.previousActions = nil
	bytes, err := json.Marshal(d.tx)
	if err != nil {
		d.tx = nil
		return err
	}

	err = d.os.PutIfAbsent(filename, bytes)
	d.tx = nil
	return err
}

func (d *DeltaLakeClient) flushRows(table string) error {
	// Early return if there's no unflushed data
	pointer, ok := d.tx.unflushedDataPointer[table]
	if !ok || pointer == 0 {
		return nil
	}

	addDataobjectAction, err := d.writeDataObject(table, d.tx.unflushedData[table])
	if err != nil {
		return err
	}

	d.tx.Actions[table] = append(d.tx.Actions[table], addDataobjectAction)

	// Don't forget to reset pointer
	d.tx.unflushedDataPointer[table] = 0
	return nil
}
