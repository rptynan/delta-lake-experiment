package deltalakeclient

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/rptynan/delta-lake/objectstorage"
)

// How many rows to accumulate before flushing
const DATAOBJECT_SIZE int = 64 * 1024

// TODO unexport this?
type DeltaLakeClient struct {
	os objectstorage.ObjectStorage
	// Current transaction, if any. Only one transaction per client at a time. All
	// reads and writes must be within a transaction.
	tx *transaction
}

// TODO maybe split up this file (types, methods, errors?)

func NewClient(os objectstorage.ObjectStorage) DeltaLakeClient {
	return DeltaLakeClient{os, nil}
}

var (
	errExistingTx  = fmt.Errorf("Existing Transaction")
	errNoTx        = fmt.Errorf("No Transaction")
	errTableExists = fmt.Errorf("Table Exists")
	errNoTable     = fmt.Errorf("No Such Table")
)

type DataobjectAction struct {
	Name  string
	Table string
}

type ChangeMetadataAction struct {
	Table   string
	Columns []string
}

// an enum, only one field will be non-nil
type Action struct {
	AddDataobject  *DataobjectAction
	ChangeMetadata *ChangeMetadataAction
	// TODO: Support object removal.
	// DeleteDataobject *DataobjectAction
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
				if action.AddDataobject != nil {
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

func (d *DeltaLakeClient) CreateTable(table string, columns []string) error {
	if d.tx == nil {
		return errNoTx
	}

	if _, exists := d.tx.tables[table]; exists {
		return errTableExists
	}

	// Store it in the in-memory mapping.
	d.tx.tables[table] = columns

	// And also add it to the action history for future transactions.
	d.tx.Actions[table] = append(d.tx.Actions[table], Action{
		ChangeMetadata: &ChangeMetadataAction{
			Table:   table,
			Columns: columns,
		},
	})

	return nil
}

func (d *DeltaLakeClient) WriteRow(table string, row []any) error {
	if d.tx == nil {
		return errNoTx
	}

	if _, ok := d.tx.tables[table]; !ok {
		return errNoTable
	}

	// First see if we have unflushed data
	pointer, ok := d.tx.unflushedDataPointer[table]
	if !ok {
		d.tx.unflushedDataPointer[table] = 0
		pointer = 0 // not necessary, but I'll be explicit (maybe not idiomatic go?)
		d.tx.unflushedData[table] = &[DATAOBJECT_SIZE][]any{}
	}

	if pointer == DATAOBJECT_SIZE {
		d.flushRows(table)
		pointer = 0
	}
	d.tx.unflushedData[table][pointer] = row
	d.tx.unflushedDataPointer[table]++
	return nil
}

type dataobject struct {
	Table string
	Name  string
	Data  [DATAOBJECT_SIZE][]any
	Len   int
}

func (d *DeltaLakeClient) flushRows(table string) error {
	// Early return if there's no unflushed data
	pointer, ok := d.tx.unflushedDataPointer[table]
	if !ok || pointer == 0 {
		return nil
	}

	dataobject := dataobject{
		Table: table,
		Name:  uuid.New().String(),
		Data:  *d.tx.unflushedData[table],
		Len:   pointer,
	}
	serialisedbytes, err := json.Marshal(dataobject)
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("_table_%s_%s", table, dataobject.Name)
	err = d.os.PutIfAbsent(filename, serialisedbytes)
	if err != nil {
		return err
	}

	d.tx.Actions[table] = append(d.tx.Actions[table], Action{
		AddDataobject: &DataobjectAction{
			Name: dataobject.Name, Table: table,
		},
	})

	// Don't forget to reset pointer
	d.tx.unflushedDataPointer[table] = 0
	return nil
}

type scanIterator struct {
	d     *DeltaLakeClient
	table string

	// First we iterate through unflushed rows.
	unflushedRows       [DATAOBJECT_SIZE][]any
	unflushedRowsLen    int
	unflushedRowPointer int

	// Then we move through each dataobject.
	dataobjects        []string // Just the names, we fetch next one on calling next()
	dataobjectsPointer int

	// And within each currentDataobject we iterate through rows.
	currentDataobject        *dataobject // Content of current dataobject
	currentDataObjectPointer int
}

func (d *DeltaLakeClient) Scan(table string) (*scanIterator, error) {
	if d.tx == nil {
		return nil, errNoTx
	}

	// Unflushed rows
	var unflushedRows [DATAOBJECT_SIZE][]any
	if unflushedData, ok := d.tx.unflushedData[table]; ok {
		unflushedRows = *unflushedData
	}
	unflushedRowsLen := d.tx.unflushedDataPointer[table]

	// Flushed
	var dataobjects []string
	allActions := append(d.tx.previousActions[table], d.tx.Actions[table]...)
	for _, action := range allActions {
		if action.AddDataobject != nil {
			dataobjects = append(dataobjects, action.AddDataobject.Name)
		}
	}

	return &scanIterator{
		d:                d,
		table:            table,
		unflushedRows:    unflushedRows,
		unflushedRowsLen: unflushedRowsLen,
		dataobjects:      dataobjects,
	}, nil
}

func (d *DeltaLakeClient) readDataobject(table, name string) (*dataobject, error) {
	bytes, err := d.os.Read(fmt.Sprintf("_table_%s_%s", table, name))
	if err != nil {
		return nil, err
	}

	var do dataobject
	err = json.Unmarshal(bytes, &do)
	return &do, err
}

func (si *scanIterator) Next() ([]any, error) {
	// Unflushed rows first
	if si.unflushedRowPointer < si.unflushedRowsLen {
		row := si.unflushedRows[si.unflushedRowPointer]
		si.unflushedRowPointer++
		return row, nil
	}

	// Then flushed rows

	// If we are done with dataobjects, we're done overall.
	if si.dataobjectsPointer == len(si.dataobjects) {
		return nil, nil
	}

	if si.currentDataobject == nil {
		object, err := si.d.readDataobject(si.table, si.dataobjects[si.dataobjectsPointer])
		if err != nil {
			return nil, err
		}
		si.currentDataobject = object
	}

	if si.currentDataObjectPointer > si.currentDataobject.Len {
		si.currentDataobject = nil
		si.dataobjectsPointer++
		si.currentDataObjectPointer = 0

		return si.Next()
	}

	row := si.currentDataobject.Data[si.currentDataObjectPointer]
	si.currentDataObjectPointer++
	return row, nil
}
