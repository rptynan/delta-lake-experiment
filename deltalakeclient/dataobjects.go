package deltalakeclient

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/google/uuid"
)

type dataobjectT struct {
	Table string
	Name  string
	Data  [DATAOBJECT_SIZE][]any
	Len   int
}

func (d *DeltaLakeClient) readDataobject(table, name string) (*dataobjectT, error) {
	bytes, err := d.os.Read(fmt.Sprintf("_table_%s_%s", table, name))
	if err != nil {
		return nil, err
	}

	var do dataobjectT
	err = json.Unmarshal(bytes, &do)
	return &do, err
}

// Writes the rows provided (filtering out nils) and returns the AddDataobject action for the created file. Callers are
// responsible for putting that action into the transaction.
// For most purposes, txId can be the current transaction ID (i.e. d.tx.Id), however in some cases (such as
// copy-on-write, the caller provides a different value).
func (d *DeltaLakeClient) writeDataObject(table string, rows *[DATAOBJECT_SIZE][]any, txId int) (Action, error) {
	// We filter here because of deletes using nils as tombstones in the unflushed data.
	var filteredRows [DATAOBJECT_SIZE][]any
	filteredRowsPointer := 0
	for _, row := range rows {
		if row != nil {
			filteredRows[filteredRowsPointer] = row
			filteredRowsPointer++
		}
	}

	newDataobject := dataobjectT{
		Table: table,
		Name:  uuid.New().String(),
		Data:  filteredRows,
		Len:   filteredRowsPointer,
	}

	serialisedbytes, err := json.Marshal(newDataobject)
	if err != nil {
		return Action{}, err
	}

	filename := fmt.Sprintf("_table_%s_%s", table, newDataobject.Name)
	err = d.os.PutIfAbsent(filename, serialisedbytes)
	if err != nil {
		return Action{}, err
	}

	return Action{
		AddDataobject: &dataobjectActionT{
			Name: newDataobject.Name, Table: table, TxId: txId,
		},
	}, nil
}

// For a given table, lists all dataobjects that have not been deleted.
// This will return the dataobjects in chronological order.
// The returned slice contains dataobjectActions for all adds that have not been deleted.
func (d *DeltaLakeClient) listExtantDataobjects(table string) []*dataobjectActionT {
	allActions := append(d.tx.previousActions[table], d.tx.Actions[table]...)

	deletedDataobjectsSet := make(map[string]struct{})
	for _, action := range allActions {
		if action.DeleteDataobject != nil {
			deletedDataobjectsSet[action.DeleteDataobject.Name] = struct{}{}
		}
	}

	var extantDataobjects []*dataobjectActionT
	for _, action := range allActions {
		if action.AddDataobject != nil {
			if _, deleted := deletedDataobjectsSet[action.AddDataobject.Name]; !deleted {
				extantDataobjects = append(extantDataobjects, action.AddDataobject)
			}
		}
	}

	// Sort by the TxId. See note on that field for why.
	sort.Slice(extantDataobjects, func(i, j int) bool { return extantDataobjects[i].TxId < extantDataobjects[j].TxId })
	return extantDataobjects
}
