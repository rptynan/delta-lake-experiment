package deltalakeclient

import "slices"

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
		ChangeMetadata: &changeMetadataAction{
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

type QueryRange struct {
	// Inclusive
	Start any
	End   any
}

// Because of JSON, all our numbers come back as floats. We are just going to assume everything is an int for now.
func asInt(x any) (int, error) {
	switch v := x.(type) {
	case int:
		return v, nil
	case float64:
		return int(v), nil
	default:
		return 0, errTypeMismatch
	}
}

func inRange(columnIndex int, queryRange QueryRange, row []any) (bool, error) {
	value := row[columnIndex]

	switch start := queryRange.Start.(type) {
	case int:
		end, ok1 := queryRange.End.(int)
		if !ok1 {
			return false, errTypeMismatch
		}
		val, err := asInt(value)
		if err != nil {
			return false, err
		}
		return start <= val && val <= end, nil
	case string:
		end, ok1 := queryRange.End.(string)
		val, ok2 := value.(string)
		if !ok1 || !ok2 {
			return false, errTypeMismatch
		}
		return start <= val && val <= end, nil
	default:
		return false, errTypeMismatch
	}
}

func (d *DeltaLakeClient) DeleteRows(table string, column string, queryRange QueryRange) error {
	if d.tx == nil {
		return errExistingTx
	}

	columnIndex := slices.Index(d.tx.tables[table], column)
	if columnIndex == -1 {
		return errNoTable
	}

	// Unflushed data
	for i := 0; i < d.tx.unflushedDataPointer[table]; i++ {
		r, err := inRange(columnIndex, queryRange, d.tx.unflushedData[table][i])
		if err != nil {
			return err
		}
		if r {
			// Tombstone unflushed rows
			d.tx.unflushedData[table][i] = nil
		}
	}

	// Flushed data
	// We are doing copy-on-write, so we find any dataobjects that have matching
	// rows, mark them as deleted and then rewrite those objects without said rows.
	extantDataobjects := d.listExtantDataobjects(table)

	var filteredRows [DATAOBJECT_SIZE][]any
	for _, dataobject := range extantDataobjects {
		filteredRowsPointer := 0

		dataobject, err := d.readDataobject(table, dataobject)
		if err != nil {
			return err
		}

		for i := 0; i < dataobject.Len; i++ {
			row := dataobject.Data[i]

			r, err := inRange(columnIndex, queryRange, row)
			if err != nil {
				return err
			}
			if !r {
				filteredRows[filteredRowsPointer] = row
				filteredRowsPointer++
			}
		}

		// If this is true, we know we have filtered out some rows, so we need to delete the old dataobject and write a new
		// one with the contents of our filtered rows array.
		if filteredRowsPointer != dataobject.Len {
			addDataobjectAction, err := d.writeDataObject(table, &filteredRows)
			if err != nil {
				return err
			}

			d.tx.Actions[table] = append(d.tx.Actions[table],
				addDataobjectAction,
				Action{
					DeleteDataobject: &dataobjectAction{
						Name: dataobject.Name, Table: table,
					},
				},
			)
		}
	}

	return nil
}
