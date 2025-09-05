package deltalakeclient

type scanIterator struct {
	d     *DeltaLakeClient
	table string

	// First we iterate through unflushed rows.
	unflushedRows       [DATAOBJECT_SIZE][]any
	unflushedRowsLen    int
	unflushedRowPointer int

	// Then we move through each dataobject.
	allDataobjects        []string // Just the names, we fetch next one on calling next()
	allDataobjectsPointer int

	// And within each currentDataobject we iterate through rows.
	currentDataobject        *dataobjectT // Content of current dataobject
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
	extantDataobjects := d.listExtantDataobjects(table)
	extantDataobjectNames := make([]string, len(extantDataobjects))
	for i, obj := range extantDataobjects {
		extantDataobjectNames[i] = obj.Name
	}

	return &scanIterator{
		d:                d,
		table:            table,
		unflushedRows:    unflushedRows,
		unflushedRowsLen: unflushedRowsLen,
		// To be reverse-chronological, we need to iterate backwards on unflushed data.
		unflushedRowPointer:   unflushedRowsLen - 1,
		allDataobjects:        extantDataobjectNames,
		allDataobjectsPointer: len(extantDataobjects) - 1,
	}, nil
}

// Iterates over the rows, in reverse-chronological order (i.e. latest version of rows will appear first).
func (si *scanIterator) Next() ([]any, error) {
	// Unflushed rows first
	// We have to loop here to find first non-nil row, as DeleteRows tombstones them to nil. We are also iterating
	// backwards, as mentioned above.
	for si.unflushedRowPointer >= 0 {
		row := si.unflushedRows[si.unflushedRowPointer]
		si.unflushedRowPointer--
		if row != nil {
			return row, nil
		}
	}

	// Then flushed rows

	// If we are done with dataobjects, we're done overall.
	if si.allDataobjectsPointer < 0 {
		return nil, nil
	}

	if si.currentDataobject == nil {
		object, err := si.d.readDataobject(si.table, si.allDataobjects[si.allDataobjectsPointer])
		if err != nil {
			return nil, err
		}
		si.currentDataobject = object
		si.currentDataObjectPointer = object.Len - 1
	}

	// Similar reason for for-loop as above.
	for {
		// If we have reached the end of the current dataobject, recurse to get next.
		if si.currentDataObjectPointer < 0 {
			si.currentDataobject = nil
			si.allDataobjectsPointer--
			return si.Next()
		}

		row := si.currentDataobject.Data[si.currentDataObjectPointer]
		si.currentDataObjectPointer--
		if row != nil {
			return row, nil
		}
	}
}
