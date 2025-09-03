package deltalakeclient

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

	return &scanIterator{
		d:                d,
		table:            table,
		unflushedRows:    unflushedRows,
		unflushedRowsLen: unflushedRowsLen,
		dataobjects:      extantDataobjects,
	}, nil
}

func (si *scanIterator) Next() ([]any, error) {
	// Unflushed rows first
	// We have to loop here to find first non-nil row, as DeleteRows tombstones
	// them to nil.
	for si.unflushedRowPointer < si.unflushedRowsLen {
		row := si.unflushedRows[si.unflushedRowPointer]
		si.unflushedRowPointer++
		if row != nil {
			return row, nil
		}
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

	// Similar reason for for-loop as above.
	for {
		// If we have reached the end of the current dataobject, recurse to get next.
		if si.currentDataObjectPointer > si.currentDataobject.Len {
			si.currentDataobject = nil
			si.dataobjectsPointer++
			si.currentDataObjectPointer = 0

			return si.Next()
		}

		row := si.currentDataobject.Data[si.currentDataObjectPointer]
		si.currentDataObjectPointer++
		if row != nil {
			return row, nil
		}
	}
}
