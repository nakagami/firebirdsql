//go:build go1.27

package firebirdsql

import (
	"database/sql"
	"database/sql/driver"
)

var _ driver.RowsColumnScanner = (*firebirdsqlRows)(nil)

func (rows *firebirdsqlRows) NextRow() error {
	rows.scanRow = nil
	if !rows.scanInitialized {
		rows.scanInitialized = true
		for _, x := range rows.stmt.resultXsqlda {
			if x.sqltype == SQL_TYPE_BLOB || x.sqltype == SQL_TYPE_ARRAY {
				// Keep large values out of currentChunk: retaining materialized
				// BLOBs/arrays for every consumed row would grow memory usage.
				rows.scanBuffer = make([]driver.Value, len(rows.stmt.resultXsqlda))
				break
			}
		}
	}
	if err := rows.next(rows.scanBuffer); err != nil {
		rows.scanRow = nil
		return err
	}
	if rows.scanBuffer != nil {
		rows.scanRow = rows.scanBuffer
	}
	return nil
}

func (rows *firebirdsqlRows) ScanColumn(scanCtx driver.ScanContext, index int, dest any) error {
	value := rows.scanRow[index]
	// Exact scalar matches avoid reflection. Leave conversions, NULLs, typed nil
	// destinations, byte ownership and custom Scanners to database/sql.
	switch d := dest.(type) {
	case *int64:
		if v, ok := value.(int64); ok && d != nil {
			*d = v
			return nil
		}
	case *float64:
		if v, ok := value.(float64); ok && d != nil {
			*d = v
			return nil
		}
	case *bool:
		if v, ok := value.(bool); ok && d != nil {
			*d = v
			return nil
		}
	}
	return sql.ConvertAssign(scanCtx, dest, value)
}
