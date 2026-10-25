//go:build go1.27

package firebirdsql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
)

var _ driver.RowsColumnScanner = (*firebirdsqlRows)(nil)

func (rows *firebirdsqlRows) NextRow() error {
	if rows.scanRow == nil {
		rows.scanRow = make([]driver.Value, len(rows.stmt.resultXsqlda))
	}
	if err := rows.Next(rows.scanRow); err != nil {
		rows.scanRow = nil
		return err
	}
	return nil
}

func (rows *firebirdsqlRows) ScanColumn(scanCtx driver.ScanContext, index int, dest any) error {
	if index < 0 || index >= len(rows.scanRow) {
		return fmt.Errorf("firebirdsql: column index %d out of range [0, %d)", index, len(rows.scanRow))
	}
	value := rows.scanRow[index]
	// Exact scalar matches avoid reflection. Leave conversions, NULLs, typed nil
	// destinations, byte ownership and custom Scanners to database/sql.
	switch d := dest.(type) {
	case *int:
		if v, ok := value.(int64); ok && d != nil && int64(int(v)) == v {
			*d = int(v)
			return nil
		}
	case *int32:
		if v, ok := value.(int64); ok && d != nil && int64(int32(v)) == v {
			*d = int32(v)
			return nil
		}
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
