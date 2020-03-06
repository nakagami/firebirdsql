package firebirdsql

import (
	"context"
)

// This file implements the optional pinger interface for the database/sql package
func (fc *firebirdsqlConn) Ping(ctx context.Context) (err error) {
	_, err = fc.Exec("select 1 from RDB$RELATION_FIELDS rows 1", nil)
	return
}
