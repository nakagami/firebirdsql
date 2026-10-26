package firebirdsql

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"testing"
)

func TestReadOnlySnapshotTPB(t *testing.T) {
	for _, tc := range []struct {
		name      string
		isolation sql.IsolationLevel
		readOnly  bool
		want      []byte
	}{
		{"snapshot_read_only", sql.LevelRepeatableRead, true, []byte{isc_tpb_version3, isc_tpb_read, isc_tpb_wait, isc_tpb_concurrency}},
		{"snapshot_read_write", sql.LevelRepeatableRead, false, []byte{isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_concurrency}},
		{"default_read_only", sql.LevelDefault, true, []byte{isc_tpb_version3, isc_tpb_read, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_rec_version}},
		{"committed_read_only", sql.LevelReadCommitted, true, []byte{isc_tpb_version3, isc_tpb_read, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_rec_version}},
		{"committed_read_write", sql.LevelReadCommitted, false, []byte{isc_tpb_version3, isc_tpb_write, isc_tpb_wait, isc_tpb_read_committed, isc_tpb_rec_version}},
		{"nowait_read_only", LevelReadCommittedNoWait, true, []byte{isc_tpb_version3, isc_tpb_read, isc_tpb_nowait, isc_tpb_read_committed, isc_tpb_rec_version}},
		{"nowait_read_write", LevelReadCommittedNoWait, false, []byte{isc_tpb_version3, isc_tpb_write, isc_tpb_nowait, isc_tpb_read_committed, isc_tpb_rec_version}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			level, err := txIsolationLevel(driver.TxOptions{Isolation: driver.IsolationLevel(tc.isolation), ReadOnly: tc.readOnly})
			if err != nil {
				t.Fatal(err)
			}
			got, err := tpbForIsolationLevel(level)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("TPB=%v, want %v", got, tc.want)
			}
		})
	}
}
