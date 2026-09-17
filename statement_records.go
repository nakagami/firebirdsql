package firebirdsql

import (
	"encoding/binary"
	"errors"
	"math"
)

var errStatementRecords = errors.New("firebirdsql: invalid statement record counts")
var errStatementRecordsOverflow = errors.New("firebirdsql: statement record count overflow")

// statementRecords contains only counts supplied by isc_info_sql_records.
// They describe the outer statement, not all trigger/procedure modifications.
// DDL may omit the records item entirely; available distinguishes that from zero.
type statementRecords struct {
	selected, inserted, updated, deleted int64
	available                            bool
}

// nextStatementInfo reads the common tag/unsigned-16-bit-length/value encoding.
// Structural markers are single bytes. Bytes following isc_info_end may be
// unused response-buffer padding and are not interpreted as additional items.
func nextStatementInfo(buf []byte) (tag byte, value, rest []byte, err error) {
	if len(buf) == 0 {
		return 0, nil, nil, errStatementRecords
	}
	tag = buf[0]
	if tag == isc_info_end {
		return tag, nil, nil, nil
	}
	if tag == isc_info_truncated || tag == isc_info_error || len(buf) < 3 {
		return 0, nil, nil, errStatementRecords
	}
	size := int(binary.LittleEndian.Uint16(buf[1:3]))
	if size > len(buf)-3 {
		return 0, nil, nil, errStatementRecords
	}
	return tag, buf[3 : 3+size], buf[3+size:], nil
}

func decodeStatementRecords(buf []byte) (records statementRecords, err error) {
	for {
		tag, value, rest, e := nextStatementInfo(buf)
		if e != nil {
			return statementRecords{}, e
		}
		if tag == isc_info_end {
			return records, nil
		}
		if tag == isc_info_sql_records {
			if records.available {
				return statementRecords{}, errStatementRecords
			}
			records, e = decodeRecordItems(value)
			if e != nil {
				return statementRecords{}, e
			}
		}
		buf = rest
	}
}

func decodeRecordItems(buf []byte) (records statementRecords, err error) {
	var seen byte
	for {
		tag, value, rest, e := nextStatementInfo(buf)
		if e != nil {
			return statementRecords{}, e
		}
		if tag == isc_info_end {
			if seen != 15 {
				return statementRecords{}, errStatementRecords
			}
			records.available = true
			return records, nil
		}
		if tag >= isc_info_req_select_count && tag <= isc_info_req_delete_count {
			bit := byte(1 << (tag - isc_info_req_select_count))
			if seen&bit != 0 {
				return statementRecords{}, errStatementRecords
			}
			seen |= bit
			// INF_convert uses signed little-endian SLONG or SINT64. Values above
			// MAX_SLONG are encoded in eight bytes, not truncated to four bytes.
			var count int64
			switch len(value) {
			case 4:
				count = int64(int32(binary.LittleEndian.Uint32(value)))
			case 8:
				count = int64(binary.LittleEndian.Uint64(value))
			default:
				return statementRecords{}, errStatementRecords
			}
			if count < 0 {
				return statementRecords{}, errStatementRecordsOverflow
			}
			switch tag {
			case isc_info_req_select_count:
				records.selected = count
			case isc_info_req_insert_count:
				records.inserted = count
			case isc_info_req_update_count:
				records.updated = count
			case isc_info_req_delete_count:
				records.deleted = count
			}
		}
		buf = rest
	}
}

func (r statementRecords) rowsAffected(stmtType int32) (int64, error) {
	if stmtType == isc_info_sql_stmt_select || stmtType == isc_info_sql_stmt_select_for_upd {
		return r.selected, nil
	}
	if r.inserted > math.MaxInt64-r.updated {
		return 0, errStatementRecordsOverflow
	}
	count := r.inserted + r.updated
	if count > math.MaxInt64-r.deleted {
		return 0, errStatementRecordsOverflow
	}
	return count + r.deleted, nil
}
