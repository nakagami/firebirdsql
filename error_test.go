package firebirdsql

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"slices"
	"testing"
)

// ---------------------------------------------------------------------------
// Unit tests — no live Firebird server required
// ---------------------------------------------------------------------------

// testProtocol returns a wireProtocol whose reader is backed by the provided bytes.
func testProtocol(data []byte) *wireProtocol {
	p := &wireProtocol{}
	p.conn.reader = bufio.NewReader(bytes.NewReader(data))
	return p
}

// statusBuf is a helper to build synthetic Firebird status-vector byte sequences.
type statusBuf struct{ buf bytes.Buffer }

func (s *statusBuf) tag(t int32)  { s.int32(t) }
func (s *statusBuf) end()         { s.tag(isc_arg_end) }
func (s *statusBuf) int32(v int32) {
	_ = binary.Write(&s.buf, binary.BigEndian, v)
}
func (s *statusBuf) gds(code int) { s.tag(isc_arg_gds); s.int32(int32(code)) }
func (s *statusBuf) warning(code int) { s.tag(isc_arg_warning); s.int32(int32(code)) }
func (s *statusBuf) str(v string) {
	s.tag(isc_arg_string)
	s.int32(int32(len(v)))
	s.buf.WriteString(v)
	if pad := (4 - len(v)%4) % 4; pad > 0 {
		s.buf.Write(make([]byte, pad))
	}
}
func (s *statusBuf) sqlState(st string) {
	s.tag(isc_arg_sql_state)
	s.int32(int32(len(st)))
	s.buf.WriteString(st)
	if pad := (4 - len(st)%4) % 4; pad > 0 {
		s.buf.Write(make([]byte, pad))
	}
}
func (s *statusBuf) bytes() []byte { return s.buf.Bytes() }

func TestParseStatusVector_Params(t *testing.T) {
	var sb statusBuf
	// Unique key violation: GDS code with two string params + SQLSTATE
	sb.gds(335544665) // isc_unique_key_violation
	sb.str("INTEG_1") // @1 constraint name
	sb.str("T")       // @2 table name
	sb.sqlState("23000")
	sb.end()

	sv, err := testProtocol(sb.bytes())._parse_status_vector()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !slices.Equal(sv.gdsCodes, []int{335544665}) {
		t.Errorf("gdsCodes: got %v, want [335544665]", sv.gdsCodes)
	}
	if sv.sqlState != "23000" {
		t.Errorf("sqlState: got %q, want %q", sv.sqlState, "23000")
	}
	if len(sv.params) == 0 || !slices.Equal(sv.params[0], []string{"INTEG_1", "T"}) {
		t.Errorf("params[0]: got %v, want [INTEG_1 T]", sv.params)
	}
	if !bytes.Contains([]byte(sv.message), []byte("INTEG_1")) {
		t.Errorf("message should contain INTEG_1, got: %q", sv.message)
	}
}

func TestParseStatusVector_Warning(t *testing.T) {
	var sb statusBuf
	// Warning followed by a real error — verifies no stream desync
	sb.warning(335544788) // arbitrary warning code
	sb.gds(335544336)     // deadlock — the actual error
	sb.end()

	sv, err := testProtocol(sb.bytes())._parse_status_vector()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !slices.Equal(sv.warnings, []int{335544788}) {
		t.Errorf("warnings: got %v, want [335544788]", sv.warnings)
	}
	if !slices.Equal(sv.gdsCodes, []int{335544336}) {
		t.Errorf("gdsCodes: got %v, want [335544336]", sv.gdsCodes)
	}
}

func TestParseStatusVector_MultiChain(t *testing.T) {
	var sb statusBuf
	// Two chained GDS codes, each with their own params — verifies per-code Params alignment
	sb.gds(335544665) // code A
	sb.str("CON_A")  // @1 for A
	sb.gds(335544349) // code B (follow-up detail)
	sb.str("val=X")  // @1 for B
	sb.end()

	sv, err := testProtocol(sb.bytes())._parse_status_vector()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sv.gdsCodes) != 2 {
		t.Fatalf("gdsCodes: got %v, want 2 entries", sv.gdsCodes)
	}
	if !slices.Equal(sv.params[0], []string{"CON_A"}) {
		t.Errorf("params[0]: got %v, want [CON_A]", sv.params[0])
	}
	if !slices.Equal(sv.params[1], []string{"val=X"}) {
		t.Errorf("params[1]: got %v, want [val=X]", sv.params[1])
	}
}

func TestParseStatusVector_SQLCodeFallback(t *testing.T) {
	var sb statusBuf
	// Minimal vector with no isc_arg_sql_state and no isc_sqlerr — both values must come from fallback maps
	sb.gds(335544665) // isc_unique_key_violation: sqlcode=-803, sqlstate="23000" in maps
	sb.end()

	sv, err := testProtocol(sb.bytes())._parse_status_vector()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate _parse_op_response fallback logic
	sqlState := sv.sqlState
	if sqlState == "" && len(sv.gdsCodes) > 0 {
		sqlState = gdsToSQLState(sv.gdsCodes[0])
	}
	sqlCode := sv.sqlCode
	if sqlCode == 0 && len(sv.gdsCodes) > 0 {
		sqlCode = gdsToSQLCode(sv.gdsCodes[0])
	}
	if sqlState != "23000" {
		t.Errorf("fallback sqlState: got %q, want %q", sqlState, "23000")
	}
	if sqlCode != -803 {
		t.Errorf("fallback sqlCode: got %d, want -803", sqlCode)
	}
}

// ---------------------------------------------------------------------------
// Integration tests — require a live Firebird server
// ---------------------------------------------------------------------------

func TestFbErrorCodes(t *testing.T) {
	dsn := GetTestDSN("test_error_codes_")
	conn, err := sql.Open("firebirdsql_createdb", dsn)
	if err != nil {
		t.Fatalf("Error creating database: %v", err)
	}
	defer conn.Close()

	setupQueries := []string{
		`CREATE TABLE parent (
			id INTEGER NOT NULL PRIMARY KEY,
			uniq_col VARCHAR(20) UNIQUE
		)`,
		`CREATE TABLE child (
			id INTEGER NOT NULL PRIMARY KEY,
			parent_id INTEGER,
			check_col INTEGER,
			CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES parent(id),
			CONSTRAINT chk_positive CHECK (check_col > 0)
		)`,
	}
	for _, q := range setupQueries {
		if _, err := conn.Exec(q); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	if _, err := conn.Exec("INSERT INTO parent (id, uniq_col) VALUES (1, 'unique')"); err != nil {
		t.Fatalf("insert seed: %v", err)
	}

	tests := []struct {
		name         string
		query        string
		wantGdsCode  int
		wantSQLCode  int32
		wantSQLState string
	}{
		{
			name:         "Primary Key Violation",
			query:        "INSERT INTO parent (id, uniq_col) VALUES (1, 'another')",
			wantGdsCode:  335544665,
			wantSQLCode:  -803,
			wantSQLState: "23000",
		},
		{
			name:         "Unique Key Violation",
			query:        "INSERT INTO parent (id, uniq_col) VALUES (2, 'unique')",
			wantGdsCode:  335544665,
			wantSQLCode:  -803,
			wantSQLState: "23000",
		},
		{
			name:         "Foreign Key Violation",
			query:        "INSERT INTO child (id, parent_id, check_col) VALUES (1, 999, 10)",
			wantGdsCode:  335544466,
			wantSQLCode:  -530,
			wantSQLState: "23000",
		},
		{
			name:         "Check Constraint Violation",
			query:        "INSERT INTO child (id, parent_id, check_col) VALUES (2, 1, -5)",
			wantGdsCode:  335544558,
			wantSQLCode:  -297,
			wantSQLState: "23000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, execErr := conn.Exec(tt.query)
			if execErr == nil {
				t.Fatal("expected error, got nil")
			}

			var fbErr *FbError
			if !errors.As(execErr, &fbErr) {
				t.Fatalf("expected *FbError, got %T: %v", execErr, execErr)
			}

			// GDSCodes backward-compat assertion
			if !slices.Contains(fbErr.GDSCodes, tt.wantGdsCode) {
				t.Errorf("GDSCodes %v does not contain %d", fbErr.GDSCodes, tt.wantGdsCode)
			}
			// New enriched fields
			if fbErr.SQLCode != tt.wantSQLCode {
				t.Errorf("SQLCode: got %d, want %d", fbErr.SQLCode, tt.wantSQLCode)
			}
			if fbErr.SQLState != tt.wantSQLState {
				t.Errorf("SQLState: got %q, want %q", fbErr.SQLState, tt.wantSQLState)
			}
			// Params: at least the primary GDS code should have params
			if len(fbErr.Params) == 0 {
				t.Errorf("Params: expected at least one param set, got none")
			}
		})
	}
}
