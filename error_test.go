package firebirdsql

import (
	"database/sql"
	"errors"
	"slices"
	"testing"
)

func TestFbErrorCodes(t *testing.T) {
	// Create test database
	dsn := GetTestDSN("test_error_codes_")
	conn, err := sql.Open("firebirdsql_createdb", dsn)
	if err != nil {
		t.Fatalf("Error creating database: %v", err)
	}
	defer conn.Close()

	// 1. Create parent and child tables with constraints
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
			t.Fatalf("Error executing setup query: %v", err)
		}
	}

	// Insert one valid row into parent for FK tests
	if _, err := conn.Exec("INSERT INTO parent (id, uniq_col) VALUES (1, 'unique')"); err != nil {
		t.Fatalf("Error inserting initial data: %v", err)
	}

	// 2. Test cases for different types of errors
	tests := []struct {
		name        string
		query       string
		wantGdsCode int
	}{
		{
			name:        "Primary Key Violation (Parent)",
			query:       "INSERT INTO parent (id, uniq_col) VALUES (1, 'another')",
			wantGdsCode: 335544665, // violation of PRIMARY or UNIQUE KEY constraint
		},
		{
			name:        "Unique Key Violation",
			query:       "INSERT INTO parent (id, uniq_col) VALUES (2, 'unique')",
			wantGdsCode: 335544665, // violation of PRIMARY or UNIQUE KEY constraint
		},
		{
			name:        "Foreign Key Violation",
			query:       "INSERT INTO child (id, parent_id, check_col) VALUES (1, 999, 10)",
			wantGdsCode: 335544466, // violation of FOREIGN KEY constraint
		},
		{
			name:        "Check Constraint Violation",
			query:       "INSERT INTO child (id, parent_id, check_col) VALUES (2, 1, -5)",
			wantGdsCode: 335544558, // Operation violates CHECK constraint
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := conn.Exec(tt.query)
			if err == nil {
				t.Fatal("Expected error, got nil")
			}

			// Check error type and codes
			var fbErr *FbError
			if errors.As(err, &fbErr) {
				if !slices.Contains(fbErr.GDSCodes, tt.wantGdsCode) {
					t.Errorf("Expected GdsCode %d to be in %v", tt.wantGdsCode, fbErr.GDSCodes)
				}
			} else {
				t.Errorf("Expected error to be of type *FbError, got %T: %v", err, err)
			}
		})
	}
}
