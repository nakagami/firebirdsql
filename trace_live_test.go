package firebirdsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// The test creates only its own database and trace session. Explicit opt-in is
// needed because Trace requires server-side privileges and exposes raw SQL.
func TestTraceLiveLifecycle(t *testing.T) {
	if os.Getenv("FIREBIRDSQL_TRACE_TEST") != "1" {
		t.Skip("set FIREBIRDSQL_TRACE_TEST=1 on an isolated test server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	svc, err := NewServiceManagerContext(ctx, testServerAddr(), GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	if err != nil {
		t.Fatal(err)
	}
	version, err := svc.GetServerVersion()
	if err != nil {
		t.Fatal(err)
	}
	_ = svc.Close()
	t.Logf("server: %s", version.Full)
	db, err := sql.Open("firebirdsql_createdb", GetTestDSN("trace_live_"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var profilerPackages int
	if version.Major >= 3 {
		err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM RDB$PACKAGES WHERE RDB$PACKAGE_NAME = 'RDB$PROFILER'").Scan(&profilerPackages)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("profiler package count: %d", profilerPackages)
		if version.Major < 5 && profilerPackages != 0 {
			t.Fatal("unexpected profiler package on legacy server")
		}
	}
	cfg := "database {\n enabled = true\n log_statement_start = true\n log_statement_finish = true\n time_threshold = 0\n}\n"
	if version.Major < 3 {
		cfg = "<database>\n enabled true\n log_statement_start true\n log_statement_finish true\n time_threshold 0\n</database>\n"
	}
	manager, _ := NewTraceManager(testServerAddr(), GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	session, err := manager.StartWithNameContext(ctx, "firebirdsql-lifecycle-test", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	listed, err := manager.ListContext(ctx)
	if err != nil || !strings.Contains(listed, fmt.Sprintf("Session ID: %d", session.ID())) {
		t.Fatalf("list: %q %v", listed, err)
	}
	if err = session.PauseContext(ctx); err != nil {
		t.Fatal(err)
	}
	if err = session.ResumeContext(ctx); err != nil {
		t.Fatal(err)
	}
	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()
	lines := make(chan string, 16)
	done := make(chan error, 1)
	go func() { done <- session.WaitStringsContext(streamCtx, lines) }()
	var n int
	if err = conn.QueryRowContext(ctx, "SELECT 123456789 FROM RDB$DATABASE").Scan(&n); err != nil || n != 123456789 {
		t.Fatalf("query: %d %v", n, err)
	}
	found := false
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for !found {
		select {
		case line := <-lines:
			found = strings.Contains(line, "SELECT 123456789")
		case err = <-done:
			t.Fatalf("stream exited early: %v", err)
		case <-timer.C:
			t.Fatal("trace statement not received")
		}
	}
	// Stop uses another connection while the original stream reader is active.
	if err = session.StopContext(ctx); err != nil {
		t.Fatal(err)
	}
	if err = session.StopContext(ctx); err != nil {
		t.Fatal(err)
	}
	stopStream()
	select {
	case err = <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader did not exit")
	}
	if err = session.Close(); err != nil {
		t.Fatal(err)
	}
	if err = session.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.ExecContext(ctx, "CREATE TABLE TRACE_BUSINESS (ID INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.ExecContext(ctx, "INSERT INTO TRACE_BUSINESS VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM TRACE_BUSINESS").Scan(&n); err != nil || n != 1 {
		t.Fatalf("ordinary DML after trace: %d %v", n, err)
	}
}
