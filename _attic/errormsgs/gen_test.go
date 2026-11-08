package main

import (
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// goldenRev is a fixed commit, so golden output does not depend on upstream.
const goldenRev = "0123456789abcdef0123456789abcdef01234567"

var update = flag.Bool("update", false, "update golden files")

func TestGolden(t *testing.T) {
	outDir := t.TempDir()

	// srcDir points at testdata/; readContent will look under testdata/firebird/impl/
	if err := run("testdata", defaultBaseURL, goldenRev, outDir); err != nil {
		t.Fatalf("run: %v", err)
	}

	goldenDir := filepath.Join("testdata", "golden")
	for _, name := range []string{"errmsgs.go", "fberrcode.go", "sqlstate_map.go", "sqlcode_map.go"} {
		got, err := os.ReadFile(filepath.Join(outDir, name))
		if err != nil {
			t.Fatalf("read output %s: %v", name, err)
		}
		goldenPath := filepath.Join(goldenDir, name)
		if *update {
			if err := os.MkdirAll(goldenDir, 0o755); err != nil {
				t.Fatalf("mkdir golden: %v", err)
			}
			if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
				t.Fatalf("write golden %s: %v", goldenPath, err)
			}
			t.Logf("updated %s", goldenPath)
			continue
		}
		want, err := os.ReadFile(goldenPath)
		if err != nil {
			t.Fatalf("read golden %s: %v\n(run with -update to create it)", goldenPath, err)
		}
		if string(got) != string(want) {
			gotLines := strings.Split(string(got), "\n")
			wantLines := strings.Split(string(want), "\n")
			for i := 0; i < len(gotLines) && i < len(wantLines); i++ {
				if gotLines[i] != wantLines[i] {
					t.Errorf("%s: first diff at line %d:\n  got:  %q\n  want: %q",
						name, i+1, gotLines[i], wantLines[i])
					return
				}
			}
			t.Errorf("%s: length differs: got %d lines, want %d lines",
				name, len(gotLines), len(wantLines))
		}
	}
}

func TestParseEntriesWarnings(t *testing.T) {
	cases := []struct {
		name        string
		line        string
		wantEntries int
		wantWarning string
	}{
		{"valid", `FB_IMPL_MSG(TEST, 2, sym, -900, "HY", "000", "text")`, 1, ""},
		{"valid no symbol", `FB_IMPL_MSG_NO_SYMBOL(TEST, 1, "text")`, 1, ""},
		{"empty text", `FB_IMPL_MSG_NO_SYMBOL(TEST, 0, "")`, 0, ""},
		{"empty text with symbol", `FB_IMPL_MSG(TEST, 3, sym, -900, "HY", "000", "")`, 0, ""},
		{"commented out", `// FB_IMPL_MSG_NEW(TEST, 4, "text")`, 0, ""},
		{"too few arguments", `FB_IMPL_MSG(TEST, 5, sym, -900)`, 0, "want 7 macro arguments, got 4"},
		{"multi-line macro", `FB_IMPL_MSG(TEST, 6, sym, -900, "HY", "000",`, 0, "want 7 macro arguments, got 6"},
		{"bad SQLCODE", `FB_IMPL_MSG(TEST, 7, sym, oops, "HY", "000", "text")`, 0, "bad SQLCODE"},
		{"unrecognized macro", `FB_IMPL_MSG_WARN(TEST, 8, sym, "text")`, 0, "unrecognized macro"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			entries, warnings := parseEntries(c.line, "msg/test.h", "TEST", 11)
			if len(entries) != c.wantEntries {
				t.Errorf("got %d entries, want %d", len(entries), c.wantEntries)
			}
			if c.wantWarning == "" {
				if len(warnings) != 0 {
					t.Errorf("unexpected warnings: %q", warnings)
				}
				return
			}
			if len(warnings) != 1 || !strings.Contains(warnings[0], c.wantWarning) || !strings.HasPrefix(warnings[0], "msg/test.h:1: ") {
				t.Errorf("warnings = %q, want one at msg/test.h:1 containing %q", warnings, c.wantWarning)
			}
		})
	}
}

// No case here reaches the network: an empty rev with the default source is the
// only path that looks a commit up.
func TestResolveRevision(t *testing.T) {
	const customURL = "https://example.com/impl"
	pinnedURL := fmt.Sprintf(pinnedBaseURL, goldenRev)
	cases := []struct {
		name            string
		srcDir, baseURL string
		rev             string
		wantURL         string
		wantRev         string
		wantErr         bool
	}{
		{name: "default url pins to rev", baseURL: defaultBaseURL, rev: goldenRev, wantURL: pinnedURL, wantRev: goldenRev},
		{name: "default url rejects unknown", baseURL: defaultBaseURL, rev: unknownRevision, wantErr: true},
		{name: "src without rev", srcDir: "src", baseURL: defaultBaseURL, wantURL: defaultBaseURL, wantRev: unknownRevision},
		{name: "src with rev", srcDir: "src", baseURL: defaultBaseURL, rev: goldenRev, wantURL: defaultBaseURL, wantRev: goldenRev},
		{name: "url without rev", baseURL: customURL, wantURL: customURL, wantRev: unknownRevision},
		{name: "url with rev", baseURL: customURL, rev: goldenRev, wantURL: customURL, wantRev: goldenRev},
		{name: "url with unknown", baseURL: customURL, rev: unknownRevision, wantURL: customURL, wantRev: unknownRevision},
		{name: "short sha", srcDir: "src", baseURL: defaultBaseURL, rev: goldenRev[:7], wantErr: true},
		{name: "branch name", baseURL: customURL, rev: "master", wantErr: true},
		{name: "uppercase sha", srcDir: "src", baseURL: defaultBaseURL, rev: strings.ToUpper(goldenRev), wantErr: true},
		{name: "newline", srcDir: "src", baseURL: defaultBaseURL, rev: goldenRev + "\nfunc init() {}", wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotURL, gotRev, err := resolveRevision(c.srcDir, c.baseURL, c.rev)
			if c.wantErr {
				if err == nil {
					t.Fatalf("got url %q rev %q, want an error", gotURL, gotRev)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if gotURL != c.wantURL || gotRev != c.wantRev {
				t.Errorf("got url %q rev %q, want url %q rev %q", gotURL, gotRev, c.wantURL, c.wantRev)
			}
		})
	}
}

func TestLookupCommit(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		wantErr bool
	}{
		{name: "bare sha", status: http.StatusOK, body: goldenRev + "\n"},
		{name: "rate limited", status: http.StatusForbidden, body: "rate limit", wantErr: true},
		{name: "json body", status: http.StatusOK, body: `{"sha":"` + goldenRev + `"}`, wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("Accept"); got != "application/vnd.github.sha" {
					t.Errorf("Accept = %q", got)
				}
				w.WriteHeader(c.status)
				fmt.Fprint(w, c.body)
			}))
			defer srv.Close()
			got, err := lookupCommit(srv.URL)
			if c.wantErr {
				if err == nil {
					t.Fatalf("got %q, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != goldenRev {
				t.Errorf("got %q, want %q", got, goldenRev)
			}
		})
	}
}

// A symbol that is not a valid Go identifier must stop the run before any existing
// file is overwritten.
func TestRunInvalidOutputKeepsExistingFiles(t *testing.T) {
	srcDir := t.TempDir()
	implDir := filepath.Join(srcDir, "firebird", "impl")
	if err := os.MkdirAll(filepath.Join(implDir, "msg"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"msg_helper.h": "#define FB_IMPL_MSG_FACILITY_TEST 11\n",
		"msg/all.h":    "#include \"test.h\"\n",
		"msg/test.h":   "FB_IMPL_MSG_SYMBOL(TEST, 1, bad$symbol, \"text\")\n",
	} {
		if err := os.WriteFile(filepath.Join(implDir, filepath.FromSlash(name)), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	outDir := t.TempDir()
	const existing = "existing content"
	names := []string{"errmsgs.go", "fberrcode.go", "sqlstate_map.go", "sqlcode_map.go"}
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(outDir, name), []byte(existing), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	if err := run(srcDir, defaultBaseURL, goldenRev, outDir); err == nil {
		t.Fatal("run succeeded, want a format error")
	}
	for _, name := range names {
		got, err := os.ReadFile(filepath.Join(outDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != existing {
			t.Errorf("%s was overwritten", name)
		}
	}
}
