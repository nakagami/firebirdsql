package main

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "update golden files")

func TestGolden(t *testing.T) {
	outDir := t.TempDir()

	// srcDir points at testdata/; readContent will look under testdata/firebird/impl/
	if err := run("testdata", defaultBaseURL, outDir); err != nil {
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
