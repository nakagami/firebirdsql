// Generator for errmsgs.go, fberrcode.go, sqlstate_map.go, sqlcode_map.go.
// Fetches Firebird's message headers and emits the error-code tables.
//
// Usage:
//
//	go run ./_attic/errormsgs                                     # fetch from upstream master
//	go run ./_attic/errormsgs -url <base-url>                     # custom branch/tag base URL
//	go run ./_attic/errormsgs -src /path/to/firebird/src/include  # local Firebird checkout
//	go run ./_attic/errormsgs -outdir /path/to/output             # write files elsewhere
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	defaultBaseURL = "https://raw.githubusercontent.com/FirebirdSQL/firebird/master/src/include/firebird/impl"

	idplHeader = `/****************************************************************************
The contents of this file are subject to the Interbase Public
License Version 1.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy
of the License at http://www.Inprise.com/IPL.html

Software distributed under the License is distributed on an
"AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express
or implied. See the License for the specific language governing
rights and limitations under the License.

*****************************************************************************/
`
)

type msgEntry struct {
	facilityName string
	facilityID   int
	number       int
	symbol       string // empty for FB_IMPL_MSG_NO_SYMBOL
	sqlCode      int32  // 0 if not provided
	sqlClass     string // e.g. "22"
	sqlSub       string // e.g. "000"
	text         string // inner C-string content, without surrounding quotes
}

func (e msgEntry) iscCode() int {
	return (e.facilityID&0x1F)<<16 | (e.number&0x3FFF) | 0x14000000
}

func (e msgEntry) sqlState() string {
	if e.sqlClass == "" {
		return ""
	}
	return e.sqlClass + e.sqlSub
}

func (e msgEntry) goConst() string {
	return "ISC" + toCamelCase(e.symbol)
}

func main() {
	baseURL := flag.String("url", defaultBaseURL, "base URL of Firebird impl/ include directory")
	srcDir := flag.String("src", "", "local firebird src/include path (overrides -url)")
	outDir := flag.String("outdir", ".", "directory for generated output files")
	flag.Parse()

	if err := run(*srcDir, *baseURL, *outDir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(srcDir, baseURL, outDir string) error {
	helperSrc, err := readContent(srcDir, baseURL, "msg_helper.h")
	if err != nil {
		return fmt.Errorf("read msg_helper.h: %w", err)
	}
	facilities, err := parseFacilities(helperSrc)
	if err != nil {
		return fmt.Errorf("parse msg_helper.h: %w", err)
	}

	allSrc, err := readContent(srcDir, baseURL, "msg/all.h")
	if err != nil {
		return fmt.Errorf("read msg/all.h: %w", err)
	}
	includes := parseIncludes(allSrc)

	var entries []msgEntry
	for _, inc := range includes {
		facSrc, err := readContent(srcDir, baseURL, "msg/"+inc)
		if err != nil {
			return fmt.Errorf("read msg/%s: %w", inc, err)
		}
		facName := strings.ToUpper(strings.TrimSuffix(inc, ".h"))
		facID, ok := facilities[facName]
		if !ok {
			return fmt.Errorf("facility ID not found for %q (derived from %s); known: %v", facName, inc, facilityKeys(facilities))
		}
		got, err := parseEntries(facSrc, facName, facID)
		if err != nil {
			return fmt.Errorf("parse msg/%s: %w", inc, err)
		}
		entries = append(entries, got...)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].iscCode() < entries[j].iscCode()
	})

	// Always use the canonical upstream URL so generated files are stable
	// regardless of whether -src or -url was used.
	sourceRef := defaultBaseURL + "/msg/all.h"

	for _, emit := range []struct {
		name string
		fn   func(string, []msgEntry, string) error
	}{
		{"errmsgs.go", emitErrmsgs},
		{"fberrcode.go", emitFbErrcode},
		{"sqlstate_map.go", emitSQLStateMap},
		{"sqlcode_map.go", emitSQLCodeMap},
	} {
		if err := emit.fn(outDir, entries, sourceRef); err != nil {
			return fmt.Errorf("emit %s: %w", emit.name, err)
		}
	}

	var withSym, withState int
	for _, e := range entries {
		if e.symbol != "" {
			withSym++
		}
		if e.sqlState() != "" {
			withState++
		}
	}
	fmt.Printf("Generated 4 files in %s: %d entries, %d with symbols, %d with SQLSTATE\n",
		outDir, len(entries), withSym, withState)
	return nil
}

func readContent(srcDir, baseURL, relPath string) (string, error) {
	if srcDir != "" {
		p := filepath.Join(srcDir, "firebird", "impl", filepath.FromSlash(relPath))
		b, err := os.ReadFile(p)
		return string(b), err
	}
	url := baseURL + "/" + relPath
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d fetching %s", resp.StatusCode, url)
	}
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

var reFacility = regexp.MustCompile(`#define\s+FB_IMPL_MSG_FACILITY_(\w+)\s+(\d+)`)

func parseFacilities(src string) (map[string]int, error) {
	m := make(map[string]int)
	for _, sub := range reFacility.FindAllStringSubmatch(src, -1) {
		n, _ := strconv.Atoi(sub[2])
		m[sub[1]] = n
	}
	if len(m) == 0 {
		return nil, fmt.Errorf("no FB_IMPL_MSG_FACILITY_* defines found")
	}
	return m, nil
}

var reInclude = regexp.MustCompile(`#include\s+"([^"]+)"`)

func parseIncludes(src string) []string {
	var result []string
	for _, sub := range reInclude.FindAllStringSubmatch(src, -1) {
		result = append(result, sub[1])
	}
	return result
}

var reMacroStart = regexp.MustCompile(`\bFB_IMPL_MSG(_NO_SYMBOL|_SYMBOL)?\s*\(`)

func parseEntries(src, facilityName string, facilityID int) ([]msgEntry, error) {
	var entries []msgEntry
	for _, line := range strings.Split(src, "\n") {
		if idx := strings.Index(line, "//"); idx >= 0 {
			line = line[:idx]
		}
		if !strings.Contains(line, "FB_IMPL_MSG") {
			continue
		}
		loc := reMacroStart.FindStringIndex(line)
		if loc == nil {
			continue
		}
		match := line[loc[0]:loc[1]]
		variant := ""
		switch {
		case strings.Contains(match, "_NO_SYMBOL"):
			variant = "no_symbol"
		case strings.Contains(match, "_SYMBOL"):
			variant = "symbol"
		}
		args := splitArgs(line, loc[1]) // loc[1] points past the opening '('
		e, ok := buildEntry(variant, args, facilityName, facilityID)
		if ok {
			entries = append(entries, e)
		}
	}
	return entries, nil
}

func buildEntry(variant string, args []string, facilityName string, facilityID int) (msgEntry, bool) {
	if len(args) < 3 {
		return msgEntry{}, false
	}
	number, err := strconv.Atoi(strings.TrimSpace(args[1]))
	if err != nil {
		return msgEntry{}, false
	}
	e := msgEntry{facilityName: facilityName, facilityID: facilityID, number: number}
	switch variant {
	case "no_symbol":
		e.text = unquote(args[2])
	case "symbol":
		if len(args) < 4 {
			return msgEntry{}, false
		}
		e.symbol = strings.TrimSpace(args[2])
		e.text = unquote(args[3])
	default: // full FB_IMPL_MSG(fac, num, sym, sqlCode, class, sub, text)
		if len(args) < 7 {
			return msgEntry{}, false
		}
		e.symbol = strings.TrimSpace(args[2])
		sc, err := strconv.ParseInt(strings.TrimSpace(args[3]), 10, 32)
		if err != nil {
			return msgEntry{}, false
		}
		e.sqlCode = int32(sc)
		e.sqlClass = unquote(args[4])
		e.sqlSub = unquote(args[5])
		e.text = unquote(args[6])
	}
	if e.text == "" {
		return msgEntry{}, false
	}
	return e, true
}

// splitArgs splits comma-separated macro arguments starting just after the opening '(' at pos.
// String literals with escaped characters are handled correctly.
func splitArgs(src string, pos int) []string {
	var args []string
	var cur strings.Builder
	depth := 1
	inStr := false
	escape := false
	for i := pos; i < len(src); i++ {
		c := src[i]
		if escape {
			cur.WriteByte(c)
			escape = false
			continue
		}
		if c == '\\' && inStr {
			cur.WriteByte(c)
			escape = true
			continue
		}
		if c == '"' {
			inStr = !inStr
			cur.WriteByte(c)
			continue
		}
		if inStr {
			cur.WriteByte(c)
			continue
		}
		switch c {
		case '(':
			depth++
			cur.WriteByte(c)
		case ')':
			depth--
			if depth == 0 {
				args = append(args, strings.TrimSpace(cur.String()))
				return args
			}
			cur.WriteByte(c)
		case ',':
			args = append(args, strings.TrimSpace(cur.String()))
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		args = append(args, strings.TrimSpace(cur.String()))
	}
	return args
}

func unquote(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

func toCamelCase(s string) string {
	var sb strings.Builder
	capNext := true
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '_' {
			capNext = true
			continue
		}
		if capNext && c >= 'a' && c <= 'z' {
			c -= 32
		}
		capNext = false
		sb.WriteByte(c)
	}
	return sb.String()
}

func facilityKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func writeFileHeader(w *bufio.Writer, sourceRef string, withGenerate bool) {
	fmt.Fprint(w, idplHeader)
	fmt.Fprintf(w, "\n// Code generated by _attic/errormsgs; DO NOT EDIT.\n")
	fmt.Fprintf(w, "// Source: %s\n", sourceRef)
	if withGenerate {
		fmt.Fprintf(w, "\n//go:generate go run ./_attic/errormsgs\n")
	}
	fmt.Fprintf(w, "\npackage firebirdsql\n\n")
}

func createOut(outDir, name string) (*os.File, error) {
	return os.Create(filepath.Join(outDir, name))
}

func emitErrmsgs(outDir string, entries []msgEntry, sourceRef string) (retErr error) {
	f, err := createOut(outDir, "errmsgs.go")
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	w := bufio.NewWriter(f)
	writeFileHeader(w, sourceRef, true)
	fmt.Fprintln(w, "// errmsgs returns the Firebird message template for a GDS error code,")
	fmt.Fprintln(w, "// or an empty string when the code is unknown.")
	fmt.Fprintln(w, "func errmsgs(code int) string {")
	fmt.Fprintln(w, "\tswitch code {")
	for _, e := range entries {
		fmt.Fprintf(w, "\tcase %d:\n\t\treturn \"%s\\n\"\n", e.iscCode(), e.text)
	}
	fmt.Fprintln(w, "\tdefault:")
	fmt.Fprintln(w, "\t\treturn \"\"")
	fmt.Fprintln(w, "\t}")
	fmt.Fprintln(w, "}")
	return w.Flush()
}

func emitFbErrcode(outDir string, entries []msgEntry, sourceRef string) (retErr error) {
	f, err := createOut(outDir, "fberrcode.go")
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	w := bufio.NewWriter(f)
	writeFileHeader(w, sourceRef, false)
	fmt.Fprintln(w, "// ISC* constants map Firebird symbol names to their numeric GDS error codes.")
	fmt.Fprintln(w, "// Use with errors.As to classify *FbError values:")
	fmt.Fprintln(w, "//")
	fmt.Fprintln(w, "//\tvar fbErr *FbError")
	fmt.Fprintln(w, "//\tif errors.As(err, &fbErr) && slices.Contains(fbErr.GDSCodes, ISCUniqueKeyViolation) { ... }")
	fmt.Fprintln(w, "const (")
	cur := ""
	for _, e := range entries {
		if e.symbol == "" {
			continue
		}
		if e.facilityName != cur {
			if cur != "" {
				fmt.Fprintln(w)
			}
			fmt.Fprintf(w, "\t// %s facility\n", e.facilityName)
			cur = e.facilityName
		}
		fmt.Fprintf(w, "\t%s = %d\n", e.goConst(), e.iscCode())
	}
	fmt.Fprintln(w, ")")
	return w.Flush()
}

func emitSQLStateMap(outDir string, entries []msgEntry, sourceRef string) (retErr error) {
	f, err := createOut(outDir, "sqlstate_map.go")
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	w := bufio.NewWriter(f)
	writeFileHeader(w, sourceRef, false)
	fmt.Fprintln(w, "// gdsToSQLState returns the SQL:2003 SQLSTATE string for a GDS error code,")
	fmt.Fprintln(w, "// or an empty string when the code has no SQLSTATE mapping.")
	fmt.Fprintln(w, "// Used as a fallback when the server does not send isc_arg_sql_state.")
	fmt.Fprintln(w, "func gdsToSQLState(code int) string {")
	fmt.Fprintln(w, "\tswitch code {")
	for _, e := range entries {
		if e.sqlState() == "" {
			continue
		}
		fmt.Fprintf(w, "\tcase %d:\n\t\treturn %q\n", e.iscCode(), e.sqlState())
	}
	fmt.Fprintln(w, "\tdefault:")
	fmt.Fprintln(w, "\t\treturn \"\"")
	fmt.Fprintln(w, "\t}")
	fmt.Fprintln(w, "}")
	return w.Flush()
}

func emitSQLCodeMap(outDir string, entries []msgEntry, sourceRef string) (retErr error) {
	f, err := createOut(outDir, "sqlcode_map.go")
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	w := bufio.NewWriter(f)
	writeFileHeader(w, sourceRef, false)
	fmt.Fprintln(w, "// gdsToSQLCode returns the Firebird SQL error code for a GDS error code, or 0.")
	fmt.Fprintln(w, "// Used as a fallback when the server does not send isc_arg_sqlerr.")
	fmt.Fprintln(w, "func gdsToSQLCode(code int) int32 {")
	fmt.Fprintln(w, "\tswitch code {")
	for _, e := range entries {
		if e.sqlCode == 0 {
			continue
		}
		fmt.Fprintf(w, "\tcase %d:\n\t\treturn %d\n", e.iscCode(), e.sqlCode)
	}
	fmt.Fprintln(w, "\tdefault:")
	fmt.Fprintln(w, "\t\treturn 0")
	fmt.Fprintln(w, "\t}")
	fmt.Fprintln(w, "}")
	return w.Flush()
}
