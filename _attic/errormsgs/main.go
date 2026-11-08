// Generator for errmsgs.go, fberrcode.go, sqlstate_map.go, sqlcode_map.go.
// Fetches Firebird's message headers and emits the error-code tables.
//
// Usage:
//
//	go run ./_attic/errormsgs                                     # fetch from the current upstream master commit
//	go run ./_attic/errormsgs -rev <sha>                          # fetch from that upstream commit
//	go run ./_attic/errormsgs -url <base-url> [-rev <sha>]        # custom branch/tag base URL
//	go run ./_attic/errormsgs -src /path/to/firebird/src/include [-rev <sha>]  # local Firebird checkout
//	go run ./_attic/errormsgs -outdir /path/to/output             # write files elsewhere
//
// Generated files record the Firebird commit they came from. With -url or -src
// the commit cannot be discovered, so -rev states it; without -rev it is "unknown".
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultBaseURL  = "https://raw.githubusercontent.com/FirebirdSQL/firebird/master/src/include/firebird/impl"
	pinnedBaseURL   = "https://raw.githubusercontent.com/FirebirdSQL/firebird/%s/src/include/firebird/impl"
	masterCommitURL = "https://api.github.com/repos/FirebirdSQL/firebird/commits/master"
	unknownRevision = "unknown"

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

var (
	httpClient  = &http.Client{Timeout: 30 * time.Second}
	reCommitSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)
	// errEmptyText marks a message upstream deliberately left without text.
	errEmptyText = errors.New("empty message text")
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
	return (e.facilityID&0x1F)<<16 | (e.number & 0x3FFF) | 0x14000000
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
	rev := flag.String("rev", "", "Firebird commit: pins the default URL, or states the commit for -url and -src")
	flag.Parse()

	resolvedURL, resolvedRev, err := resolveRevision(*srcDir, *baseURL, *rev)
	if err == nil {
		err = run(*srcDir, resolvedURL, resolvedRev, *outDir)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// resolveRevision returns the base URL to read from and the Firebird commit to
// record. The default source is pinned to one commit, so every header comes from
// it. A checkout or custom URL cannot be tied to a commit, so there rev only states
// what to record.
func resolveRevision(srcDir, baseURL, rev string) (string, string, error) {
	pinned := srcDir == "" && baseURL == defaultBaseURL
	switch {
	case rev == "" && !pinned:
		return baseURL, unknownRevision, nil
	case rev == "":
		sha, err := lookupCommit(masterCommitURL)
		if err != nil {
			return "", "", fmt.Errorf("resolve Firebird master commit: %w (pass -rev <40-hex sha> to skip this lookup)", err)
		}
		rev = sha
	case rev == unknownRevision && !pinned:
		return baseURL, rev, nil
	case !reCommitSHA.MatchString(rev):
		return "", "", fmt.Errorf("invalid -rev %q: want a full 40-character hex commit SHA", rev)
	}
	if pinned {
		baseURL = fmt.Sprintf(pinnedBaseURL, rev)
	}
	return baseURL, rev, nil
}

// lookupCommit reads a commit SHA from a GitHub commits endpoint. The Accept header
// makes GitHub return the bare SHA instead of a JSON document.
func lookupCommit(url string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/vnd.github.sha")
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	sha := strings.TrimSpace(string(b))
	if !reCommitSHA.MatchString(sha) {
		return "", fmt.Errorf("unexpected response from %s: %q", url, sha)
	}
	return sha, nil
}

func run(srcDir, baseURL, rev, outDir string) error {
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
	var warnings []string
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
		got, warned := parseEntries(facSrc, "msg/"+inc, facName, facID)
		entries = append(entries, got...)
		warnings = append(warnings, warned...)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].iscCode() < entries[j].iscCode()
	})

	// Always use the canonical upstream URL so generated files are stable
	// regardless of whether -src or -url was used.
	sourceRef := defaultBaseURL + "/msg/all.h"

	outputs := []struct {
		name string
		emit func(io.Writer, []msgEntry, string, string)
	}{
		{"errmsgs.go", emitErrmsgs},
		{"fberrcode.go", emitFbErrcode},
		{"sqlstate_map.go", emitSQLStateMap},
		{"sqlcode_map.go", emitSQLCodeMap},
	}

	// Render everything first: invalid output must not overwrite any existing file.
	rendered := make([][]byte, len(outputs))
	for i, out := range outputs {
		var buf bytes.Buffer
		out.emit(&buf, entries, sourceRef, rev)
		src, err := format.Source(buf.Bytes())
		if err != nil {
			return fmt.Errorf("format %s: %w", out.name, err)
		}
		rendered[i] = src
	}
	for i, out := range outputs {
		if err := os.WriteFile(filepath.Join(outDir, out.name), rendered[i], 0o644); err != nil {
			return fmt.Errorf("write %s: %w", out.name, err)
		}
	}

	for _, w := range warnings {
		fmt.Fprintln(os.Stderr, "warning:", w)
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
	fmt.Printf("Generated 4 files in %s from Firebird commit %s: %d entries, %d with symbols, %d with SQLSTATE, %d warnings\n",
		outDir, rev, len(entries), withSym, withState, len(warnings))
	return nil
}

func readContent(srcDir, baseURL, relPath string) (string, error) {
	if srcDir != "" {
		p := filepath.Join(srcDir, "firebird", "impl", filepath.FromSlash(relPath))
		b, err := os.ReadFile(p)
		return string(b), err
	}
	url := baseURL + "/" + relPath
	resp, err := httpClient.Get(url) //nolint:noctx
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

// parseEntries returns the messages in one facility header, plus a warning for
// each macro line it could not turn into a message. Messages upstream left empty
// are skipped silently.
func parseEntries(src, fileName, facilityName string, facilityID int) ([]msgEntry, []string) {
	var entries []msgEntry
	var warnings []string
	for i, line := range strings.Split(src, "\n") {
		if idx := strings.Index(line, "//"); idx >= 0 {
			line = line[:idx]
		}
		if !strings.Contains(line, "FB_IMPL_MSG") {
			continue
		}
		loc := reMacroStart.FindStringIndex(line)
		if loc == nil {
			warnings = append(warnings, fmt.Sprintf("%s:%d: unrecognized macro: %s", fileName, i+1, strings.TrimSpace(line)))
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
		e, err := buildEntry(variant, args, facilityName, facilityID)
		switch {
		case err == nil:
			entries = append(entries, e)
		case !errors.Is(err, errEmptyText):
			warnings = append(warnings, fmt.Sprintf("%s:%d: %v: %s", fileName, i+1, err, strings.TrimSpace(line)))
		}
	}
	return entries, warnings
}

func buildEntry(variant string, args []string, facilityName string, facilityID int) (msgEntry, error) {
	if len(args) < 3 {
		return msgEntry{}, fmt.Errorf("want at least 3 macro arguments, got %d", len(args))
	}
	number, err := strconv.Atoi(strings.TrimSpace(args[1]))
	if err != nil {
		return msgEntry{}, fmt.Errorf("bad message number: %w", err)
	}
	e := msgEntry{facilityName: facilityName, facilityID: facilityID, number: number}
	switch variant {
	case "no_symbol":
		e.text = unquote(args[2])
	case "symbol":
		if len(args) < 4 {
			return msgEntry{}, fmt.Errorf("want 4 macro arguments, got %d", len(args))
		}
		e.symbol = strings.TrimSpace(args[2])
		e.text = unquote(args[3])
	default: // full FB_IMPL_MSG(fac, num, sym, sqlCode, class, sub, text)
		if len(args) < 7 {
			return msgEntry{}, fmt.Errorf("want 7 macro arguments, got %d", len(args))
		}
		e.symbol = strings.TrimSpace(args[2])
		sc, err := strconv.ParseInt(strings.TrimSpace(args[3]), 10, 32)
		if err != nil {
			return msgEntry{}, fmt.Errorf("bad SQLCODE: %w", err)
		}
		e.sqlCode = int32(sc)
		e.sqlClass = unquote(args[4])
		e.sqlSub = unquote(args[5])
		e.text = unquote(args[6])
	}
	if e.text == "" {
		return msgEntry{}, errEmptyText
	}
	return e, nil
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

func writeFileHeader(w io.Writer, sourceRef, rev string, withGenerate bool) {
	fmt.Fprint(w, idplHeader)
	fmt.Fprintf(w, "\n// Code generated by _attic/errormsgs; DO NOT EDIT.\n")
	fmt.Fprintf(w, "// Source: %s\n", sourceRef)
	fmt.Fprintf(w, "// Firebird commit: %s\n", rev)
	if withGenerate {
		fmt.Fprintf(w, "\n//go:generate go run ./_attic/errormsgs\n")
	}
	fmt.Fprintf(w, "\npackage firebirdsql\n\n")
}

func emitErrmsgs(w io.Writer, entries []msgEntry, sourceRef, rev string) {
	writeFileHeader(w, sourceRef, rev, true)
	fmt.Fprintln(w, "// errmsgs returns the Firebird message template for a GDS error code,")
	fmt.Fprintln(w, "// or an empty string when the code is unknown.")
	fmt.Fprintln(w, "func errmsgs(code int) string {")
	fmt.Fprintln(w, "\tswitch code {")
	for _, e := range entries {
		fmt.Fprintf(w, "\tcase %d:\n\t\treturn \"%s\"\n", e.iscCode(), e.text)
	}
	fmt.Fprintln(w, "\tdefault:")
	fmt.Fprintln(w, "\t\treturn \"\"")
	fmt.Fprintln(w, "\t}")
	fmt.Fprintln(w, "}")
}

func emitFbErrcode(w io.Writer, entries []msgEntry, sourceRef, rev string) {
	writeFileHeader(w, sourceRef, rev, false)
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
}

func emitSQLStateMap(w io.Writer, entries []msgEntry, sourceRef, rev string) {
	writeFileHeader(w, sourceRef, rev, false)
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
}

func emitSQLCodeMap(w io.Writer, entries []msgEntry, sourceRef, rev string) {
	writeFileHeader(w, sourceRef, rev, false)
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
}
