package firebirdsql

import (
	"strings"
	"testing"
)

// serverVersionFrame builds the op_response GetServerVersion reads: a service
// buffer of [isc_info_svc_server_version][2-byte len][banner].
func serverVersionFrame(banner string) []byte {
	var f acceptFrame
	buf := []byte{isc_info_svc_server_version, byte(len(banner)), byte(len(banner) >> 8)}
	buf = append(buf, banner...)
	f.opResponseFrame(0, buf)
	return f.bytes()
}

func TestGetServerVersion_Garbage(t *testing.T) {
	svc := &ServiceManager{wp: testProtocol(serverVersionFrame("totally bogus banner"))}
	_, err := svc.GetServerVersion()
	if err == nil {
		t.Fatal("expected error for unrecognized banner, got nil")
	}
	if !strings.Contains(err.Error(), "unrecognized server version") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestGetServerVersion_Valid(t *testing.T) {
	svc := &ServiceManager{wp: testProtocol(serverVersionFrame("LI-V3.0.11.33703 Firebird 3.0"))}
	v, err := svc.GetServerVersion()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.Major != 3 || v.Minor != 0 {
		t.Errorf("got %d.%d, want 3.0", v.Major, v.Minor)
	}
}

func TestParseFirebirdVersion_RealBanners(t *testing.T) {
	cases := []struct {
		raw                       string
		major, minor, patch, bnum int
	}{
		{"LI-V2.5.9.27139 Firebird 2.5", 2, 5, 9, 27139},
		{"LI-V3.0.11.33703 Firebird 3.0", 3, 0, 11, 33703},
		{"WI-V5.0.1.1469 Firebird 5.0", 5, 0, 1, 1469},
		{"LI-T6.0.0.345 Firebird 6.0 Initial", 6, 0, 0, 345},
	}
	for _, tt := range cases {
		t.Run(tt.raw, func(t *testing.T) {
			v := ParseFirebirdVersion(tt.raw)
			if v.Major != tt.major || v.Minor != tt.minor || v.Patch != tt.patch || v.BuildNumber != tt.bnum {
				t.Errorf("got %d.%d.%d.%d, want %d.%d.%d.%d",
					v.Major, v.Minor, v.Patch, v.BuildNumber, tt.major, tt.minor, tt.patch, tt.bnum)
			}
			if v.Full == "" {
				t.Error("Full should be populated for a real banner")
			}
		})
	}
}

func TestParseFirebirdVersion_Garbage(t *testing.T) {
	// A non-matching banner must not panic; it yields a zero version with Raw
	// preserved and a conservatively-false EqualOrGreater.
	for _, raw := range []string{"", "not a version", "garbage from a hostile server"} {
		v := ParseFirebirdVersion(raw)
		if v.Full != "" || v.Major != 0 {
			t.Errorf("%q: expected zero version, got %+v", raw, v)
		}
		if v.Raw != raw {
			t.Errorf("%q: Raw not preserved, got %q", raw, v.Raw)
		}
		if v.EqualOrGreater(3, 0) {
			t.Errorf("%q: zero version must not satisfy EqualOrGreater(3,0)", raw)
		}
	}
}

// TestFirebirdVersionEqualOrGreaterMatrix mirrors Jaybird's GDSServerVersionTest
// isEqualOrAbove semantics: every released banner must compare correctly
// against the version boundaries the driver gates features on.
func TestFirebirdVersionEqualOrGreaterMatrix(t *testing.T) {
	cases := []struct {
		raw     string
		atLeast [][2]int // EqualOrGreater must be true
		below   [][2]int // EqualOrGreater must be false
	}{
		{"LI-V2.5.9.27139 Firebird 2.5", [][2]int{{2, 5}, {2, 0}}, [][2]int{{3, 0}, {5, 0}}},
		{"LI-V3.0.11.33703 Firebird 3.0", [][2]int{{2, 5}, {3, 0}}, [][2]int{{3, 1}, {4, 0}}},
		{"WI-V4.0.5.3140 Firebird 4.0", [][2]int{{3, 0}, {4, 0}}, [][2]int{{4, 1}, {5, 0}}},
		{"WI-V5.0.5.1876 Firebird 5.0", [][2]int{{4, 0}, {5, 0}}, [][2]int{{5, 1}, {6, 0}}},
	}
	for _, c := range cases {
		t.Run(c.raw, func(t *testing.T) {
			v := ParseFirebirdVersion(c.raw)
			for _, m := range c.atLeast {
				if !v.EqualOrGreater(m[0], m[1]) {
					t.Errorf("%s: EqualOrGreater(%d,%d) = false, want true", c.raw, m[0], m[1])
				}
			}
			for _, m := range c.below {
				if v.EqualOrGreater(m[0], m[1]) {
					t.Errorf("%s: EqualOrGreater(%d,%d) = true, want false", c.raw, m[0], m[1])
				}
			}
		})
	}
}

// TestFirebirdVersionEqualOrGreaterPatch covers patch-level comparisons,
// including the boundary patches a fix release depends on.
func TestFirebirdVersionEqualOrGreaterPatch(t *testing.T) {
	v := ParseFirebirdVersion("WI-V5.0.5.1876 Firebird 5.0")
	for _, m := range [][3]int{{5, 0, 0}, {5, 0, 4}, {5, 0, 5}, {4, 9, 9}} {
		if !v.EqualOrGreaterPatch(m[0], m[1], m[2]) {
			t.Errorf("EqualOrGreaterPatch(%d,%d,%d) = false, want true", m[0], m[1], m[2])
		}
	}
	for _, m := range [][3]int{{5, 0, 6}, {5, 1, 0}, {6, 0, 0}} {
		if v.EqualOrGreaterPatch(m[0], m[1], m[2]) {
			t.Errorf("EqualOrGreaterPatch(%d,%d,%d) = true, want false", m[0], m[1], m[2])
		}
	}
}
