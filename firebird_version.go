package firebirdsql

import (
	"regexp"
	"strconv"
)

var FirebirdVersionPattern = regexp.MustCompile(`((\w{2})-(\w)(\d+)\.(\d+)\.(\d+)\.(\d+)(?:-\S+)?) (.+)`)

type FirebirdVersion struct {
	Platform    string
	Type        string
	Full        string
	Major       int
	Minor       int
	Patch       int
	BuildNumber int
}

func ParseFirebirdVersion(rawVersionString string) FirebirdVersion {
	res := FirebirdVersionPattern.FindStringSubmatch(rawVersionString)
	major, _ := strconv.Atoi(res[4])
	minor, _ := strconv.Atoi(res[5])
	patch, _ := strconv.Atoi(res[6])
	build, _ := strconv.Atoi(res[7])
	return FirebirdVersion{Platform: res[2],
		Type:        res[3],
		Full:        res[1],
		Major:       major,
		Minor:       minor,
		Patch:       patch,
		BuildNumber: build}
}

func (v FirebirdVersion) EqualOrGreater(major int, minor int) bool {
	return v.Major > major || (v.Major == major && v.Minor >= minor)
}

func (v FirebirdVersion) EqualOrGreaterPatch(major int, minor int, patch int) bool {
	return v.Major > major || (v.Major == major && (v.Minor == minor && v.Patch >= patch || v.Minor > minor))
}
