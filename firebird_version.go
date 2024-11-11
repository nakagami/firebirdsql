/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2023-2024 Artyom Smirnov <artyom_smirnov@me.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*******************************************************************************/

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
	Raw         string
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
		BuildNumber: build,
		Raw:         rawVersionString,
	}
}

func (v FirebirdVersion) EqualOrGreater(major int, minor int) bool {
	return v.Major > major || (v.Major == major && v.Minor >= minor)
}

func (v FirebirdVersion) EqualOrGreaterPatch(major int, minor int, patch int) bool {
	return v.Major > major || (v.Major == major && (v.Minor == minor && v.Patch >= patch || v.Minor > minor))
}
