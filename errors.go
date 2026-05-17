/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2020 Nils Krause

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
	"errors"
	"fmt"
)

// ErrOpResponse operation request error
type ErrOpResponse struct{ opRCode int32 }

// Error interface implementation
func NewErrOpResonse(opRCode int32) error { return &ErrOpResponse{opRCode: opRCode} }
func (e *ErrOpResponse) Error() string    { return fmt.Sprintf("Error op_response:%d", e.opRCode) }

var ErrOpSqlResponse = errors.New("Error op_sql_response")

// ErrInvalidIsolationLevel is returned when an unsupported isolation level is requested.
// This is an internal guard: normal code paths should only use supported levels.
var ErrInvalidIsolationLevel = errors.New("invalid isolation level")

// FbError is the structured error type returned by this driver for all
// Firebird-originated failures. All fields are populated from the wire protocol;
// none require a live connection to interpret.
//
// Classification: callers inspect GDSCodes, SQLCode, or SQLState directly.
// This package does not publish sentinel errors or predicate helpers — those
// are application concerns.
//
// Example — detect a unique-key violation:
//
//	var fbErr *firebirdsql.FbError
//	if errors.As(err, &fbErr) && slices.Contains(fbErr.GDSCodes, firebirdsql.ISCUniqueKeyViolation) {
//	    // handle duplicate key
//	}
type FbError struct {
	GDSCodes []int      // all GDS codes from the status vector, in order
	SQLCode  int32      // Firebird SQLCODE (e.g. -803); 0 if absent
	SQLState string     // 5-char SQLSTATE (e.g. "23505"); server value if sent, else fallback
	Params   [][]string // Params[i] holds @N substitution values for GDSCodes[i]
	Warnings []int      // GDS codes that arrived as isc_arg_warning entries
	Message  string     // formatted human-readable message (params already substituted)
}

func (e *FbError) Error() string {
	return e.Message
}
