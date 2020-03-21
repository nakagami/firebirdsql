package firebirdsql

import (
	"fmt"

	"github.com/pkg/errors"
)

// ErrOpResponse operation request error
type ErrOpResponse struct{ opRCode int32 }

// Error interface implementation
func NewErrOpResonse(opRCode int32) error { return &ErrOpResponse{opRCode: opRCode} }
func (e *ErrOpResponse) Error() string    { return fmt.Sprintf("Error op_response:%d", e.opRCode) }

var ErrOpSqlResponse = errors.New("Error op_sql_response")
