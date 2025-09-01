package firebirdsql

import (
	"database/sql/driver"
	"fmt"
	"time"
)

type Null[T any] struct {
	value     T
	isDefined bool
}

func (n *Null[T]) Scan(from interface{}) error {
	if from == nil {
		n.isDefined = false
		return nil
	}

	var value T
	switch destination := any(&value).(type) {
	case *int64:
		switch s := from.(type) {
		case int64:
			*destination = s
		case int32:
			*destination = int64(s)
		default:
			return fmt.Errorf("unsupported type %T for int64", from)
		}
	case *string:
		switch s := from.(type) {
		case []byte:
			*destination = string(s)
		case string:
			*destination = s
		case []rune:
			*destination = string(s)
		default:
			return fmt.Errorf("unsupported type %T for string", from)
		}
	case *float64:
		switch s := from.(type) {
		case float64:
			*destination = s
		case float32:
			*destination = float64(s)
		default:
			return fmt.Errorf("unsupported type %T for float64", from)
		}
	case *time.Time:
		switch s := from.(type) {
		case time.Time:
			*destination = s
		default:
			return fmt.Errorf("unsupported type %T for time.Time", from)
		}
	default:
		return fmt.Errorf("unsupported generic type %T", from)
	}

	n.value = value
	n.isDefined = true

	return nil
}

func (n Null[T]) Value() (driver.Value, error) {
	if !n.isDefined {
		return nil, nil
	}

	var value T
	switch any(&value).(type) {
	case *int64:
	case *string:
	case *float64:
	case *time.Time:
	default:
		return nil, fmt.Errorf("unsupported type %T", any(&value))
	}

	return n.Value, nil
}
