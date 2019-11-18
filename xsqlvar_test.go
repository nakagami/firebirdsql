package firebirdsql

import (
	"testing"
	"time"
)

func TestParseDateTypes(t *testing.T) {
	tz := time.Local
	date := time.Date(1992, 1, 12, 4, 13, 22, 629*1000000, tz)

	var expected = []string{"1992-01-12", "04:13:22.629", "1992-01-12 04:13:22.629"}
	var sqlDateTypes = []int{SQL_TYPE_DATE, SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP}

	for i, sqltype := range sqlDateTypes {

		casteableDate := parseDateType(date, sqltype)

		if casteableDate != expected[i] {
			t.Errorf("parse date to casteable date fail, expected %s returned %s", expected[i], casteableDate)
		}
	}
}
