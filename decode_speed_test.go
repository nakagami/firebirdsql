package firebirdsql

import (
	"database/sql"
	"log"
	"testing"
)

func BenchmarkRead(b *testing.B) {
	b.StopTimer()
	temppath := TempFileName("test_basic_")
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050"+temppath)
	query := `
	CREATE TABLE PERFTEST (
		A SMALLINT,
		B INT,
		C BIGINT,
		D VARCHAR(255),
		B1 INT,
		B2 INT,
		B3 INT,
		B4 INT,
		B5 INT,
		B6 INT,
		B7 INT,
		B8 INT,
		B9 INT,
		B11 INT,
		B12 INT,
		B13 INT,
		B14 INT,
		B15 INT,
		B16 INT,
		B17 INT,
		B18 INT,
		B19 INT
	);
	`
	_, err = conn.Exec(query)
	if err != nil {
		b.Error(err)
		log.Fatal(err)
	}

	tx, err := conn.Begin()
	if err != nil {
		b.Error(err)
	}

	stmt, err := tx.Prepare("INSERT INTO PERFTEST (A,B,C,D,B1,B2,B3,B4,B5,B6,B7,B8,B9,B11,B12,B13,B14,B15,B16,B17,B18,B19) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		b.Error(err)
		log.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		stmt.Exec(i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i)
	}
	tx.Commit()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		rows, err := conn.Query("SELECT * FROM PERFTEST")
		if err != nil {
			b.Error(err)
		}

		var vals []interface{}

		for rows.Next() {
			rows.Scan(vals...)
		}
		rows.Close()
	}
}
