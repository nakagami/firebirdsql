# Statement record counts

`RowsAffected` now decodes the existing `isc_info_sql_records` response by item
tags and unsigned little-endian lengths. It no longer assumes fixed offsets or
adds three 32-bit counters before converting their sum to 64 bits. No additional
server requests are made. The successful result has the same size and allocation
behavior as before; parsing valid metadata allocates no memory.

The internal typed record model distinguishes an absent records block (normal
for DDL) from four known zero counts. Unknown TLV items are skipped by length;
missing or duplicate record counts, malformed lengths, missing terminators,
truncation/error markers, negative counts and overflowing totals are rejected.
The decoder accepts the signed four/eight-byte encoding used by `INF_convert`.
Selected counts remain separate from inserted/updated/deleted counts. These are
statement counters, not a promise to include all effects of triggers or nested
procedures, and not a count of distinct business objects.

Execution has already succeeded when this metadata is decoded. A malformed count
therefore returns an error from `Result.RowsAffected()`, while `Exec` retains its
successful outcome and performs the existing autocommit step. This error is never
`driver.ErrBadConn` and never triggers a SQL retry. Network/transaction failures
in the pre-existing execution path are outside this decoder change.

The public driver API is unchanged. This is the records-decoder portion of the
broader diagnostics proposal; it does not add observer callbacks, connection IDs,
capability discovery, plan retrieval, a Trace parser, or a Profiler controller.
There is no dependency on OpenTelemetry or Firebird 5 packages. The separate
Trace lifecycle proposal is [PR #286](https://github.com/nakagami/firebirdsql/pull/286).
Both PRs are independently based on upstream master `8d1f017`.

The encoding and nesting were checked against Firebird's
[`INF_convert` and `INF_request_info`](https://github.com/FirebirdSQL/firebird/blob/v5.0.3/src/jrd/inf.cpp)
and [`isc_info_sql_records` handling](https://github.com/FirebirdSQL/firebird/blob/v5.0.3/src/dsql/dsql.cpp).
An initial live test used INSERT/SELECT against the same table; it was corrected
to select from `RDB$DATABASE` because Firebird 2.5 has a documented
[unstable insert cursor](https://www.firebirdsql.org/file/documentation/chunk/en/refdocs/fblangref25/fblangref25-dml-insert.html).
The test now uses the same finite workload on every version, without a skip.

## Checks

- Unit tests cover ordering, unknown items, four/eight-byte widths, counts above
  32 bits, absent/zero distinction, every truncated prefix, duplicate fields,
  signed-value rejection and sum overflow.
- A wire-level regression simulates successful DML followed by malformed counts.
  It verifies exactly one execution, one existing info request and one existing
  commit-retaining (in autocommit mode). Explicit-transaction mode sends no commit.
- The live test verifies DDL, insert, insert/select, update, zero-row update,
  delete, repeated prepared execution and rollback on one pinned connection.
- `FuzzStatementRecords`: 1,288,995 executions in 11 seconds without failure.
- Apple M4 / Go 1.27.0: decoder benchmark 13.28–13.62 ns/op, 0 B/op, 0 allocs/op
  (three runs). This is a decoder microbenchmark, not a server latency claim.

To repeat the focused tests against an isolated server:

```sh
TMPDIR=/tmp FIREBIRD_TEST_SERVER_ADDR=localhost:3050 \
  go test -race -run TestStatementRecords -count=1 ./...
go test -run '^$' -fuzz '^FuzzStatementRecords$' -fuzztime=10s ./...
go test -run '^$' -bench BenchmarkStatementRecords -benchmem ./...
```

Use the existing `ISC_USER`/`ISC_PASSWORD` test settings as necessary. Full-suite
Docker runs should put the test runner in the server's network namespace, so the
auxiliary port used by the existing event tests is reachable. The matrix runner
shared with PR #286 provides this setup (the script is identical in both PRs).
The dedicated Go 1.22 CI matrix runs these regressions on all five server versions;
the tests also run in the existing Firebird 2.5 and 3 CI jobs. HQBird/vendor servers and performance under production
load have not been verified.

Verified locally on 2026-09-05:

| Server | Validation |
| --- | --- |
| LI-V2.5.9.27139 | Full suite with race detector, pass after fixture correction |
| LI-V3.0.10.33601 | Full suite with race detector, pass |
| LI-V4.0.6.3221 | Full suite with race detector, pass |
| LI-V5.0.4.1812 | Full suite with race detector, pass |
| LI-V5.0.3.1683 | Focused live records test with race detector, pass |
| LI-V5.0.0.1306 | Focused live/unit records tests with race detector, pass on Go 1.22 |

Full runs used Go 1.26.3 Linux amd64 and returned PASS with the existing
version/fixture skips. These include features unsupported on older engines,
external EMPLOYEE fixtures, optional performance tests, and authentication/limbo
fixtures absent from the images. `go vet -composites=false ./...` passes;
default vet reports the existing unkeyed `sql.TxOptions` literal in
`driver_go18_test.go`. Neither passing these tests nor decoding wide counters
proves support for every server configuration or every nested PSQL counting scope.
