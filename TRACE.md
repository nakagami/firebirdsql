# Trace lifecycle and compatibility

The existing Trace API remains available. Context variants are additive:
`StartContext`, `StartWithNameContext`, `ListContext`, `StopContext`,
`PauseContext`, `ResumeContext`, `WaitContext`, `WaitStringsContext`, and
`CloseContext`. `ID()` is the server Trace session ID. `State()` reports local
control state, not a continuously polled server status.

```go
manager, err := firebirdsql.NewTraceManager(addr, user, password, options)
if err != nil { return err }
session, err := manager.StartWithNameContext(ctx, "diagnostic-window", config)
if err != nil { return err }
defer session.Close()
lines := make(chan string)
// A caller-owned goroutine may read lines. The caller closes the channel only
// after WaitStringsContext returns. Cancellation interrupts an unread channel.
err = session.WaitStringsContext(ctx, lines)
```

Use one reader per session. Concurrent readers fail with `ErrServiceBusy`.
Stop, pause and resume use separate service attachments; they can run while the
stream is being read. Control responses must contain the requested session ID.
A reply for another session never changes the local session to the requested
state. Stop is idempotent after successful stop. Close is idempotent and preserves
both a stop error and a transport-close error. Even when stopping fails, Close
releases the streaming connection.

Cancellation closes the service transport and joins its cancellation callback
before returning. It does not send detach while another goroutine reads the
protocol. A canceled service connection cannot be reused. Explicitly start a new
session to get a new stream; there is no automatic reconnection or promise of
recovering lost events. Close without a context uses a ten-second cleanup budget.
A caller-supplied CloseContext should have a deadline. Cancellation of a control
operation may leave the server outcome unknown: retry stop or close the session.

`ServiceManager` has corresponding context methods for connect, start, wait,
string/chunk streaming and close. The legacy methods delegate to these methods.
Collectors (`WaitString`, Trace `List` and their context variants) stop at 16 MiB
and return the collected prefix with an error. Use streaming for larger output.
Service chunks are bounded by the existing request buffer size; payload lengths
are decoded unsigned, statuses are checked, and malformed/truncated headers
return errors. Timeout/data-not-ready responses are polled with a delay instead
of being sent as artificial empty lines. No consumer channel is closed by the
library. Do not copy a ServiceManager or TraceSession after use.

## Scope and decisions

This is the lifecycle foundation from the Firebird diagnostics handoff (D-PR1
and the stream/control portion of D-PR3). It fixes errors and ownership first,
without introducing new SQL or database-info requests on ordinary Exec/Query.
There is no OpenTelemetry dependency, business-transaction change, automatic SQL
retry, or dependency on Firebird 5 system packages. Existing protocol/authentication
negotiation remains in use on all versions.

Raw strings remain the single streaming API. They can contain SQL, credentials,
bind values and result values emitted by the server. Do not persist/export raw
Trace without sanitization. This PR does **not** add a typed event parser or
privacy filter. In particular, zero argument limits are not a redaction guarantee.
It accepts the caller's native Trace configuration unchanged: Firebird 2.5 uses
`<database> ... </database>`, while Firebird 3+ uses `database { ... }`.

Deferred work is explicit: neutral observers and physical connection identities,
full typed statement metadata/capabilities, the canonical Trace event parser and
config builder, and an explicit Profiler controller/reader. Automatic profiling
is a further phase. No API in this PR claims to provide those capabilities, to
construct a call tree, or to detect Profiler permissions. Raw Trace session IDs
must never be confused with statement, transaction, or attachment handles.

The protocol decisions were checked against Firebird's
[Services API implementation](https://github.com/FirebirdSQL/firebird/blob/v5.0.3/src/jrd/svc.cpp)
and [Trace configuration](https://github.com/FirebirdSQL/firebird/blob/v5.0.3/src/utilities/ntrace/fbtrace.conf).
The service timeout send item is a length-prefixed integer. Service output can
carry timeout, data-not-ready, end, or continuation markers. The implementation
uses those existing items rather than assuming a Firebird 5 capability.

## Verification

Run `scripts/test-trace-matrix.sh VERSION` to create an isolated Docker server
and run the full existing suite with the race detector. Supported matrix arguments:
`2.5.9`, `3.0.10`, `4.0.6`, `5.0.0`, `5.0.3`, `5.0.4`.
The default Go version is 1.22. Set `GO_VERSION` or `TEST_PATTERN` to select a
runtime or test subset. Images are pinned by digest; the live test logs the exact server banner. No production server is used.

The dedicated CI matrix runs the Trace/service regression subset on Go 1.22.
`FIREBIRDSQL_TRACE_TEST=1` enables the live Trace test for a separately provisioned
server; existing test settings (`FIREBIRD_TEST_SERVER_ADDR`, `ISC_USER`,
`ISC_PASSWORD`) still apply. Use only an isolated server with Trace privileges.
On macOS, `TMPDIR=/tmp` makes generated SQL database paths valid inside Linux.
Full-suite event tests need access to the auxiliary port; the script's runner
shares the server network to provide that access.

Unit tests cover start/list failures, ID mismatch/overflow, combined errors,
concurrent/repeated Close, canceled authentication, canceled blocked reads and
channel delivery, second-reader rejection, idle statuses and malformed payloads.
Fuzz targets exercise service chunks and Trace acknowledgement parsing.
Live tests use both legacy and modern configuration formats and exercise start,
list, pause, resume, statement output, stop while streaming, cancel, repeated
close and ordinary DML afterwards. On Firebird 3/4 they explicitly verify that
the Profiler package is absent.

Verified locally on 2026-09-05 (Linux amd64 Docker servers; Go 1.26.3 with race
detection for full runs, and Go 1.27.0 darwin/arm64 for focused regressions):

| Server banner | Existing full suite + Trace | Extra Trace/service checks |
| --- | --- | --- |
| LI-V2.5.9.27139 | Pass, version/fixture skips | Legacy config/output, pass |
| LI-V3.0.10.33601 | Pass, version/fixture skips | Profiler absent, pass |
| LI-V4.0.6.3221 | Pass, version/fixture skips | Profiler absent, pass |
| LI-V5.0.3.1683 | Pass, fixture skips | Pass |
| LI-V5.0.0.1306 | Not run | Race, Trace + service-info, pass |
| LI-V5.0.4.1812 | Not run | Race, Trace + service regressions, pass |

All full runs returned PASS. Existing skips include unavailable protocol features
on older engines, external EMPLOYEE fixtures, the opt-in 100k benchmark, and
LegacyAuth/limbo transaction fixtures where unavailable. A passed run with those
skips is not a guarantee for every server configuration. HQBird/vendor builds,
Profiler operation, parser privacy, runtime call trees and production overhead
were not tested. No general performance or complete compatibility claim is made.
The new service decoder fuzz run completed 883,824 executions and the Trace reply
parser 702,776 executions without failure (10 seconds each). `go vet
-composites=false ./...` passes; default vet still reports the pre-existing
unkeyed `sql.TxOptions` literal in `driver_go18_test.go`.

The reproducible script was also executed successfully with Go 1.22 on Firebird
5.0.4 for all new unit/lifecycle tests and the live Trace scenario, with `-race`.
