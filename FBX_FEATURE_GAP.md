# fbx → firebirdsql-go feature gap analysis

Source: E:\Projects_2026\fbx (fbx v0.5.0, Red Database / Firebird driver for Go, pgx-inspired).
Compared against: E:\Projects_2026\firebirdsql-go current state — `master` plus the wire-protocol feature work (batch DML, scrollable cursors, inline blobs, execute-immediate) on branch `wire-protocol-improvements`.

Legend: ❌ not implemented in firebirdsql-go · 🟡 partial (exists, but weaker/less configurable) · ✅ parity (not listed per-area unless relevant).

---

## 1. Native API & architecture

| Feature | Status | Notes |
|---|---|---|
| Native `fbx.Conn` API (context-first, beyond `database/sql`) | ❌ | firebirdsql-go is `database/sql`-only; internal conn is not a public API |
| `Hijack()` / `Construct()` raw connection escape hatch | ❌ | exposes DB handle, tx status, wire client to the application |
| Statement cache (LRU, default cap 512, invalidation on error/schema change) | ❌ | every Prepare allocates a new statement handle |
| Query execution modes (`QueryExecModeCacheStatement` / `QueryExecModeExec`, per-call override, DSN `default_query_exec_mode`) | ❌ | — |
| Wire-level tracing (`fbproto.Client.Trace` — dumps every request/response op) | ❌ | only internal `debugPrint` |
| Application tracers: QueryTracer / PrepareTracer / ConnectTracer + `tracelog` package | ❌ | — |

## 2. Connection / DSN

| Feature | Status | Notes |
|---|---|---|
| Keyword/value connection strings (`host=… database=…`) + URL | ❌ | URL only |
| Environment variables: `FBHOST`, `FBPORT`, `FBDATABASE`, `FBUSER`, `FBPASSWORD`, `FBAPPNAME`, `FBCONNECT_TIMEOUT`, `FBTARGETSESSIONATTRS`, `FBFETCH_BATCH_SIZE`, `FBWIRECOMPRESSION`, `FBTZ`, `FBOPTIONS`, `FBSEARCH_PATH` | ❌ | only `ISC_USER`/`ISC_PASSWORD` |
| Multi-host fallback (comma-separated hosts, per-fallback validation, ordered retry, host randomize) | ❌ | single host:port |
| `target_session_attrs`: primary / replica / prefer-replica / read-only / read-write (validators via `mon$transactions`, `RDB$GET_CONTEXT` replica context) | ❌ | — |
| `connect_timeout` DSN option | 🟡 | no DSN knob; context deadlines only |
| Config allow-list restriction (`ConnStringAllowedKeys`) for untrusted input | ❌ | — |
| Lifecycle hooks: `DialFunc`, `LookupFunc`, `ValidateConnect`, `AfterConnect`, `OnFbError`, `OnFbWarning`, custom DPB bytes | ❌ | — |
| Create/Drop database API family (`CreateDatabase[WithOptions]`, `DropDatabase…`) | 🟡 | create via `firebirdsql_createdb` driver name; no public Drop API |
| `application_name` / process name & pid in DPB, `system_user`, `system_host`, `client_version` params | ❌ | — |
| `search_path` DPB parameter (Firebird 6 / Red Database schemas) | ❌ | — |

## 3. Authentication

| Feature | Status | Notes |
|---|---|---|
| SRP hash variants Srp224 / Srp384 / Srp512 | ❌ | only Srp256, Srp, Legacy_Auth |
| Pluggable auth framework (`RegisterAuthPlugin`, `AuthPlugin` interface, multi-step `op_cont_auth` negotiation) | ❌ | fixed plugin allow-list |
| GSS / Kerberos authentication (`krbsrvname`, `krbspn`; gokrb5-based) | ❌ | — |
| Certificate / multifactor auth fields (Red Database security policies) | ❌ | planned in fbx too (fields exist) |

## 4. Wire protocol

| Feature | Status | Notes |
|---|---|---|
| Protocol 14/15 negotiation rows | 🟡 | firebirdsql negotiates what the server picks but its advertised table covers 10–19 already on the feature branch (10–17 on master) |
| `op_ping` based Ping | 🟡 | firebirdsql pings via `isc_info_ods_version` info request |
| Statement-level `SetTimeout` (per-statement statement timeout in execute) | ❌ | execute trailer field is always sent as 0 |
| Statement-level `SetCursorFlags` / `SetInlineBlobSize` public setters | ❌ | cursor flags only via `QueryScrollable`; inline size only via DSN |
| Red Database SRP compat mode (`srp_compat`) | ❌ | — |

## 5. Statement execution & batching

| Feature | Status | Notes |
|---|---|---|
| Named batches (`Conn.Prepare(name, "batch …")` + `ExecBatch`/`Flush`/`CancelBatch`/`Deallocate`) | ❌ | only object batches on `sql.Conn.Raw` |
| `PreparedBatch.Flush` (send to server buffer without executing) + `PendingBytes()` accounting + auto-flush at 128 KiB | 🟡 | firebirdsql batch has Add/Exec/Cancel/Close — no flush-only mode, no byte accounting |
| `BatchInsert` helper (`BatchInsertRows/Slice/Func`, pending-size auto-flush, no partial inserts) | ❌ | — |
| Batch validation of statement type and blob/array param rejection messages | ✅ | parity |
| `BatchResult.UpdateCounts` per-row counts incl. Firebird sentinels (−1/−2) | ✅ | parity |

## 6. Transactions

| Feature | Status | Notes |
|---|---|---|
| Lock timeout option (`LockTimeout` seconds → `tpb_lock_timeout`) | ❌ | only isolation + read-only |
| Raw TPB transactions (`BeginTxTPB([]byte)`) | ❌ | TPB builder is internal |
| Pseudo-nested transactions via SAVEPOINT API (`tx.Begin/Commit/Rollback` nesting) | ❌ | savepoints work via raw SQL only (tested) |
| `BeginFunc` / `BeginTxFunc` helpers | ❌ | — |
| `op_commit_retaining` / `op_rollback_retaining` protocol ops | 🟡 | autocommit uses commit-retaining internally; no public control |

## 7. BLOBs

| Feature | Status | Notes |
|---|---|---|
| Streaming BLOB mode (BPB `isc_bpb_type_stream`) + `use_stream_blobs` option | ❌ | segmented only |
| Configurable segment/buffer size (`blob_buffer_size`, 1..32767) | ❌ | fixed segment size |
| Lazy BLOB readers (`io.ReadCloser` scan target, open-on-first-read, Seek) | ❌ | whole blob read into memory |
| `io.Reader` / `io.Writer` BLOB parameter and streaming write API | ❌ | `[]byte`/string params only (full in-memory write) |
| BLOB config per parameter (subtype, stream, temporary, character set) | 🟡 | subtype inferred; no per-param stream/temporary control |
| `fbtype.DriverBytes` zero-copy scan target | ❌ | — |

## 8. Arrays

| Feature | Status | Notes |
|---|---|---|
| Array read/write via `op_get_slice`/`op_put_slice` (SDL descriptors) | ✅ | implemented 2026-09 (see ARRAY_MIGRATION_PLAN.md); wire layout verified against FB 2.5–5.0 |
| Array metadata (dimensions, bounds, element type; schema-aware on RDB 6) | 🟡 | dimensions/bounds/element type resolved at prepare time; RDB 6 schema column deferred |
| `fbtype.ArrayCodec`, `Array[T]`, `FlatArray[T]`, multi-dimensional support, ragged guards | 🟡 | `firebirdsql.FlatArray[T]` / `firebirdsql.Array[T]` (sql.Scanner + driver.Valuer) + ragged guards; no codec-map indirection (plain slices work as params via NamedValueChecker, but NOT as scan targets — database/sql conversion limitation) |
| database/sql scanning/encoding of array slices (incl. `Map.SQLScanner`) | 🟡 | via the generic wrappers; plain-slice scan targets unsupported (see above) |
| Array support in batch API | n/a | fbx rejects blob/array in batches too (server limitation) |
| TIME_TZ / TIMESTAMP_TZ / INT128 / DECFLOAT array elements | ❌ | clear "not supported" error, planned follow-up |

## 9. Types

| Feature | Status | Notes |
|---|---|---|
| `zeronull` package (zero-value-equals-NULL Scanner/Valuer wrappers: Int2/4/8, Float8, Text, Timestamp, Timestamptz, UUID, Int128) | ❌ | `database/sql` Null* and ad-hoc Null generic only |
| Typed nullable structs (`fbtype.Text`, `Int8`, `DecFloat16`…) with codecs | ❌ | — |
| `Int128` Go type (128-bit) beyond string/int64 handling | 🟡 | INT128 supported as string/big-int conversions; no native Int128 type |
| `Numeric` arbitrary-scale big-decimal Go type | 🟡 | scaled numerics scan to string/float |
| DecFloat NaN/Infinity special-value handling type | 🟡 | strings only |
| UUID type over CHAR/OCTETS | ❌ | — |
| `fbtype.Date` multi-layout parsing type | ❌ | `time.Time` only |
| SMALLINT scans to `int16` when scanning into `any` | ❌ | returns int64 |
| `column_name_to_lower` DSN option | ✅ | parity (firebirdsql-only feature, listed for completeness) |

## 10. SQL-layer conveniences

| Feature | Status | Notes |
|---|---|---|
| Named arguments: `NamedArgs` / `StrictNamedArgs` / `StructArgs` (`@name` → `?`, quote/comment-aware lexer) | ❌ | positional `?` only |
| Row collection helpers: `CollectRows[T]`, `CollectOneRow[T]`, `CollectExactlyOneRow[T]`, `RowTo[T]`, `RowToStructByPos/Name`, `RowToMap`, `ForEachRow`, `ScanRow`… | ❌ | — |
| `sql.Conn.Raw` bridge returning a rich native Conn | 🟡 | Raw exposes narrow feature accessors only |

## 11. Events

| Feature | Status | Notes |
|---|---|---|
| Event manager with dedicated connection + delivery worker (`fbevent.Manager`) | 🟡 | firebirdsql `FbEvent` covers subscribe/callback/channel |
| One-shot `WaitForEvent(ctx, name, timeout)` | 🟡 | expressible via channel+timer; no dedicated API |
| pgx-style `Listen`/`Unlisten`/`WaitForNotification` | ❌ | — |
| Attach manager to an existing connection (`ConnectFor`) | ❌ | separate connection only |

## 12. Services API

| Feature | Status | Notes |
|---|---|---|
| Backup with streaming verbose output + full option set (Factor, Expand, Zip, Compressor, Replace, NoDatabaseTriggers, ParallelWorkers…) | ✅ | firebirdsql BackupManager covers these |
| Drop database via services/`op_drop_database` | ❌ | missing (see §2) |
| Restore, NBackup, Maintenance (shutdown/online/validate/sweep/…), User management, Trace sessions, Statistics | ✅ | firebirdsql-only strengths (fbx has these as "planned") |

## 13. Red Database / Firebird 6 specifics

| Feature | Status | Notes |
|---|---|---|
| Schema-qualified identifiers (FB6/RDB): `FieldDescription.Schema`, schema in array metadata | ❌ | — |
| `Isc_dpb_search_path` / SET SEARCH_PATH aware statement-cache invalidation | ❌ | — |
| Red Database 5.1+/5+ backup options (Replace, Compressor plugin) | 🟡 | option structs exist in firebirdsql backup manager for zip/workers; Replace/Compressor parity to verify |
| Wire compression + wire encryption combined | ✅ | parity |

---

## Priority suggestion (impact/effort)

1. **Arrays** (`op_get_slice`/`op_put_slice` + scanning) — the largest real functionality gap; fbx proves the wire-level design.
2. **Statement cache + query exec modes** — biggest performance lever for real applications.
3. **NamedArgs** query rewriting — small, high ergonomic value.
4. **BLOB streaming** (stream BPB, buffer size option, lazy `io.ReadCloser` scan, `io.Reader` params) — removes whole-blob-in-memory limits.
5. **Connection-string modernization**: env vars, keyword/value strings, multi-host fallback, `target_session_attrs`, `connect_timeout`.
6. **zeronull package + row collectors** — pure-Go add-ons, zero wire changes.
7. **Raw TPB / lock timeout / savepoint nesting API / BeginFunc** — transaction ergonomics.
8. **Drop database / `op_drop_database`** and per-statement `SetTimeout`/`SetCursorFlags`/`SetInlineBlobSize`.
9. **Schemas (FB6/RDB)**, **extra SRP variants + pluggable auth**, **tracers/tracelog**, **Hijack/Construct**.
