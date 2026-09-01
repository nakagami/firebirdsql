## Test suite — tests, applicability and results

Legend: ✅ pass · ⏭️ skip (version/capability gate or server configuration) · blank = test not present on that run. Results from full-suite runs against local Firebird 2.5.9 / 3.0 / 4.0 / 5.0.5 instances.

### Test harness self-checks — testutil_selftest_test.go

| Test | What it verifies | FB 2.5 | FB 3.0 | FB 4.0 | FB 5.0 |
|---|---|:-:|:-:|:-:|:-:|
| `TestTestUtilServerVersion` | Server version detection via the Services API (cached, env-overridable) | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestTestUtilCreateDatabaseWithDDL` | Throwaway database creation + DDL execution + file cleanup | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestTestUtilMustExecRowsAffected` | mustExec helper and RowsAffected reporting | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestTestUtilErrorMatchers` | requireGDSError / requireSQLCode / requireSQLState matchers on a real constraint violation | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestTestUtilProtocolGate` | negotiatedProtocol + requireProtocol gates pass or skip, never fail | ⏭️ skip | ⏭️ skip | ⏭️ skip | ⏭️ skip |
| `TestTestUtilVersionGates` | Firebird version gates (pass on new servers, skip on old) | ⏭️ skip | ⏭️ skip | ⏭️ skip | ⏭️ skip |
| `TestTestUtilUserFixture` | Services-API user fixture: create → authenticate → grant → auto-drop | ⏭️ skip | ✅ pass | ✅ pass | ✅ pass |

### Datatype round trips — datatype_test.go

| Test | What it verifies | FB 2.5 | FB 3.0 | FB 4.0 | FB 5.0 |
|---|---|:-:|:-:|:-:|:-:|
| `TestDatatypeMatrix/BLOB_SUB_TYPE_BINARY_round_trip` | BLOB SUB_TYPE BINARY: empty/NULL/2-byte/1 MB random blob round trips | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/BLOB_SUB_TYPE_TEXT_round_trip` | BLOB SUB_TYPE TEXT: text/unicode/100 KB round trips | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/BOOLEAN_all_values` | BOOLEAN true/false/NULL round trips | ⏭️ skip | ✅ pass | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/CHAR_trailing_space_trim` | CHAR(10) values return trailing-blank trimmed (Jaybird TrimmableField parity) | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/DATE_boundaries` | DATE 0001-01-01 / 9999-12-31 / today + NULL | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/DECFLOAT(16)` | DECFLOAT(16) parameterized round trips (string binding) | ⏭️ skip | ⏭️ skip | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/DECFLOAT(34)` | DECFLOAT(34) parameterized round trips | ⏭️ skip | ⏭️ skip | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/DOUBLE_boundaries` | DOUBLE PRECISION ±max/0/NULL | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/FLOAT_round_trip` | FLOAT: float32-precision values incl. near-max | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/INT128_extremes` | INT128 ±(2^127−1−1) via string binding | ⏭️ skip | ⏭️ skip | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/INTEGER_boundaries` | INTEGER −2^31 / 2^31−1 / 0 / NULL | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/NUMERIC(18,2)_extremes` | NUMERIC(18,2) ±999999999999999.99 | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/NUMERIC(38,2)_extremes` | NUMERIC(38,2) ±(10^36−1)/100 (Firebird 4+) | ⏭️ skip | ⏭️ skip | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/NUMERIC(9,2)` | NUMERIC(9,2) decimal string round trips | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/SMALLINT_boundaries` | SMALLINT −32768/32767/0/NULL | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/BIGINT_boundaries` | BIGINT ±2^63 boundaries / NULL | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/TIME_WITH_TIME_ZONE_instant_preserved` | TIME WITH TIME ZONE keeps the instant (session-zone render) | ⏭️ skip | ⏭️ skip | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/TIME_boundaries` | TIME 00:00:00.0000 / 23:59:59.9999 / NULL | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/TIMESTAMP_WITH_TIME_ZONE_instant_preserved` | TIMESTAMP WITH TIME ZONE instant preserved across zones | ⏭️ skip | ⏭️ skip | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/TIMESTAMP_boundaries` | TIMESTAMP min/max/leap-day + NULL | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDatatypeMatrix/VARCHAR_values` | VARCHAR: empty/unicode/3000-char/NULL | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDatatypeColumnMetadata` | ColumnTypes metadata: names, database type names, scan types, nullability, precision/scale, length | ⏭️ skip | ✅ pass | ✅ pass | ✅ pass |
| `TestSessionTimeZone` | DSN ?timezone= drives CURRENT_TIMESTAMP; naive columns keep wall clock across zones; aware columns keep instants | ⏭️ skip | ⏭️ skip | ✅ pass | ✅ pass |
| `TestCharsetRoundTrip` | Text round trips over WIN1251/WIN1252/UTF8 connection charsets | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSqlCounts` | Exact insert/update/delete/no-op affected-row counts | ✅ pass | ✅ pass | ✅ pass | ✅ pass |

### Statements, procedures, transactions — statement_test.go

| Test | What it verifies | FB 2.5 | FB 3.0 | FB 4.0 | FB 5.0 |
|---|---|:-:|:-:|:-:|:-:|
| `TestStatementLifecycle` | Transactional DDL commit/rollback, prepared statement reuse, closed-statement errors | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestStoredProcedureCalls` | Executable + selectable procedures, NULL in/out params, exception propagation | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestReturningEdgeCases` | Single/multi-row UPDATE+DELETE RETURNING, unknown RETURNING column; per-version singleton semantics | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestTransactionIsolation` | Two-connection visibility, REPEATABLE READ snapshot, read-only tx refuses writes | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSavepoints` | SAVEPOINT / ROLLBACK TO / nested / RELEASE via SQL | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestAutoCommitSemantics` | Autocommit visible immediately; explicit tx invisible until commit | ✅ pass | ✅ pass | ✅ pass | ✅ pass |

### BLOB/CLOB lifecycle — blob_test.go

| Test | What it verifies | FB 2.5 | FB 3.0 | FB 4.0 | FB 5.0 |
|---|---|:-:|:-:|:-:|:-:|
| `TestBlobSegmentBoundaries` | Blob sizes 1 → 100 000 bytes across the 32 K segment boundary | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestBlobEdgeCases` | NULL vs empty (both subtypes), cross-connection visibility, rollback, parameter reuse | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestClobLargeText` | Multi-segment unicode CLOB round trip | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestInlineBlobTempDB` | Inline-blob DSN options: enabled/disabled/oversized (needs protocol 19; skips otherwise) | ⏭️ skip | ⏭️ skip | ⏭️ skip | ⏭️ skip |

### Events — event_parity_test.go

| Test | What it verifies | FB 2.5 | FB 3.0 | FB 4.0 | FB 5.0 |
|---|---|:-:|:-:|:-:|:-:|
| `TestEventChanDelivery` | Channel subscriber receives per-event counts | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestEventMultipleSubscribers` | Both subscribers receive the same posted event | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestEventLargeLoad` | 500 rapid posts fully accounted for (count coalescing) | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestEventUnsubscribeStopsDelivery` | Silence after Unsubscribe; fresh subscription still delivers | ✅ pass | ✅ pass | ✅ pass | ✅ pass |

### Services API — services_parity_test.go

| Test | What it verifies | FB 2.5 | FB 3.0 | FB 4.0 | FB 5.0 |
|---|---|:-:|:-:|:-:|:-:|
| `TestBackupOptionsMatrix` | Metadata-only backup, WithReplace restore, restore-over-existing failure without it | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestBackupRestoreBlobIntegrity` | 660 KB streamed blob survives backup/restore byte-for-byte | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestStatisticsManagerReports` | Header-page and table-scoped statistics report content | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestMaintenanceAccessModeParity` | Read-only database refuses writes; read-write accepts | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestServiceManagerSweepParity` | Sweep completes; database stays usable | ✅ pass | ✅ pass | ✅ pass | ✅ pass |

### Protocol gating — protocol_gate_test.go

| Test | What it verifies | FB 2.5 | FB 3.0 | FB 4.0 | FB 5.0 |
|---|---|:-:|:-:|:-:|:-:|
| `TestNegotiatedProtocolVersion` | Negotiated protocol within the advertised 10–17 range | ⏭️ skip | ⏭️ skip | ⏭️ skip | ⏭️ skip |
| `TestCancelOperationParity` | Context cancellation aborts a long statement; pool stays usable | ✅ pass | ✅ pass | ✅ pass | ✅ pass |

### Additions to existing files

| Test | What it verifies | FB 2.5 | FB 3.0 | FB 4.0 | FB 5.0 |
|---|---|:-:|:-:|:-:|:-:|
| `TestGdsToSQLStateTable` | GDS code → SQLSTATE fallback mapping incl. unmapped codes | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestGdsToSQLCodeTable` | GDS code → SQLCODE fallback mapping incl. unmapped codes | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestFirebirdVersionEqualOrGreaterMatrix` | Version-banner comparisons against feature boundaries (2.5…5.0) | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestFirebirdVersionEqualOrGreaterPatch` | Patch-level version comparisons | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDSNIPv6Formats` | Bracketed IPv6 hosts keep/append the default port | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDSNOptionsDefaults` | Every documented DSN option resolves to its default | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDSNOptionsOverridesAndAliases` | Explicit options win over defaults | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDSNOptionsFailFast` | Invalid wire_crypt / auth plugin config fails at parse time | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestTimezoneLegacyAliases` | GMT/ACT/AET/AGT/ART/AST legacy zone aliases round-trip | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestTimezoneMapInvalidEntries` | Unknown zone names/ids yield zero values, never a wrong zone | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestAdvertisedProtocolRange` | Connect packet offers protocols 10–17, weights ascending, pflag only with compression | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `FuzzParseDSN` | DSN parser never panics on hostile input (seed corpus) | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestLegacyAuthWireCrypt` | Legacy auth + wire-crypt policy combinations (skips plaintext case when the server requires encryption) | ✅ pass | ⏭️ skip | ✅ pass | ✅ pass |


## Pre-existing suite

All tests that already existed in the repository (connection/URL handling, wire-protocol parse hardening and fuzz targets, compression, SRP, wire-crypt policy, events plumbing, service/backup/user manager coverage, and the GitHub-issue regression tests) are unchanged by this PR except:

- `TestLegacyAuthWireCrypt` — skips the `wire_crypt=false` case on servers that reject plaintext connections.
- `TestServiceManager_Info` — the `GetSvrDbInfo` attachment listing is logged instead of asserted (server-configuration dependent).
- Service-manager, backup, nbackup and user-manager tests — gated on Services API availability so they skip (fast) instead of hanging on servers without it.
