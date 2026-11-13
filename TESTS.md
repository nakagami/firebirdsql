## Test suite — tests, applicability and results

Legend: ✅ pass · ⏭️ skip (version/capability gate or server configuration) · blank = test not present on that run. Results from full-suite runs against local Firebird 2.5.9 / 3.0 / 4.0 / 5.0.5 instances.

### Test harness self-checks — testutil_selftest_test.go

| Test | What it verifies | FB 2.5 | FB 3.0 | FB 4.0 | FB 5.0 |
|---|---|:-:|:-:|:-:|:-:|
| `TestTestUtilServerVersion` | Server version detection via the Services API (cached, env-overridable) | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestTestUtilCreateDatabaseWithDDL` | Throwaway database creation + DDL execution + file cleanup | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestTestUtilMustExecRowsAffected` | mustExec helper and RowsAffected reporting | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestTestUtilErrorMatchers` | requireGDSError / requireSQLCode / requireSQLState matchers on a real constraint violation | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestTestUtilProtocolGate` | negotiatedProtocol + requireProtocol gates pass or skip, never fail | ⏭️ skip | ⏭️ skip | ⏭️ skip | ✅ pass |
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
| `TestEmptyBinaryBlobNotNil` | Empty binary BLOB reads as empty, not nil, on `[]byte`/`sql.Null`/`any`, cross-checked with `IS NULL`/`OCTET_LENGTH` (`inline` subtest needs protocol 19; skips otherwise) | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
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
| `TestNegotiatedProtocolVersion` | Negotiated protocol within the advertised 10–19 range | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestFeatureProtocolGating/batch_(protocol_16+)` | PrepareBatch insert behind the protocol-16 gate | ⏭️ skip | ⏭️ skip | ✅ pass | ✅ pass |
| `TestFeatureProtocolGating/scrollable_cursors_(protocol_18+)` | First/Absolute/Relative/Last/Prior/Next return the expected rows; Next past the end returns io.EOF | ⏭️ skip | ⏭️ skip | ⏭️ skip | ✅ pass |
| `TestCancelOperationParity` | Context cancellation aborts a long statement; pool stays usable | ✅ pass | ✅ pass | ✅ pass | ✅ pass |

### Deferred cursor close — deferred_close_live_test.go

| Test | What it verifies | FB 2.5 | FB 3.0 | FB 4.0 | FB 5.0 | FB 6.0 |
|---|---|:-:|:-:|:-:|:-:|:-:|
| `TestDeferredCursorClosePacketCount` | A query on a prepared statement writes exactly 2 packets in a transaction and 3 in autocommit mode: the cursor close rides with the next request (counted under the driver's write buffer; skips on a non-lazy port type) | ✅ pass | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDeferredCursorCloseFollowedBy` | Every request that can follow a queued close on one connection (query, blob read, statement error, insert + commit, rollback, drop, prepare, ping, connection close) gets its own response, never a skipped one | ✅ pass | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestDeferredCursorCloseConcurrent` | Queries and updates from 8 goroutines over a 4-connection pool, deferred closes interleaving with other requests | ✅ pass | ✅ pass | ✅ pass | ✅ pass | ✅ pass |

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
| `TestDSNOptionsFailFast` | Invalid wire_crypt / auth plugin / page_size config fails at parse time | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestParseDSNPageSize` | page_size defaults to empty (server default); an explicit or empty value is passed through | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestParseDSNInvalidPageSize` | Sizes outside 4096/8192/16384/32768 (including 0) and non-numeric values fail at parse time | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestCreateDatabasePageSize` | `firebirdsql_createdb` creates `mon$page_size` 16384 for `page_size=16384`, 4096 for `page_size=4096` (8192 on 6.0) and the server default for an unset or empty `page_size` (4096 on 2.5, 8192 on 3.0+) | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestParsePageSize` | An empty page_size maps to 0 (no `isc_dpb_page_size`), a valid size passes through, 0 and invalid values fail | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestCreateDPBPageSize` | The op_create DPB carries `isc_dpb_page_size` (little-endian) only for a set page size; the other items stay intact | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestCreateDPBUnchangedWithPageSize` | With `page_size=4096` the op_create DPB is byte-identical to the one sent before the page size became optional, except for the password, which goes as its hash below protocol 13 | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestServiceAttachDeferredAuthData` | After a canned connect reply, the op_service_attach packet carries a deferred SRP proof under the SPB tag (111) and no password, and no proof after plain `op_accept` | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestAuthPluginListHardened` | `auth_plugin_list=Srp256,Srp` authenticates through SRP at protocol 13+ and is refused below it, naming the list | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestParseConnectResponse_SuccessWithoutAccept` | A success `op_response` to `op_connect` is an error, whatever `wire_crypt` says | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestParseConnectResponse_SuccessDuringContAuth` | A success `op_response` to `op_cont_auth` after `op_cond_accept` is refused under `wire_crypt=required`, and counts as authenticated otherwise | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestParseConnectResponse_RefusalKeepsServerError` | The server's own error still comes back from both of those places | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachBlockCarriesEmptyItem` | After a plugin switch, attach, create and service blocks carry one empty specific-data item | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachFullSequence` | Exact bytes of the exchange after the attach, for `Srp` and `Srp256`, and for an `op_cont_auth` that names no plugin; the keys don't outlive the attach reply | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachServerSkipsPublicKeyRequest` | A server that sends the salt and B straight away gets the proof and no public key | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachRequiredSendsNothing` | `wire_crypt=required` refuses a plugin switch and sends nothing | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachServerErrorStaysFbError` | The server's `isc_login` after any step comes back as `FbError`, and the exchange state is gone | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachUsesUserAndPluginList` | The exchange hashes the upper-cased user name and sends the client's own plugin list | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachRespectsAllowList` | A server selecting a plugin outside `auth_plugin_list` is refused and gets nothing | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestCondAcceptEmptyDataSendsPublicKeyInHex` | On `op_cond_accept` with no data, the public key goes out hex-encoded | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachRogueServer` | Endless continuations, bad lengths and a continuation naming another plugin end in an error, with at most one public key and one proof sent, and no hash or password. A server public key B that isn't valid hex isn't refused; that case ends only because the script does | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachLaterContAuthGetsNoProof` | An `op_cont_auth` after the attach reply gets an error and no reply | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachNotSetOnOtherPaths` | At protocol 13, `op_accept_data` with data, `op_cond_accept`, `Legacy_Auth` and `op_accept` leave no exchange behind | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestLegacyHashIsRawAtEverySink` | The `Legacy_Auth` hash is raw in `op_cont_auth` and in the attach, create and service blocks | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachStallRespectsDeadline` | A server going quiet at any step can't outlive the context, on any entry point | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachCancelInterrupts` | A cancel with no deadline ends the stall at any step | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachSocketClose` | The peer closing the socket at any step is a plain failed connect | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachCompletesOverSocket` | The whole exchange through every entry point against a fake server | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachEventOwnerCloseEndsStall` | Closing the event owner ends the exchange at any step | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestSrpAfterAttachLive` | Create, attach, query and service attach through a real plugin switch. Its subtests skip unless the server switches, and the parent then reports pass. CI's default `AuthServer` doesn't switch; the `3-srp` CI job (FB 3, `AuthServer = Srp`) does, and sets `FIREBIRDSQL_REQUIRE_PLUGIN_SWITCH=1` so a missing switch fails instead of skipping. Any other handshake error fails it | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestUnknownUserWithoutCipherLive` | Without wire encryption, an unknown user gets a login failure and not `isc_unavailable`, on attach and service. On CI's default `AuthServer` (`Srp256, Srp, Legacy_Auth`) that failure is the client's refusal to move on to `Legacy_Auth`; with `AuthServer = Srp` it is `isc_login` | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestParseConnectResponse_LegitimateAuthFlows` | The replies real servers send still complete the handshake | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestWireCryptRequiredLegacyPluginSendsNoHash` | Under `wire_crypt=required`, a server selecting `Legacy_Auth` is refused before the hash goes out | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestConnectSuccessResponseSendsNoAttach` | Through all three entry points against a fake server, a success reply to `op_connect` draws no attach request | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestLegacyPluginHashInConnectPacket` | Documented limit: `auth_plugin_name=Legacy_Auth` puts the hash in `op_connect`, even under `wire_crypt=required` | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestLegacyPluginSendsHashNotPassword` | A server selecting `Legacy_Auth` without `wire_crypt=required` gets the raw hash, never the password | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestAttachBuildersOmitPassword` | At protocols 13, 16 and 19, no attach block builder sends a password item, for a 300-byte password too | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestLegacyPasswordHash` | The hash matches fixed vectors, and only the first 8 bytes and the low 7 bits of each count | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestAttachBuildersBelow13` | Below protocol 13, each builder sends exactly one `password_enc` item and no cleartext | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestPasswordNotOnWire` | Through all three entry points against a fake server, neither the password nor the hash leaves at protocol 13; at 12 only the hash does, and `wire_crypt=required` refuses it on attach and create | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestBelow13RequiresLegacyAuth` | Below protocol 13, every accept opcode is refused before anything is sent if `auth_plugin_list` omits `Legacy_Auth` | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestListWithoutLegacyAuthConnectsAt13` | From protocol 13, a list that omits `Legacy_Auth` isn't refused, on any accept opcode | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestBelow13RefusalSendsNoAttach` | That refusal draws no attach request; documented limit: the service manager always allows `Legacy_Auth` | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestUserManagerPasswordIsRequestData` | Documented limit: a password changed through `UserManager` is in the request itself | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestPasswordOnWireLive` | Reads real plaintext logins off the wire through a relay: create, attach and service attach send no password at any protocol, and `password_enc` below 13 | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestServiceManagerWireCryptFalse` | A service attach without wire crypt authenticates and round-trips; skips when the Services API is unavailable or the server requires wire encryption | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestTimezoneLegacyAliases` | GMT/ACT/AET/AGT/ART/AST legacy zone aliases round-trip | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestTimezoneMapInvalidEntries` | Unknown zone names/ids yield zero values, never a wrong zone | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestAdvertisedProtocolRange` | Connect packet offers protocols 10–19, weights ascending, pflag only with compression | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `FuzzParseDSN` | DSN parser never panics on hostile input (seed corpus) | ✅ pass | ✅ pass | ✅ pass | ✅ pass |
| `TestLegacyAuthWireCrypt` | Legacy auth + wire-crypt policy combinations, including `auth_plugin_name=Legacy_Auth` with `wire_crypt=false` (skips the plaintext steps when the server requires encryption) | ✅ pass | ✅ pass | ✅ pass | ✅ pass |


## Pre-existing suite

All tests that already existed in the repository (connection/URL handling, wire-protocol parse hardening and fuzz targets, compression, SRP, wire-crypt policy, events plumbing, service/backup/user manager coverage, and the GitHub-issue regression tests) are unchanged by this PR except:

- `TestLegacyAuthWireCrypt` — skips the `wire_crypt=false` case on servers that reject plaintext connections.
- `TestServiceManager_Info` — the `GetSvrDbInfo` attachment listing is logged instead of asserted (server-configuration dependent).
- Service-manager, backup, nbackup and user-manager tests — gated on Services API availability so they skip (fast) instead of hanging on servers without it.
