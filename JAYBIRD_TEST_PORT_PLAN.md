# Jaybird Test Inventory & Test Port Plan for `firebirdsql` (Go)

Generated: 2026-08-27
**Status: COMPLETE** — all 10 phases implemented (Phases 1–10); the full suite verified against Firebird 2.5, 3.0, 4.0 and 5.0 (see matrix below). Driver fixes that fell out of the port: IPv6 default-port DSN handling, TIME/TIMESTAMP WITH TIME ZONE wire codec + nil-location panic, DSN parse out-of-range panic.

### Cross-version validation (local HQbird instances)

| Server | Port | Negotiated protocol | Result | Notes |
|---|---|---|---|---|
| Firebird 5.0.5 | 3055 | 19 | ✅ green | reference server; scroll + inline blobs + batch exercised |
| Firebird 4.0 | 3054 | 17 | ✅ green | batch exercised; scroll/inline skip (protocol gate) |
| Firebird 3.0 (HQbird) | 3053 | 15 | ✅ green | Services API unavailable → service tests skip via the 15 s attach guard; `wire_crypt=false` connections rejected by server → plaintext case skips |
| Firebird 2.5.9 HQbird | 3052 | 11 | ✅ green | legacy auth; multi-row RETURNING raises "multiple rows in singleton select" (asserted per version); table identifiers capped at 31 bytes in the matrix |

Known FB4-and-older finding: multi-row `UPDATE/DELETE … RETURNING` fails with "multiple rows in singleton select" (executed as singleton returning) — candidate driver improvement (execute with the cursor flag).

Sources analyzed:

- **Jaybird** (Firebird JDBC driver, Java): `E:\Projects_2026\jaybird` — `src/test`, `src/jna-test`, `chacha64-plugin`
- **firebirdsql** (Firebird driver for Go): `E:\Projects_2026\firebirdsql`

Contents:

1. [How Jaybird tests are organized](#1-how-jaybird-tests-are-organized)
2. [Jaybird test inventory (all test classes)](#2-jaybird-test-inventory)
3. [Current test coverage in firebirdsql (Go)](#3-current-test-coverage-in-firebirdsql-go)
4. [Plan: implement Jaybird-equivalent tests in Go](#4-plan-implement-jaybird-equivalent-tests-in-go)
5. [Tests for features that do NOT exist in the Go driver](#5-tests-for-features-that-do-not-exist-in-the-go-driver)

---

## 1. How Jaybird tests are organized

- **Framework**: JUnit 5 (Jupiter), AssertJ + Hamcrest assertions, Mockito for mock-based unit tests, Awaitility for async waits.
- **~345 test classes, ≈3,180 `@Test` methods** (many parameterized, so the executed test count is far higher).
- **Integration vs unit**: most JDBC-level tests are integration tests against a live Firebird server. Wire-level and utility classes are pure unit tests (Mockito / fake objects / byte-buffer driven).
- **Shared infrastructure** (`src/test/org/firebirdsql/common`):
  - `FBTestProperties` — connection config from system properties (server, user, password, GDS type: pure Java wire / native / embedded).
  - `UsesDatabaseExtension` — creates/drops a throwaway database per test or per class via `FBManager`; `DdlHelper` seeds fixtures.
  - `RequireFeatureExtension` / `RequireProtocolExtension` — skip when the server lacks a feature or protocol version.
  - `GdsTypeExtension` — restrict/exclude native or embedded backends; `DatabaseUserExtension` — creates test users; `RunEnvironmentExtension` — requires locally path-mapped databases (services/backup tests).
- **Behavioral suites via inheritance**: generic suites (`AbstractStatementTest`, `AbstractTransactionTest`, `AbstractStatementTimeoutTest`, `BaseTestInputBlob`/`BaseTestOutputBlob`) are re-run for every wire protocol version (`V10StatementTest → V11 → V12 → V13 → V15 → V16 → V18 → V19`) and for the native JNA backend.
- Source sets: `src/test` (main), `src/jna-test` (native client library backend), `chacha64-plugin` (separate Gradle subproject for the ChaCha64 wire-crypt plugin).

### Overview by area

| Area | Test classes | `@Test` methods |
|---|---:|---:|
| `common` (helpers) | 2 | 1 |
| `gds` root + `gds.impl` (GDS factory, URLs, versions, DPB) | 9 | ~55 |
| `gds.ng` core (protocol objects, datatype coders, errors, ODS…) | 18 | 211 |
| `gds.ng` subpackages (dbcrypt, fields, listeners, tz) | 20 | 146 |
| `gds.ng.wire` (inline blobs, protocol collection, connection) | 4 | 51 |
| `gds.ng.wire.auth` + `crypt` | 6 | 11 |
| Wire protocol version suites V10–V19 | 69 | 83 |
| `jdbc` root (Connection/Statement/ResultSet/BLOB/metadata suites) | 100 | 942 |
| `jdbc.field` (datatype conversion layer) | 19 | 1,035 |
| `jdbc.escape` (JDBC escape syntax) | 20 | 94 |
| `jdbc.metadata` (pattern/SQL helpers) | 4 | 15 |
| `management` (Services API managers) | 9 | 90 |
| `ds` + `xca` (DataSources, pooling, XA) | 17 | 90 |
| `encodings` | 4 | 29 |
| `event` (EventManager) | 1 | 13 |
| `jaybird.parser` (SQL parser) | 11 | 54 |
| `jaybird.props` (connection property registry) | 4 | 26 |
| `jaybird.util` | 13 | 65 |
| `util` (NumericHelper) | 1 | 2 |
| `jna-test` (native backend) | 13 | 66 |
| `chacha64-plugin` | 1 | 4 |
| **Total** | **345** | **≈3,180** |

---

## 2. Jaybird test inventory

### 2.1 `org.firebirdsql.gds` root & `gds.impl`

| Class | What it tests |
|---|---|
| MessageTemplateTest | Lookup of message templates by error code (Jaybird and Firebird ranges), parameter counting, message formatting, SQL state default/override, error-info suffix. |
| ReconnectTransactionTest | `GDSHelper.reconnectTransaction` reattaches an in-limbo prepared transaction after database restart; verified via `RDB$TRANSACTIONS`. |
| VaxEncodingTest | `iscVaxInteger`/`iscVaxLong`/`iscVaxInteger2` VAX-style integer decoding across byte widths/offsets + round trips. |
| DatabaseParameterBufferImpTest | DPB metadata; a version 1 DPB is upgraded to version 2 when an argument too long for DPB1 is added. |
| DbAttachInfoTest | Parameterized parse of connect strings: server/port/path variants (IPv6, drive letters, defaults) and invalid URL errors. |
| GDSFactoryTest | Parameterized mapping of JDBC URL prefixes (pure java / native / embedded / local) to GDS type. |
| GDSHelperTest | `GDSHelper.compareToVersion` against the real server version. |
| GDSServerVersionTest | Parsing of server version strings (1.5–3.0 variants, compression, SPARC, missing extended info, invalid), equality/`isEqualOrAbove`. |
| SpecialEmbeddedServerUrlsTest | Embedded-only: URLs without server/port for FBManager and DriverManager connections. |

### 2.2 `org.firebirdsql.gds.ng` core

| Class | What it tests |
|---|---|
| AbstractStatementTest | *Abstract* generic FbStatement suite (re-run per protocol version + JNA): describe/execute/fetch, stored procedures, INSERT RETURNING, execution plans, cursor close states, error recovery, timeouts, MaxFieldSize. |
| AbstractStatementTimeoutTest | *Abstract* Firebird 4 statement-timeout suite: execute within timeout, timeout between execute and fetch, reuse after timeout, interleaving. (V16/V18/V19 + JNA). |
| AbstractTransactionTest | *Abstract* generic FbTransaction suite: commit, rollback, prepare+commit, prepare+rollback, starting a transaction with an SQL statement. |
| CachedInfoResponseTest | CachedInfoResponse: construction, copy semantics, filtering cached items against requested ones. |
| DefaultDatatypeCoderTest | Offline encode/decode of short/int/long/float/double/string/boolean/java.time incl. nulls, decimal64/128, int128; coder caching. |
| DefaultDatatypeCoderMockTest | Mockito: DefaultDatatypeCoder delegates string/streams to the encoding factory. |
| EncodingSpecificDatatypeCoderTest | Encoding-specific coder delegation, switching, unwrap, factory access. |
| FbConnectionPropertiesTest | Connection properties accessors (database, server, user, encoding, timeouts, wireCrypt, auth plugins), copy, immutable view, session timezone normalization, system-property defaults. |
| FbDatabaseOperationTest | Operation tracking: execute/fetch notifications, cancellation allowed while open, refused once closed. |
| FbExceptionBuilderTest | Exception/warning building from error codes, parameter substitution, causes, SQL state override, chaining, type upgrade. |
| FbServicePropertiesTest | Service properties `enableProtocol` default from system property. |
| JaybirdBlobBackupProblemTest | Regression: backup (gbak) of a database with streamed blobs — "segment buffer length shorter" problem. |
| OdsVersionTest | ODS version round trips, bounds, withMajor/withMinor, comparisons, error cases. |
| OperationMonitorTest | OperationMonitor: operation start/end notifications, cancellation during execute and fetch. |
| ServerVersionInformationTest | ODS major/minor → ServerVersionInformation constants mapping. |
| ServicesAPITest | Services API: service attach/detach, full database backup + restore. |
| SqlCountHolderTest | Insert/update/delete/select count storage incl. Integer.MAX_VALUE overflow wrapping. |
| TransactionHelperTest | Transaction state validation helpers. |

Helpers (not tests): `BaseTestBlob`, `BaseTestInputBlob`, `BaseTestOutputBlob` (generic blob suites), `EmptyProtocolDescriptor`, `Simple*Listener` fakes.

### 2.3 `gds.ng` subpackages — dbcrypt, fields, listeners, tz

| Class | What it tests |
|---|---|
| DbCryptDataTest | DbCryptData: null plugin data, 32767-byte limit, reply size range, createReply semantics. |
| StaticValueDbCryptCallbackSpiTest | Fixed-response db-crypt callback SPI: null/base64/base64url/plain config values. |
| StaticValueDbCryptCallbackTest | Callback returns configured fixed reply for null and non-null requests. |
| FieldDescriptorTest | FieldDescriptor datatype coder selection (default vs encoding-specific for CHAR/VARCHAR/BLOB TEXT per charset). |
| RowDescriptorBuilderTest | RowDescriptorBuilder: empty/single/copyFrom/resetField/chained adds, dbkey detection, incomplete descriptors. |
| RowValueTest | RowValue factories, field init tracking, count-mismatch errors, reset, deep copy. |
| DatabaseListenerDispatcherTest | Listener dispatch (detaching/detached/warning), per-listener exception isolation, shutdown suppression. |
| ServiceListenerDispatcherTest | Same dispatch suite for service listeners. |
| StatementListenerDispatcherTest | Dispatch of rows/beforeFirst/afterLast/executed/stateChanged/warnings/sqlCounts with exception isolation. |
| TransactionListenerDispatcherTest | Transaction state dispatch + exception isolation. |
| TimeZoneCodecAbstract*Test (6 classes) | Encode/decode of TIME/TIMESTAMP WITH TIME ZONE via network/big/little-endian coders; extended (FB4) vs standard (FB3) formats. |
| TimeZoneDatatypeCoderTest | Codec lookup throws for every non-tz SQL type. |
| TimeZoneMappingTest | Invalid/out-of-range zone ids and offsets fall back to UTC/zero. |
| TimeZoneByNameMappingTest | Exhaustive mapping of every Firebird/ICU zone id to java.time zones and back (incl. legacy aliases). |
| TimeZoneOffsetMappingTest | Round trips for offset zones and offset minutes. |

### 2.4 `gds.ng.wire` — connection, inline blobs, protocol registry, auth, crypt

| Class | What it tests |
|---|---|
| InlineBlobCacheTest | Inline blob cache: size limits, add/getAndRemove, transaction/blob identity rules, invalidation, detach clearing. |
| InlineBlobTest | InlineBlob handle allocation/wraparound, open/close/reopen, EOF, getSegment, seek modes, unsupported output ops. |
| ProtocolCollectionTest | Protocol descriptors by classpath, sorted versions, create-by-version, programmatic filtering. |
| WireDatabaseConnectionTest | identify() against real server, connect timeouts to nonexistent IP, XDR stream creation, custom socket factories. |
| ClientAuthBlockNormalizeLoginTest | Login normalization: unquoted upper-casing, quoted case-sensitivity, escaped quotes, invalid escapes. |
| Firebird3PlusAuthenticationTest | Live: DB + service attach with Legacy_Auth and SRP (protocol 13+). |
| SrpClientTest | SRP-6a handshake: client proof, client/server session key agreement (SYSDBA/masterkey). |
| EncryptionInitInfoTest | Encryption init success/failure instances, identifier/cipher handling, null validation. |
| Arc4EncryptionPluginSpiTest | Arc4 plugin identifier; supported for protocols 13, 15, 16, 18. |
| ChaChaEncryptionPluginSpiTest | ChaCha plugin identifier; supported for protocols 16/18, not 13/15. |

### 2.5 Wire protocol version suites (V10–V19)

**Pattern**: `VnXTest extends V(n-1)XTest`, so every version re-runs the accumulated suite; each subclass only re-points the protocol version and re-registers `RequireProtocolExtension(n)` (skip if server doesn't negotiate it). No version 14 and no version 17 (skipped by Firebird). Genuine deltas only where a version added a feature.

| Version | Feature added (from package-info/docs) | Suite deltas beyond inherited |
|---|---|---|
| V10 (FB 1.0+) | Base protocol | 17 DB tests (attach/double/failed, create+drop, close, warnings, cancelOperation abort-only, executeImmediate, ODS), 10 event tests, 5+3 blob mock tests, generic blob/statement/transaction suites, 2 service tests, 6 wireOperations tests |
| V11 (FB 2.1+) | Async/deferred row fetch | +7 async-fetch tests (`testAsyncFetchRows_*`) |
| V12 (FB 2.5+) | `op_cancel` raise/disable/enable | +3 cancelOperation tests (inherited V10 "unsupported" tests disabled) |
| V13 (FB 3.0+) | Auth framework (SRP) + wire encryption (Arc4/ChaCha) | none of its own (exercised via attach) |
| V15 (FB 3.0.2+) | Revised db-crypt key callback wire format | none of its own |
| V16 (FB 4.0+) | Batch operations (`op_batch`) + statement timeout | +7 batch tests (execute, single/multi errors, cancel followed by execute, too large, release); + statement-timeout suite (4 tests) |
| V18 (FB 5.0+) | Scrollable cursors (`op_fetch_scroll`, cursor info) | +18 scroll tests (getCursorInfo, fetchScroll next/prior/absolute/relative/first/last, boundaries) |
| V19 (FB 5.0.3+) | Inline blobs (`op_inline_blob`, DPB 103/104) | +5 inline-blob tests (default/max sizes, cache size zero) |

Full file list (each file = `Database|EventHandling|InputBlob|InputBlobMock|OutputBlob|OutputBlobMock|Service|Statement|Transaction|WireOperations` per version; V16/V18/V19 add `StatementTimeoutTest`):
`version10/` (10 files), `version11/` (8), `version12/` (8), `version13/` (8), `version15/` (8), `version16/` (9), `version18/` (9), `version19/` (9) = **69 files**. Pure unit (no server): `V10InputBlobMockTest`, `V10OutputBlobMockTest`, `V10WireOperationsTest` (+inherited copies); everything else needs a live server.

### 2.6 `org.firebirdsql.jdbc` — connection & URL handling

| Class | What it tests |
|---|---|
| FBConnectionTest | Broad FBConnection suite: charsets, wire crypt/compression/auth, IPv6, network timeout, abort, read-only, transaction settings, process id/name, holdability, lock/alter table. |
| FBConnectionPropertiesTest | Parameterized parsing of connection properties, non-standard properties, invalid value formats. |
| ConnectionPropertiesTest | `defaultIsolation`/`isolation` handling on DataSource and DriverManager property objects. |
| FBDriverTest | FBDriver: acceptsURL/connect, URL-encoded properties, property normalization, transaction config via URL, auth, warnings, rollback on close. |
| DatabaseUrlFormatsTest | All supported JDBC URL formats connect via DriverManager and SimpleDataSource. |
| JDBCUrlPrefixTest | Alternative URL prefixes map to expected GDS type. |
| FBUnmanagedConnectionTest | Basic unmanaged connection: commit, autocommit, createStatement, metadata, type map, nativeSQL. |
| FBConnectionTimeoutTest | Connection-level timeout (FB3+) from property or DriverManager. |
| CreateDatabaseIfNotExistTest | `createDatabaseIfNotExist`: DB created when absent, not created by default, privileged vs non-privileged user. |
| DatabaseEncryptionTest | Connecting to database-encrypted DBs with callback/base64 keys (needs pre-made encrypted aliases `crypttest`/`cryptsec`). |
| ReconnectTest | Legacy scenario: create/populate 10 related tables, alter cascade rules, read metadata. |
| FBConnectionSchemaTest | setSchema/getSchema: default PUBLIC, SYSTEM restrictions, search path lists, no-schema errors. |
| FBConnectionClientInfoPropertiesTest | Client info get/set via ClientInfoProvider, invalid values, pooled resets. |
| ClientInfoPropertyTest | Unit: ClientInfoProperty parsing, null-argument handling. |

### 2.7 `org.firebirdsql.jdbc` — statements & execution

| Class | What it tests |
|---|---|
| FBStatementTest | Main Statement suite: execute variants, escape processing, execution plans, fetch direction/size, close-on-completion, poolable, batch, enquoteIdentifier, warnings. |
| FBPreparedStatementTest | Main PreparedStatement suite: null/long/octets/UTF-8 binding, batches, blobs, execution plans, cancel, exception-ends-transaction. |
| FBCallableStatementTest | CallableStatement: executable vs selectable procedures, in/out/in-out params, named params, batches, metadata. |
| FBCallableStatementSchemaTest | Procedure resolution across schemas/search paths, package-qualified calls. |
| FBTxPreparedStatementTest | COMMIT/ROLLBACK/SET TRANSACTION prepared statements: unsupported methods, execution paths. |
| FBStatementAllowTxStmtsTest | Allowing/disallowing transaction-management statements in execute/batch methods. |
| FBConnectionAllowTxStmtsTest | Same for prepareStatement/prepareCall + happy-path commit execution. |
| LargeUpdateCountSupportTest | JDBC 4.2 large update counts incl. batch and generated keys. |
| BatchUpdatesTest | Batch for Statement, PreparedStatement (65-blob batch), CallableStatement procedure batches. |
| DDLTest | DDL (foreign-key constraints) under autocommit and explicit transactions. |
| BoundaryTest | Regression: specific statement sequence no longer hangs. |
| ReservedWordsTest | Creates a column per reserved word to verify Firebird reserved words. |

### 2.8 `org.firebirdsql.jdbc` — generated keys

| Class | What it tests |
|---|---|
| FBStatementGeneratedKeysTest | Statement.getGeneratedKeys: RETURN_GENERATED_KEYS, column indexes/names, RETURNING variants, dialect 1, multi-row, auto-commit blob. |
| FBPreparedStatementGeneratedKeysTest | PreparedStatement generated keys: names/indexes, RETURNING, nonexistent columns/tables, batch. |
| GeneratedKeysEnabledTest | `generatedKeysEnabled` property (default/disabled/ignored/insert/update) behavior. |
| GeneratedKeysQueryTest | Unit (Mockito): RETURNING-query rewriting per dialect and column selection. |
| GeneratedKeysSupportFactoryTest | Unit: implementation selection per property value and server version. |

### 2.9 `org.firebirdsql.jdbc` — ResultSet behaviour

| Class | What it tests |
|---|---|
| FBResultSetTest | Scrolling/insensitive navigation, close on commit/rollback, fetch size/direction, findColumn, positioned update, updatable cursor, DB_KEY. |
| ResultSetBehaviorTest | Unit: type/concurrency/holdability/read-only defaults, conversions, upgrade/downgrade warnings. |
| ResultSetGetObjectTest | Parameterized getObject types for every Firebird column type incl. text blobs. |
| FBServerScrollFetcherTest | Server-side scrollable cursor (FB4+): absolute/relative/next/previous/first/last/afterLast/beforeFirst, empty sets, maxRows. |
| FBUpdatableFetcherTest | Updatable fetcher navigation with inserted rows. |
| FetchConfigTest | Unit: fetch size/max rows/direction handling and validation. |

### 2.10 `org.firebirdsql.jdbc` — BLOB / CLOB / RowId

| Class | What it tests |
|---|---|
| FBBlobTest | get/set bytes and streams, close on commit/rollback/connection close, use after commit, caching, validation. |
| FBBlobAccessTest | Database-backed blob access via views and field types. |
| FBBlobAutocommitTest | Blob behavior under auto-commit: empty/null blobs, setBinaryStream, setBlob. |
| FBBlobInputStreamTest | read/readFully/available semantics, offset/length validation, closed-stream errors. |
| FBBlobOutputStreamTest | Write buffering at buffer-size boundaries, closed-stream/argument errors. |
| FBBlobParamsTest | Blob parameters in prepared statements: binding, equality, combinations. |
| FBBlobStreamTest | Legacy blob streams: length, seek, writing bytes, long varchar. |
| FBCachedBlobTest | In-memory cached blob: length, getBytes, position, streams, mutation rejection. |
| FBClobTest | CLOB read/write: getSubString, character/ASCII streams, NClob, UTF8/win1252, null/cached clobs, use after free. |
| FBRowIdTest | Unit: FBRowId construction, null-byte rejection, hex toString. |
| RowIdSupportTest | DB_KEY/RowId: fetch rows by RowId, pseudo-column metadata, RowIdLifetime. |

### 2.11 `org.firebirdsql.jdbc` — transactions, auto-commit, savepoints

| Class | What it tests |
|---|---|
| AutoCommitBehaviourTest | Statement/result-set interaction during auto-commit (transaction ending on execution, result set closing). |
| FBSavepointTest | Named/unnamed savepoints, name/id getters, quoting, dialect 1, release, auto-commit rejection. |
| UseFirebirdAutocommitTest | Firebird-native autocommit mode: property parsing, commit-during-execution semantics. |
| TableReservationTest | Table reservation (`WITH LOCK`) shared/protected read/write conflicts between two concurrent transactions. |
| FBTpbMapperTest | Isolation-level → TPB mapping, defaults, lock timeout, invalid mappings. |
| FBTpbMapperNameMappingTest | TRANSACTION_* integers ↔ isolation names. |

### 2.12 `org.firebirdsql.jdbc` — datatype & timezone support

| Class | What it tests |
|---|---|
| BooleanSupportTest | BOOLEAN (FB3+): inserts, select conditions, parameter/result-set metadata, type info. |
| DecfloatSupportTest | DECFLOAT(16/34) (FB4+): literals, parameterized insert/select, metadata, type info. |
| DecimalPrecision38SupportTest | DECIMAL/NUMERIC(38) (FB4): min/max round trips, precision, metadata. |
| Int128SupportTest | INT128 (FB4): simple and min/max round trips, metadata. |
| TimeWithTimeZoneSupportTest | TIME WITH TIME ZONE (FB4): values, ZonedDateTime binding, conditions, metadata. |
| TimestampWithTimeZoneSupportTest | TIMESTAMP WITH TIME ZONE (FB4): values, binding, conditions, metadata. |
| TimeZoneBindTest | `timezonedbind` property (NATIVE/invalid) effect on CURRENT_TIMESTAMP and session zone reset. |
| TimeZoneBindLegacyTest | Legacy bind mode round-tripping. |
| SessionTimeZoneTest | Session time zone: JVM default, server default, explicit zone; invalid zone errors. |
| JDBC42JavaTimeConversionsTest | getObject → java.time types from date/string/time/timestamp columns. |
| Dialect1SpecificsTest | Dialect 1: double-based NUMERIC get/set. |

### 2.13 `org.firebirdsql.jdbc` — encoding

| Class | What it tests |
|---|---|
| FBEncodingsTest | Round trips across charsets (UTF8, CYRL, German, Hungarian, Ukrainian, octets), padding, execute block. |
| FBLongVarCharEncodingsTest | Same for text BLOB (long varchar). |
| FBPreparedStatementUTF8Test | Binding a single 4-byte (supplementary) UTF-8 character. |

### 2.14 `org.firebirdsql.jdbc` — statement/result-set metadata (non-DatabaseMetaData)

| Class | What it tests |
|---|---|
| FBParameterMetaDataTest | ParameterMetaData for callable statement parameters. |
| FBPreparedStatementMetaDataTest | Every parameter metadata attribute per datatype. |
| FBResultSetMetaDataTest | Column names/labels, precision (extended metadata on/off), auto-increment/identity, CTE aliases, octets columns. |
| FBResultSetMetaDataParametrizedTest | All 20 ResultSetMetaData attributes for every column of a representative result set. |
| FirebirdVersionMetaDataTest | Unit: version→metadata enum lookup. |

### 2.15 `org.firebirdsql.jdbc` — misc

| Class | What it tests |
|---|---|
| QuoteStrategyTest | Unit: quote strategy per dialect. |

### 2.16 `org.firebirdsql.jdbc` — DatabaseMetaData (26 classes)

Shared pattern: all use `UsesDatabaseExtension` with a DDL fixture (tables/views/procedures/grants/domains) and a local enum describing expected JDBC result-set columns (`MetadataResultSetDefinition` validates ordinals/types/rows); assumptions skip servers lacking features; all need a live server. `FBDatabaseMetaDataAbstractKeysTest` is the abstract fixture base (TABLE_1–TABLE_8 with PK/unique/FK + every referential action) shared by ImportedKeys/ExportedKeys/CrossReference.

| Class | What it tests |
|---|---|
| FBDatabaseMetaDataTest | Grab-bag: table types, getTables/getColumns wildcard escaping, keys, getTypeInfo, driver/JDBC versions, supports-flags, procedure/trigger/view source. |
| FBDatabaseMetaDataAbstractKeysTest | *Abstract* keys fixture (not a test). |
| FBDatabaseMetaDataBestRowIdentifierTest | getBestRowIdentifier for tables with/without PK. |
| FBDatabaseMetaDataCatalogsTest | getCatalogs: empty by default; packages-as-catalogs with `useCatalogAsPackage`. |
| FBDatabaseMetaDataClientInfoPropertiesTest | getClientInfoProperties columns and values. |
| FBDatabaseMetaDataColumnPrivilegesTest | getColumnPrivileges: grants, grant option, column-specific privileges. |
| FBDatabaseMetaDataColumnsTest | getColumns: per-type attributes (precision, scale, nullability, defaults, generated columns, charsets, domains, boolean, decfloat, INT128). |
| FBDatabaseMetaDataCrossReferenceTest | getCrossReference: parent↔FK pairs incl. referential actions. |
| FBDatabaseMetaDataDialect1Test | Metadata on dialect 1 DB: NUMERIC(15,2) reporting, identifier quote string per dialect. |
| FBDatabaseMetaDataExportedKeysTest | getExportedKeys per fixture table, cross-schema. |
| FBDatabaseMetaDataFindTableSchemaTest | findTableSchema with search path, quoted/system names. |
| FBDatabaseMetaDataFunctionColumnsTest | getFunctionColumns: PSQL + UDF params/returns, patterns, packages. |
| FBDatabaseMetaDataFunctionsTest | getFunctions: PSQL/UDF discovery, case sensitivity, packages excluded. |
| FBDatabaseMetaDataImportedKeysTest | getImportedKeys per fixture table, cross-schema. |
| FBDatabaseMetaDataIndexInfoTest | getIndexInfo: PK/unique/foreign/computed/asc/desc indexes, ODS-specific partial indexes. |
| FBDatabaseMetaDataNullsTest | nullsAreSortedAtStart/End/High/Low verified against actual ordering. |
| FBDatabaseMetaDataPrimaryKeysTest | getPrimaryKeys: named/unnamed, single/multi-column, schema filtering. |
| FBDatabaseMetaDataProcedureColumnsTest | getProcedureColumns: params/returns of executable & selectable procedures, packages. |
| FBDatabaseMetaDataProceduresTest | getProcedures: selectable vs executable, quoted names, packages. |
| FBDatabaseMetaDataPseudoColumnsTest | getPseudoColumns: RDB$DB_KEY/RECORD_VERSION across table kinds, patterns. |
| FBDatabaseMetaDataSchemasTest | getSchemas: PUBLIC/SYSTEM, user schemas, catalog handling. |
| FBDatabaseMetaDataTablePrivilegesTest | getTablePrivileges: grants to users/PUBLIC, grant option. |
| FBDatabaseMetaDataTablesTest | getTables: normal/quoted/system/GTT, TABLE_TYPE filter, wildcard escaping, sort order. |
| FBDatabaseMetaDataUDTsTest | getUDTs: empty result + metadata columns only. |
| FBDatabaseMetaDataVersionColumnsTest | getVersionColumns: RECORD_VERSION pseudo columns, edge cases. |

### 2.17 `org.firebirdsql.jdbc.field` — datatype conversion layer

All `FB*FieldTest` (except NullField/JdbcTypeConverter/TrimmableField) extend `BaseJUnit5TestFBField`: **pure Mockito unit tests** — a mocked `FieldDataProvider` supplies raw wire bytes; the base runs ~85 generic tests asserting unsupported getters/setters throw `TypeConversionException`; each subclass adds supported conversions with range/overflow/null/truncation checks.

| Class | What it tests |
|---|---|
| FBBigDecimalFieldTest | NUMERIC/DECIMAL (incl. INT128): BigDecimal/long/int/BigInteger/Decimal128/string/boolean conversions, ranges. |
| FBBinaryFieldTest | CHAR/VARCHAR OCTETS: bytes, streams, string, too-long handling. |
| FBBooleanFieldTest | BOOLEAN encode/decode, conversions from/to numerics/strings. |
| FBDateFieldTest | DATE: Date/LocalDate, calendar variants, string, Timestamp/Time interactions. |
| FBDecfloatFieldTest | DECFLOAT(16/34): Decimal64/128/BigDecimal, overflow/underflow handling. |
| FBDoubleFieldTest | DOUBLE PRECISION / D_FLOAT conversions with encoded-byte verification. |
| FBFloatFieldTest | FLOAT conversions, precision checks. |
| FBIntegerFieldTest | INTEGER: int get/set, byte/short ranges, overflow exceptions. |
| FBLongFieldTest | BIGINT: long get/set, ranges, numeric-string conversion. |
| FBNullFieldTest | SQL_NULL parameter type: setters accept null and values. |
| FBRowIdFieldTest | DB_KEY: RowId/bytes/string/stream conversions. |
| FBShortFieldTest | SMALLINT + scaled NUMERIC: ranges, encoded-byte verification. |
| FBStringFieldTest | CHAR/VARCHAR: strings/bytes/streams/readers, boolean-from-string, truncation. |
| FBTimeFieldTest | TIME: Time/LocalTime with calendar variants. |
| FBTimeTzFieldTest | TIME WITH TIME ZONE: OffsetTime/OffsetDateTime/ZonedDateTime, legacy interop. |
| FBTimestampFieldTest | TIMESTAMP: Timestamp/LocalDateTime, calendar variants. |
| FBTimestampTzFieldTest | TIMESTAMP WITH TIME ZONE: OffsetDateTime/OffsetTime/ZonedDateTime, legacy interop. |
| JdbcTypeConverterTest | Firebird type/subtype/scale ↔ JDBC type mapping (parameterized). |
| TrimmableFieldTest | Trailing-whitespace trimming for CHAR fields. |

### 2.18 `org.firebirdsql.jdbc.escape` — JDBC escape syntax

Subsystem: `{fn ...}`, `{d}/{t}/{ts}`, `{oj ...}`, `{escape}`, `{limit ...}`, `{[?=]call ...}` escapes rewritten to native Firebird SQL by `FBEscapedParser` (+ per-function `SQLFunction` implementations, `FBEscapedCallParser`).

Unit tests (no server): CharacterLengthFunctionTest, ConstantSQLFunctionTest, ConvertFunctionTest, FBEscapedCallParserTest, FBEscapedFunctionHelperTest, FBEscapedParserTest, LengthFunctionTest, LocateFunctionTest, PatternSQLFunctionTest, PositionFunctionTest, TimestampAddFunctionTest, TimestampDiffFunctionTest.
Live-server tests: LikeEscapeTest, LimitEscapeTest, OuterJoinEscapesTest, ScalarNumericFunctionsTest, ScalarStringFunctionsTest, ScalarSystemFunctionsTest, ScalarTimeDateFunctionsTest, TimeDateLiteralEscapesTest.

| Class | What it tests |
|---|---|
| FBEscapedParserTest | All escape kinds → native SQL, nesting, comments/literals/Q-literals, error cases, disable-escape. |
| FBEscapedCallParserTest | `{call}`/`{?=call}` → procedure call, out-param mapping, name resolution per version. |
| FBEscapedFunctionHelperTest | Function name/argument parsing incl. quoted identifiers and commas in literals. |
| ConvertFunctionTest | `{fn CONVERT}` rendering per JDBC type matrix + extensions. |
| TimestampAddFunctionTest / TimestampDiffFunctionTest | `{fn TIMESTAMPADD/DIFF}` → DATEADD/DATEDIFF per interval. |
| CharacterLengthFunctionTest / LengthFunctionTest | CHAR_LENGTH/OCTET_LENGTH rendering with CHARACTERS/OCTETS params. |
| LocateFunctionTest / PositionFunctionTest | LOCATE/POSITION rendering and arity errors. |
| ConstantSQLFunctionTest / PatternSQLFunctionTest | Fixed-token and `{n}`-placeholder function rendering. |
| LikeEscapeTest | `{escape}` in LIKE queries returns expected rows. |
| LimitEscapeTest | `{limit n [offset m]}` literal and parameterized row ranges. |
| OuterJoinEscapesTest | `{oj ...}` FULL/LEFT/RIGHT OUTER JOIN queries. |
| Scalar*FunctionsTest (4 classes) | All `{fn}` numeric/string/system/time-date functions executed against the server. |
| TimeDateLiteralEscapesTest | `{d}/{t}/{ts}` literals produce correct values. |

### 2.19 `org.firebirdsql.jdbc.metadata` — metadata query helpers (pure unit)

| Class | What it tests |
|---|---|
| ClauseTest | Building `=`/`LIKE`/`STARTING WITH` conditions from metadata patterns. |
| MetadataPatternMatcherTest | Pattern → regex translation and matching. |
| MetadataPatternTest | Pattern classification (NONE/SQL_EQUALS/SQL_LIKE/SQL_STARTING_WITH), wildcard escaping. |
| NameHelperTest | Package-qualified specific-name construction. |

### 2.20 `org.firebirdsql.management` — Services API

All except FBManagerTest/FBTableStatisticsManagerTest exercise the Services API; backup tests additionally require locally mapped paths; version-gated where noted.

| Class | What it tests |
|---|---|
| FBBackupManagerTest | gbak backup/restore: metadata-only, read-only restore, replace, multiple files, page sizes, parallel workers, custom security DB, include/skip data. |
| FBMaintenanceManagerTest | Access mode, dialect, shutdown modes, cache buffers, forced writes, page fill, validation/sweep, shadow activation, limbo commit/rollback, ODS upgrade, fix ICU. |
| FBManagerTest | Database lifecycle helper: create/drop/exists, page size, dialect 1/3, default charset, force write. |
| FBNBackupManagerTest | nbackup: GUID backup, in-place restore, fixup, restore with sequence, clean-history. |
| FBServiceManagerTest | Server version retrieval, property descriptors, >255-char passwords. |
| FBStatisticsManagerTest | Reports: header page, DB/system/table statistics, transaction info. |
| FBStreamingBackupManagerTest | Backup to streams (no files): round trip, buffer/page restrictions. |
| FBTableStatisticsManagerTest | Attachment-level per-table access/insert/update/delete statistics. |
| FBUserManagerTest | Add/retrieve/update/delete users via Services API. |

### 2.21 `org.firebirdsql.ds` + `org.firebirdsql.jaybird.xca` — DataSources, pooling, XA

| Class | What it tests |
|---|---|
| DataSourceBeanIntrospectionTest | JavaBean introspection exposes connection-property setters. |
| DataSourceFactoryTest | Building pool/XA/simple data sources from references. |
| FBConnectionPoolDataSourceTest | Pooled connection obtain/reuse, properties, wire compression. |
| FBPooledConnectionMockTest | Logical/physical lifecycle, close/error event notification. |
| FBSimpleDataSourceTest | Defaults, config-change restrictions, empty-role connect. |
| FBXADataSourceTest | Distributed transactions, savepoints, autoCommit, force-close-on-fatal. |
| PooledConnectionHandlerMockTest | Proxy close semantics, event notification, rollback on close. |
| PooledConnectionHandlerTest | Proxies from pool datasource: statement close/reuse, equals/hashCode. |
| StatementHandlerMockTest | Statement proxy: double close, closed-proxy exceptions. |
| DataSourceSerializationTest | Serialization via factory references. |
| FBBlobTest (xca) | Blob write/read via managed connections. |
| FBConnectionTest (xca) | Managed connection creation and statement execution. |
| FBManagedConnectionFactoryTest | MCF creation, default isolation, config-change rules. |
| FBResultSetTest (xca) | Result sets/PS across transactions, procedures. |
| FBStandAloneConnectionManagerTest | Connection allocation via stand-alone manager. |
| FBXAResourceTest | XAResource: start, rollback, one/two-phase commit, recover, close during XA. |
| FBXidTest | XID byte encoding. |

### 2.22 `org.firebirdsql.encodings`

| Class | What it tests |
|---|---|
| CharacterDecodingTest | Byte→string decoding via platform-default encoding factory. |
| ConnectionEncodingFactoryTest | Connection-scoped factory: default/none/octets definitions, dynamic charset ids. |
| DefaultEncodingSetTest | Encoding definitions with unsupported Java charsets. |
| EncodingFactoryTest | Java/Firebird charset alias mappings, custom instances, fallback lookups. |

### 2.23 `org.firebirdsql.event`

| Class | What it tests |
|---|---|
| FBEventManagerTest | Event waits with/without timeout, multiple listeners, large loads, slow callbacks, existing-connection lifecycle. |

### 2.24 `org.firebirdsql.jaybird.parser` (SQL parser — used for generated keys, table reservation, statement detection)

| Class | What it tests |
|---|---|
| BooleanLiteralTokenTest | TRUE/FALSE/UNKNOWN token text handling. |
| FirebirdReservedWordsTest | Reserved words enum ordering per server version. |
| GenericTokenTest | isValidIdentifier for valid/invalid text. |
| GrammarTest | Statement type, table/schema identification, RETURNING-clause detection. |
| ObjectReferenceExtractorTest | Extracting table/object references from statements. |
| QuotedIdentifierTokenTest | Quoted identifier parsing with quote escaping. |
| SearchPathExtractorTest | SET search_path parsing. |
| SqlParserTest | Parser lifecycle: parse/resume/halt, visitors. |
| SqlTokenizerTest | Tokenizer output: literals, quoted identifiers, numerics, comments. |
| StatementDetectorTest | Statement-type detection (incl. RETURNING detector). |
| StringLiteralTokenTest | String literal value extraction. |

### 2.25 `org.firebirdsql.jaybird.props` + `jaybird.util` + `util` + `common`

| Class | What it tests |
|---|---|
| ConnectionPropertyTest | Property name/alias rules, DPB/SPB mappings. |
| ConnectionPropertyTypeTest | STRING/INT/BOOLEAN conversions incl. invalid values. |
| ConnectionPropertyRegistryTest | Lookup by name/alias, SPI registration, duplicate failures. |
| TransactionNameMappingTest | Isolation names ↔ levels. |
| BasicVersionTest | Version compare/`isEqualOrAbove`. |
| ByteArrayHelperTest | Hex/base64 conversions. |
| CollectionUtilsTest | List grow/getLast helpers. |
| ConditionalHelpersTest | firstNonZero/firstNonNull. |
| FbDatetimeConversionTest | Modified Julian dates, Firebird time units, ISO/SQL timestamp round trips. |
| IdentifierTest / IdentifierChainTest | Identifier validation, quoting, multi-part names. |
| LegacyDatetimeConversionsTest | java.sql date/time ↔ java.time across zones. |
| ObjectReferenceTest | ObjectReference factories. |
| SQLExceptionChainBuilderTest | Exception chain building. |
| SearchPathHelperTest | Search-path parse/format round trips. |
| StringDeduplicatorTest | String interning cache with eviction. |
| StringUtilsTest | trimToNull/isNullOrEmpty. |
| NumericHelperTest | Unsigned long conversion helpers. |
| StreamHelperTest | Reverse range helper. |
| SystemPropertyHelperTest | Temporary system property restore. |

### 2.26 `src/jna-test` (native client library backend) & `chacha64-plugin`

| Class | What it tests |
|---|---|
| BigEndianDatatypeCoderTest / LittleEndianDatatypeCoderTest | Numeric encode/decode incl. decimals, per byte order. |
| FbClientResourceTest | Native resource disposal actually frees, no swallowed exceptions. |
| JnaBlobInputTest / JnaBlobOutputTest | Generic blob suites re-run against the JNA backend. |
| JnaDatabaseConnectionTest | Client-library resolution, null rejection, unconnected identify. |
| JnaDatabaseTest | Attach/detach lifecycle, wrong login/status vectors, create/drop. |
| JnaEventsTest | Event handle creation, queueing + notification, cancel. |
| JnaServiceConnectionTest / JnaServiceTest | Service attach lifecycle, wrong login/status vectors, start action. |
| JnaStatementTest | Generic statement suite against JNA incl. inline-blob thresholds. |
| JnaStatementTimeoutTest | Statement timeout suite (skips without client support). |
| JnaTransactionTest | Generic transaction suite against JNA. |
| ChaCha64EncryptionPluginSpiTest | ChaCha64 plugin identifier bytes and protocol-version support. |

---

## 3. Current test coverage in firebirdsql (Go)

**205 test functions** in 35 `_test.go` files. Infra: `GetTestDSN(prefix)` creates `sysdba:masterkey@localhost:3050/<tmpdir>/<random>.fdb` (env `ISC_USER`/`ISC_PASSWORD`), `firebirdsql_createdb` driver creates throwaway DBs; `EMPLOYEE.FDB` lives at repo root for live protocol tests on **localhost:3055** (skip when unreachable). CI: GitHub Actions against FB 2.5 and FB 3.

Legend: **[U]** pure unit · **[L]** live server on :3050 · **[E]** live EMPLOYEE.FDB on :3055.

| File | Coverage |
|---|---|
| driver_test.go | [L] ~45 tests: basic CRUD, RETURNING, blobs, errors, roles, timestamps, boolean, decfloat, time zones, INT128 (incl. negative/scaled), legacy auth, wire-crypt policy (`required`), auth-plugin hardening, ctx timeouts during exec/scan, conn reuse after timeout, blob charset decoding, wall-clock date/time, untyped nulls, ~20 GitHub issue regressions |
| transaction_test.go | [U] TPB bytes for isolation levels; [L] tx behavior, ping semantics, issue regressions |
| wireprotocol_parse_test.go | [U] malformed-wire guards: status vector, op response, fetch rows, blob segments, SRP accept-data |
| wireprotocol_test.go | [U] XSQLVAR/describe parsing + fuzz |
| protocol_feature_test.go | [U] proto 16/18/19 packet builders, inline-blob cache, execute trailers, DSN inline-blob options |
| protocol_live_test.go | [E] negotiated version smoke, inline blob reads, scroll fetches (gated on proto ≥18/19) |
| wire_crypt_test.go | [U] policy parse, cipher negotiation guards, WireCipher accessor |
| auth_plugin_test.go | [U] plugin allow-list, fail-fast validation, uid packet, DSN defaults |
| srp_test.go | [U] SRP client/server session agreement |
| compression_test.go | [U] zlib round trip, pflag detection, DSN param |
| error_test.go | [U] status-vector parse (params/warnings/multi-chain), SQLCODE fallback |
| xpb_test.go | [U] 22 reader/writer tests for parameter blocks |
| xsqlvar_test.go | [U] BLR calc, scaled numerics, INT128 values |
| decfloat_test.go / decfloat_vectors_test.go | [U] DECFLOAT formatting, specials, IEEE-754 vectors |
| timezonemap_test.go / timezone_resolve_test.go | [U] tz id round trip, IANA resolution |
| firebird_version_test.go | [U] version-string parsing |
| utils_test.go | [U] DSN parsing, helpers |
| remoteevent_test.go / subscriber_test.go | [U] EPB parsing, aux-connection packets |
| batch_unit_test.go | [U] batch packet layout, row encoding, completion parse, options |
| event_test.go | [L] events callback + subscribe |
| service_manager_test.go | [L/U] service info queries, options |
| backup_manager_test.go | [L/U] backup+restore round trip, options |
| nbackup_manager_test.go | [L/U] nbak/fixup/incremental, options |
| maintenance_manager_test.go | [L] sweep, validate, mend, limbo list/commit/rollback, modes, shutdown, replica, nolinger |
| user_manager_test.go / user_manager_parse_test.go | [L/U] user add/modify/delete/get; output parsing |
| dirtyconn_test.go / stmt_leak_test.go / opcancel_race_test.go | [L] dead-conn cancellation, statement-handle leak, op_cancel race |
| driver_go18_test.go | [L] database/sql 1.8+ features |
| null_generic_test.go | [L] sql.Null* scan/insert |
| wireparse_fuzz_test.go | [U] 6 fuzz targets |
| decode_speed_test.go | [U] benchmark |

**Well covered already**: wire-format hardening (fuzz + malformed packets), auth/wire-crypt negotiation, services managers (broad!), protocol 16/18/19 packet builders, time zones, DECFLOAT/INT128 unit math, events basics, DSN parsing.

**Thin / missing vs Jaybird** (details in §4/§5): datatype round-trip matrices (esp. boundary values), per-column-type result metadata checks, transaction isolation semantics across two connections, savepoints, blob lifecycle edge cases, charset matrix beyond UTF-8/KSC-5601, statement cancel coverage breadth, backup/maintenance option coverage, protocol-version feature gating tests, server-version feature gating helpers.

---

## 4. Plan: implement Jaybird-equivalent tests in Go

### 4.0 Principles

1. **Adapt, don't translate literally.** JDBC API surface (Savepoint/DatabaseMetaData/Clob/UpdatableResultSet) has no `database/sql` equivalent — port the *behavior* behind the API where it applies to the wire protocol, and skip JDBC-only concepts (see §5).
2. **Reuse Go idioms**: table-driven tests + testify (`require`/`assert`), `t.Skip` for gating, subtests named after the Jaybird test they mirror (traceability).
3. **Test infra first (Phase 1)** — a small shared harness replicating Jaybird's extensions, so feature tests stay one-liners to gate.
4. **Version/protocol gating mirrors Jaybird**: FB2.5 (protocol 11) / FB3 (13) / FB4 (16) / FB5 (18–19). CI today runs FB2.5/FB3 only — FB4/FB5-gated tests must skip cleanly there.
5. Keep the existing convention: tests live in package `firebirdsql` (same-package `_test.go` files), temp DBs via `GetTestDSN`/`CreateTestDatabase`.

### Phase 1 — Test harness parity (`testutil_test.go`, new) — **DONE** ✅

Implemented in `testutil_test.go` (harness) + `testutil_selftest_test.go` (harness self-tests): `testServerAddr()` (env `FIREBIRD_TEST_SERVER_ADDR`, default `localhost:3050`), cached `testServerVersion()` (env `FIREBIRD_TEST_SERVER_VERSION` pin, else Services API), `requireServerVersion` + datatype gates, `negotiatedProtocol`/`requireProtocol` + `requireBatchSupport`/`requireScrollableCursors`/`requireInlineBlobs`, `openTestDatabase`, `createTestDatabaseWithDDL`, `mustExec`, `createTestUserFixture`, and `requireSQLState`/`requireSQLCode`/`requireGDSError` matchers. `GetTestDSN` now routes through `testServerAddr()`.

| Jaybird mechanism | Go deliverable |
|---|---|
| `UsesDatabaseExtension` + `DdlHelper` | `createTestDatabaseWithDDL(t, ddl)` — create temp DB, exec DDL, auto-drop; `mustExec(t, ctx, conn, sql, args...)` helper |
| `RequireFeatureExtension` / `FirebirdSupportInfo` | `requireServerVersion(t, conn, ">=4.0")` using existing `FirebirdVersion.EqualOrGreater`; named checks: `requireBoolean`, `requireDecfloat`, `requireTimeZones` (FB4), `requireScrollable`/`requireInlineBlob` (protocol ≥18/19 via `ProtocolVersion()`) |
| `RequireProtocolExtension` | `requireProtocol(t, conn, minVersion)` |
| `DatabaseUserExtension` | `createTestUser(t, …)`/`dropTestUser` via existing `UserManager` (for privilege tests) |
| `SQLExceptionMatchers` | testify helpers: `requireSQLState(t, err, "42xxx")`, `requireSQLCode(t, err, -xxx)` on `FbError` |
| `FBTestProperties` | Keep `GetTestDSN`; add env for server version pinning and optional second-connection DSN (used by concurrency tests) |

### Phase 2 — No-server unit parity (extend existing files) — **DONE** ✅

Added (Jaybird analog in parentheses): `TestGdsToSQLStateTable`/`TestGdsToSQLCodeTable` (MessageTemplateTest), `TestFirebirdVersionEqualOrGreaterMatrix`/`TestFirebirdVersionEqualOrGreaterPatch` (GDSServerVersionTest/BasicVersionTest), `TestDSNIPv6Formats` (DbAttachInfoTest), `TestDSNOptionsDefaults`/`TestDSNOptionsOverridesAndAliases`/`TestDSNOptionsFailFast` (FbConnectionPropertiesTest), `TestTimezoneLegacyAliases`/`TestTimezoneMapInvalidEntries` (TimeZoneMappingTest), `TestAdvertisedProtocolRange` (ProtocolCollectionTest, via new `advertisedProtocols()` extraction).

Driver fixes required for parity: `dsn.go` — bracketed IPv6 without port now gets the default 3050 (`net.SplitHostPort`); `wireprotocol.go` — protocol descriptor table extracted from `opConnect` into `advertisedProtocols(wireCompress bool)` (no behavior change). All live tests now route through `testServerAddr()` (was 38 hardcoded `localhost:3050`); the full suite runs against any server address. VaxEncoding parity deemed covered by existing `xpb_test.go`; `OdsVersionTest` parity deferred to Phase 3 (live info).

| Jaybird suite | New/extended Go tests |
|---|---|
| MessageTemplateTest | `error_test.go`: table-driven spot checks of `gdsToSQLState`/`gdsToSQLCode` for representative codes (kind/error-class/subclass mapping) |
| GDSServerVersionTest / BasicVersionTest | `firebird_version_test.go`: `EqualOrGreater` matrix, garbage banners (partially exists) |
| DbAttachInfoTest / DatabaseUrlFormatsTest / JDBCUrlPrefixTest | `utils_test.go`: `TestDSNFormats` — IPv6 hosts, default port, local file paths, URL-encoded password, param order independence |
| FbConnectionPropertiesTest analog | `utils_test.go`: `TestDSNOptions` — every documented DSN param: defaults, invalid values fail fast (wire_crypt/auth validation exists — extend to all params) |
| OdsVersionTest | add `TestOdsVersionParse` if ODS info gets a helper; otherwise via live `TestOdsVersionFromInfo` (Phase 3) |
| VaxEncodingTest | covered indirectly by `xpb_test.go`; add `TestVaxInteger` table tests if the helper is exported, else skip (internal) |
| TimeZoneMappingTest / TimeZoneCodec suites | extend `timezonemap_test.go`: negative/overflow tz ids → UTC fallback; offset round trips incl. half-hour zones |
| FbExceptionBuilderTest | `error_test.go`: multi-error chains with params, warning extraction (partially exists) |
| SqlCountHolderTest | `TestSqlCounts` (live, Phase 3) — verified via RowsAffected paths |
| ProtocolCollectionTest | `TestProtocolAdvertisedRange` — assert connect packet offers protocols 10–19 (packet builder exists) |

### Phase 3 — Datatype round-trip matrices (live, `datatype_test.go`) — **DONE** ✅

Added: `TestDatatypeMatrix` (20 type subtests × boundary values: NULL/min/max/empty/unicode/multi-segment blobs, with instant-vs-wall-clock semantics per type — covers Boolean/Decfloat/Int128/Decimal38/TimeTz/TimestampTz/encodings support tests), `TestDatatypeColumnMetadata` (per-column DatabaseTypeName/ScanType/Nullable/PrecisionScale/Length — ResultSetMetaData parametrized parity), `TestSessionTimeZone` (DSN `?timezone=` semantics), `TestCharsetRoundTrip` (WIN1251/WIN1252/UTF8), `TestSqlCounts` (RowsAffected parity).

Driver bugs found & fixed (exposed by the new tests, verified against FB 5.0.5):
1. **Panic** `time: missing Location in call to Time.In` — `_parseTimezone` returned a nil `*time.Location` when a zone name failed `LoadLocation`; any TIME/TIMESTAMP WITH TIME ZONE read with such an id crashed the process.
2. **Timezone wire codec corrected to Jaybird semantics**: the wire wall clock is UTC and the zone id sits in the last 2 bytes (old code treated the displacement slot as a base zone and re-anchored with `time.Now()`). `parseTimeTz` now resolves offsets via the Firebird base date 2020-01-01 (Jaybird `TIME_TZ_BASE_DATE`); encode writes UTC wall + `[0,0,zone-id]`.

Mirrors: FBEncodingsTest, BooleanSupportTest, DecfloatSupportTest, DecimalPrecision38SupportTest, Int128SupportTest, *WithTimeZoneSupportTest, SessionTimeZoneTest, JDBC42JavaTimeConversionsTest, BoundaryTest, Dialect1SpecificsTest, field/* tests (integration side).

1. `TestDatatypeMatrix` — table-driven: for each type (`BOOLEAN, SMALLINT, INTEGER, BIGINT, INT128, FLOAT, DOUBLE, NUMERIC(p,s) incl. 18/38 scale, DECFLOAT(16/34), CHAR, VARCHAR, DATE, TIME, TIMESTAMP, TIME WITH TIME ZONE, TIMESTAMP WITH TIME ZONE, BLOB SUB_TYPE TEXT/BINARY`) do insert → select → scan with boundary values (NULL, min, max, 0, empty string, unicode 4-byte char) and assert `ColumnTypeDatabaseTypeName/Length/PrecisionScale/Nullable/ScanType` per column (analog of FBResultSetMetaDataParametrizedTest).
2. `TestSessionTimeZone` — DSN `?timezone=` applied: with-tz columns keep instant, without-tz columns shift wall clock; invalid zone → connect error (Jaybird SessionTimeZoneTest/TimeZoneBindTest behavior minus legacy bind).
3. `TestCharsetRoundTrip` — table of charsets (UTF8, WIN1251, WIN1252, KSC_5601, OCTETS) with padding behavior for CHAR and text-blob variant (FBEncodingsTest/FBLongVarCharEncodingsTest/FBPreparedStatementUTF8Test; KSC_5601 partially exists).
4. `TestNumericScales` — scaled INT128/NUMERIC extremes, trailing zeros, precision loss errors (mirrors xsqlvar unit tests but end-to-end).
5. `TestSqlCounts` — insert/update/delete row counts via `RowsAffected` (incl. multi-statement execute block), mirrors SqlCountHolderTest + LargeUpdateCountSupportTest.

### Phase 4 — Statements, procedures, transactions (live, `statement_test.go`) — **DONE** ✅

Added: `TestStatementLifecycle` (transactional DDL commit/rollback, prepared-statement reuse across transactions, closed-statement errors — FBStatementTest/DDLTest), `TestStoredProcedureCalls` (executable procedures with out-params, selectable procedures, NULL in/out, exception propagation — FBCallableStatementTest), `TestReturningEdgeCases` (multi-row UPDATE/DELETE RETURNING, BLOB RETURNING, unknown-column error — GeneratedKeys tests), `TestBatchParity` (200-row batch, mid-batch unique violation with ContinueOnError/DetailedErrors — BatchUpdatesTest/V16StatementTest), `TestTransactionIsolation` (two-connection visibility, REPEATABLE READ snapshot, read-only tx refuses writes — AbstractTransactionTest/FBTpbMapperTest), `TestSavepoints` (SAVEPOINT/ROLLBACK TO/RELEASE via SQL — FBSavepointTest behavior), `TestAutoCommitSemantics` (autocommit visibility — AutoCommitBehaviourTest; MON$-open-transaction assertions dropped because this driver uses commit-retaining). Harness: `mustExec` generalized to `*sql.DB`/`*sql.Tx`/`*sql.Conn`.

| Jaybird suite | Go tests |
|---|---|
| FBStatementTest, DDLTest | `TestStatementLifecycle`: DDL under tx vs autocommit, exec vs query misuse errors, close semantics, double close, exec after close |
| FBCallableStatementTest | `TestStoredProcedureCalls`: executable procedure with in/out params (`EXECUTE PROCEDURE`), selectable procedure rows (`SELECT * FROM proc`), null in/out, error propagation |
| FB*GeneratedKeys*Test | extend `TestReturning`: multi-row RETURNING, RETURNING with BLOB column, nonexistent column error, RETURNING in batch |
| BatchUpdatesTest | extend batch tests: many rows, `MultiError` with per-row errors mid-batch, record counts, cancel batch then execute, blobs rejected (Jaybird V16StatementTest parity) |
| AbstractTransactionTest | `TestTransactionStates`: commit/rollback/commit-retain paths; visibility across two connections; rollback releases locks |
| FBTpbMapperTest | extend TPB unit tests: every documented isolation + `ReadOnly` combos, lock timeout param |
| AutoCommitBehaviourTest | `TestAutoCommitSemantics`: uncommitted change invisible to second connection until commit; autocommit statement ends tx (monitoring table `RDB$TRANSACTIONS` count check) |
| UseFirebirdAutocommitTest | N/A-ish (Jaybird-specific mode) — skip; covered by above |
| FBSavepointTest | `TestSavepoints` — **pure SQL**: `SAVEPOINT a` / `ROLLBACK TO a` / `RELEASE SAVEPOINT` inside a tx; nested savepoints; rollback undoes partial work (server feature works through any driver) |
| TableReservationTest | **blocked** — no TPB table-reservation API in Go (see §5.9) |
| AbstractStatementTimeoutTest | **blocked** — no statement-timeout API (see §5.4) |

### Phase 5 — BLOB/CLOB lifecycle (live, `blob_test.go`) — **DONE** ✅

Added: `TestBlobSegmentBoundaries` (sizes 1 → 100 000 across the 32 K segment boundary — FBBlobStreamTest), `TestBlobEdgeCases` (NULL vs empty for both subtypes, cross-connection visibility, rolled-back tx, param reuse — FBBlobTest/FBBlobAutocommitTest), `TestClobLargeText` (multi-segment unicode text — FBClobTest), `TestInlineBlobTempDB` (inline-blob DSN options generalized from EMPLOYEE to throwaway DBs, threshold on/off + oversized blob — V19StatementTest/InlineBlobTest). Documented current contract: an empty `[]byte` parameter is stored as SQL NULL (Jaybird round-trips an empty binary blob — candidate future improvement).

### Phase 6 — Events parity (live, `event_parity_test.go`) — **DONE** ✅

Added: `TestEventChanDelivery` (counts per event name — FBEventManagerTest), `TestEventMultipleSubscribers` (both subscribers receive), `TestEventLargeLoad` (500 rapid posts all accounted for via count coalescing), `TestEventUnsubscribeStopsDelivery` (delivery stops after Unsubscribe; uses a separate POST_EVENT connection so the FbEvent lifecycle doesn't mask the assertion). Lifecycle notes captured: `POST_EVENT` is PSQL-only (needs the execute-block wrapper) and the last `Unsubscribe` closes the FbEvent (may surface `ErrFbEventClosed`).

Mirrors: FBBlob*Test family, FBClobTest, JaybirdBlobBackupProblemTest (via services), InlineBlob live tests.

1. `TestBlobRoundTrip` — sizes: empty, 1 byte, <32K (single segment), 100K/1MB (multi-segment), random binary (all byte values); text subtype with unicode.
2. `TestBlobEdgeCases` — NULL blob, empty-string blob vs NULL, blob in rolled-back tx, blob via RETURNING, blob param reuse, blob >32K in batch (must error per current driver contract).
3. `TestBlobCharsetDecoding` — extend existing: per-charset text blobs (FBEncodingsTest long-variant).
4. `TestInlineBlobLive` — generalize `protocol_live_test.go` inline-blob tests to temp DBs: max_inline_blob_size thresholds (under/over), cache size 0 (V19StatementTest parity), needs FB5 server.

### Phase 7 — Services API option coverage (live, `services_parity_test.go`) — **DONE** ✅

Added: `TestBackupOptionsMatrix` (metadata-only backup keeps structure without rows; `WithReplace` restores over an existing DB; restoring without it fails — FBBackupManagerTest), `TestBackupRestoreBlobIntegrity` (660 KB streamed blob survives backup/restore — JaybirdBlobBackupProblemTest regression), `TestStatisticsManagerReports` (header-page report mentions the header page; table-scoped report names the table; record-versions report non-empty — FBStatisticsManagerTest), `TestMaintenanceAccessModeParity` (read-only DB refuses writes, read-write accepts — behavioral extension of the existing SetAccessMode test; documented that the DB must be unattached for mode changes), `TestServiceManagerSweepParity` (sweep completes and DB stays usable).

Mirrors FBBackupManagerTest/FBMaintenanceManagerTest/FBStatisticsManagerTest.

1. `TestBackupOptionsMatrix` — metadata-only, ignore-checksums, no-limbo, garbage-collect, transportable/convert-tables, page size, replace-on-restore, zip (FB4+), parallel workers (FB4+); restore into existing DB with replace.
2. `TestBackupDatabaseWithStreamedBlobs` — regression parity for JaybirdBlobBackupProblemTest.
3. `TestMaintenanceMatrix` — extend: access mode read-only → write attempt fails; shutdown modes (single/multi/full) + online restore; page fill; dialect change; deactivate/activate shadow (fixture permitting); kill unavailable (optional).
4. `TestStatisticsManagerReports` — header page / DB stats / per-table stats output contains expected markers (GetDbStats exists — assert content).
5. `TestServiceInfoFields` — home dir, security DB path, msg dir, lock dir non-empty (FBServiceManagerTest parity).

### Phase 8 — Protocol-version feature gating (live, `protocol_gate_test.go`) — **DONE** ✅

Added: `TestNegotiatedProtocolVersion` (negotiated protocol in 10–19; protocol 19 implies Firebird 5+ — replaces the V10–V19 class hierarchy with data-driven checks), `TestFeatureProtocolGating` (batch ≥16, scrollable cursors ≥18 — each subtest skips on older servers, `RequireProtocolExtension` parity), `TestCancelOperationParity` (context cancel aborts a long statement; pool stays usable — V12 cancelOperation + operation-monitor coverage; complements the existing `TestTimeout*`/`TestReuseConnectionAfterTimeout`). ExecImmediate already covered by `TestExecImmediateLive`.

### Phase 9 — Hardening & fuzz parity (extend `wireparse_fuzz_test.go`) — **DONE** ✅

Added `FuzzParseDSN` (DSN parser never panics on hostile input — immediately caught and fixed a real panic: `parseDSN(":/")` sliced `dbName[2:]` out of range in dsn.go). VaxEncoding parity deemed covered by `xpb_test.go`; version-banner fuzzing already existed (`FuzzParseFirebirdVersion`).

### Phase 10 — CI matrix — **DONE** ✅

Added `.github/workflows/test_fb4.yml` and `test_fb5.yml`: Docker service container (`firebirdsql/firebird:4.0.5` / `5.0.3`, `ISC_PASSWORD=masterkey`, port 3050), TCP wait loop, Go 1.22/1.23 matrix, `go test -race ./...`. FB4/FB5 jobs run the tests that FB2.5/FB3 jobs skip via the version/protocol gates from Phase 1.

---

## 5. Tests for features that do NOT exist in the Go driver

These Jaybird suites **cannot be ported today** — the Go driver lacks the feature entirely (beyond `database/sql`'s scope, or simply unimplemented). Grouped by disposition.

### 5.1 Missing driver features — implement first, then port tests

| # | Jaybird tests | Missing Go feature | Recommendation |
|---|---|---|---|
| 5.1.1 | AbstractStatementTimeoutTest, V16/18/19StatementTimeoutTest, FBConnectionTimeoutTest | **Statement timeout API** — driver writes `p_sqldata_timeout=0` always; no DSN param/ctx mapping to Firebird's statement timeout | Add `statement timeout` DSN option (or per-query option) writing `p_sqldata_timeout`; then port the 4 timeout scenarios |
| 5.1.2 | FBXAResourceTest, FBXidTest, FBXADataSourceTest | **Two-phase commit / XA** — no `op_prepare`, no XID, no limbo participation (only limbo *resolution* via MaintenanceManager) | Out of scope for `database/sql`; consider driver-specific `Tx.Prepare()` extension API before porting |
| 5.1.3 | 26 FBDatabaseMetaData*Test + jdbc/metadata tests | **Database metadata API** — no GetTables/GetColumns/GetPrimaryKeys/… equivalent | If a Go-idiomatic metadata API is added, port as `metadata_test.go` using Jaybird's fixture DDL; the JDBC result-set-column contracts do not apply |
| 5.1.4 | DatabaseEncryptionTest, DbCryptDataTest, StaticValueDbCryptCallback*Test | **Database-level encryption key callback** — wire crypt (transport) is implemented, but supplying a key for an encrypted database (`SET DATABASE` crypt plugins) has no public API (`op_crypt_key_callback` parsing exists but unexposed) | Expose a `DbCryptCallback` DSN option (static-value first), then port against pre-encrypted fixtures |
| 5.1.5 | TableReservationTest | **Table reservation (`WITH LOCK`) TPB options** — TPB builder is internal, no public knob | Add transaction option `TableReservation`/lock flags, then port the 2-connection conflict matrix |
| 5.1.6 | FBSavepointTest (API part) | **Savepoint API** — but savepoints are plain SQL, so test the behavior now (`TestSavepoints`, §Phase 4); only the typed `sql.Savepoint`-style API is missing | Port behavior via SQL immediately; no driver change strictly required |
| 5.1.7 | GeneratedKeys*Test (LastInsertId part) | **`Result.LastInsertId`** — always −1; RETURNING covers the need but `LastInsertId` is unimplemented | Document RETURNING as the idiom, or implement LastInsertId for single-row INSERT … RETURNING id |
| 5.1.8 | Dialect1SpecificsTest, FBDatabaseMetaDataDialect1Test, QuoteStrategyTest | **Dialect 1 support** — no dialect param in DSN; driver assumes dialect 3 | Add `dialect` DSN param (low priority — dialect 1 is legacy), then port |
| 5.1.9 | V11StatementTest async-fetch tests | **Async/deferred fetch** — driver fetches fixed 400-row batches synchronously | Internal optimization; port only if implemented |

### 5.2 Not applicable — JDBC-spec / Java-platform specific (no Go counterpart concept)

| Jaybird tests | Why N/A in Go |
|---|---|
| jdbc/escape/* (20 classes, 94 tests) | JDBC escape syntax (`{fn}`/`{d}`/`{oj}`/`{limit}`) is a JDBC-spec feature; `database/sql` has no escape layer and no `nativeSQL` |
| ResultSetBehaviorTest, FBResultSetTest (scroll-insensitive/updatable parts), FBUpdatableFetcherTest, FBCachedBlobTest | `database/sql` Rows are forward-only read-only by contract; client-side scrollable/updatable result sets and cached blobs are JDBC abstractions (server-side scroll **is** available and tested via `QueryScrollable`) |
| FBConnectionClientInfoPropertiesTest, ClientInfoPropertyTest, FBDatabaseMetaDataClientInfoPropertiesTest | `Connection.setClientInfo` is JDBC-specific |
| FBConnectionSchemaTest, FBCallableStatementSchemaTest, SearchPathExtractorTest, SearchPathHelperTest | `setSchema`/search-path API absent; Firebird schema support is Jaybird-6-specific |
| ds/* (9 classes), xca pooling tests (FBPooledConnection*, PooledConnectionHandler*, StatementHandlerMock*, FBStandAloneConnectionManager*) | Connection pooling is `database/sql`'s job; driver-level DataSource/pool plumbing doesn't exist |
| DataSourceBeanIntrospectionTest, DataSourceFactoryTest, DataSourceSerializationTest | JavaBean introspection/JNDI/serialization are Java-platform concepts |
| FBRowIdTest, RowIdSupportTest, FBRowIdFieldTest | `java.sql.RowId` API; RDB$DB_KEY remains selectable as raw bytes in Go — behavior can be covered in Phase 3 matrix, the API cannot |
| MaxFieldSize / fetch direction / holdability / poolable parts of FBStatementTest/FBResultSetTest/AbstractStatementTest | `database/sql` exposes none of these knobs |
| JDBC42JavaTimeConversionsTest (getObject-to-java.time matrix) | java.time types are Java-specific; equivalent time.Time scanning is covered in Phase 3 |
| LargeUpdateCountSupportTest | Go `RowsAffected` is already int64 |
| jaybird/parser/* (11 classes) | Jaybird's SQL parser serves generated-keys rewriting, table reservation and object detection — none of which the Go driver does; no parser exists to test |
| jaybird/props/* (4 classes) | Jaybird's typed connection-property registry/bean machinery; Go DSN parsing tests (Phase 2) are the loose equivalent |
| jna-test/* (13 classes), chacha64-plugin SPI test | Native client-library (JNA) backend doesn't exist in Go (pure wire driver); ChaCha64 is built-in and covered by wire-crypt tests |
| TimeZoneBindLegacyTest, UseFirebirdAutocommitTest, FBTxPreparedStatementTest, AllowTxStmts tests | Jaybird-specific compatibility modes/APIs (legacy tz bind, firebird autocommit flag, transaction-statement passthrough control) with no Go equivalent |
| ReservedWordsTest, FirebirdReservedWordsTest | En quote/reserved-word helpers are JDBC `Statement.enquoteIdentifier` features |
| FBTpbMapperNameMappingTest, ConnectionPropertyTypeTest, TransactionNameMappingTest | Java enum/name mapping layers that don't exist in Go |

### 5.3 Suggested priority order for §5.1 features

1. **Statement timeout** (small: one DPB/execute-trailer field + DSN param) → unblocks a whole Jaybird suite.
2. **Savepoint behavior tests via SQL** (no driver change needed — do now).
3. **LastInsertId via RETURNING** (small, ergonomic win).
4. **Table reservation TPB options** (moderate).
5. **DbCrypt key callback** (moderate; needs encrypted-DB fixtures).
6. **Metadata API** (large; only if the project wants it — otherwise rely on direct system-table queries in tests).
7. XA/2PC, async fetch, dialect 1 — likely never; document as out of scope.

---

## Appendix: Jaybird test counts per source set (verification data)

| Source set | Files | `@Test` |
|---|---:|---:|
| `src/test` | 332 | ~3,112 |
| `src/jna-test` | 13 | 66 |
| `chacha64-plugin` | 1 | 4 |
| **Total** | **345** | **≈3,182** |
