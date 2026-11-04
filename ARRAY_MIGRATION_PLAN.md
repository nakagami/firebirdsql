# Array Support Migration Plan for `firebirdsql` (Go)

Generated: 2026-09-12
**Status: IMPLEMENTED** (commit `093688a`). Cross-version validation of the full test suite + the array round-trip tests:

| Server | Port | Result | Notes |
|---|---|---|---|
| Firebird 5.0.5 | 3055 | ✅ green | reference server; all 14 array live tests + full suite |
| Firebird 4.0 | 3054 | ✅ green | full suite |
| Firebird 3.0 (HQbird) | 3053 | ✅ green | BOOLEAN arrays exercised; server instance had to be restarted after unrelated connection exhaustion |
| Firebird 2.5.9 HQbird | 3052 | ✅ green | no BOOLEAN — array DDL gated by server version |

Sources analyzed:

- **fbx** (reference implementation): `E:\Projects_2026\fbx` — `fbproto/op_get_slice.go`, `fbproto/op_put_slice.go`, `fbproto/op_slice_response.go`, `fbconn/array.go`, `fbconn/fbconn.go` (lines 1923–2215, 3246–3311), `fbconn/statement.go` (lines 252–272), `fbtype/array.go`
- **firebirdsql-go** (this driver): `wireprotocol.go`, `xsqlvar.go`, `statement.go`, `driver_go18.go`, `consts.go`

---

## 1. Scope

Add Firebird array column support: read (`op_get_slice`) and write (`op_put_slice`) of
array columns/parameters with typed Go slices, on top of the existing `database/sql` API.

**In scope**

- Array-typed result columns scan into typed Go values (`FlatArray[int64]`, `[]int64` via wrapper, `Array[T]` for multi-dimensional)
- Array-typed statement parameters bound from Go slices / `driver.Valuer` wrappers
- Array element types: SHORT, LONG, INT64, FLOAT, DOUBLE, TEXT, VARYING, DATE, TIME, TIMESTAMP, TIME_TZ, TIMESTAMP_TZ, BOOLEAN
- Array metadata (element type, scale, length, subtype, dimensions) resolved at prepare time from system tables
- Multi-dimensional arrays, non-1-based lower bounds, partially-filled (short) arrays

**Out of scope (later phases)**

- INT128 / DECFLOAT element types in arrays (error clearly, don't mis-decode)
- BLOB element types (invalid in Firebird anyway)
- Array support in the batch API (server limitation — fbx rejects blob/array in batches too; verify this driver's batch path rejects with a clear message)
- Schema-qualified (FB6/RDB) array metadata (`RDB$SCHEMA_NAME` columns) — wire the struct fields now, fill later with the schemas feature

## 2. Current state in firebirdsql-go

| Concern | Today | Where |
|---|---|---|
| Output BLR for array columns | Already emits `blr_quad {9,0}` — server sends the 8-byte array ID per row | `xsqlvar.go:493` (`calcBlr`, case `SQL_TYPE_ARRAY`) |
| Array column scan | **No `SQL_TYPE_ARRAY` case in `value()`** — silently scans as `nil` | `xsqlvar.go:340` |
| Array param bind | Go slice falls into `paramsToBlr`'s `default` → stringified → type error at execute | `wireprotocol.go:1902` (case list ends `[]byte`) |
| Param converter | No `CheckNamedValue`/`ColumnConverter` implemented — custom param types cannot pass through `driver.DefaultParameterConverter` | `driver_go18.go` (`flattenNamedValues` only) |
| Field names for metadata | `isc_info_sql_field`/`isc_info_sql_relation` already requested; `xSQLVAR.fieldname`/`relname` populated for select **and** bind XSQLVAs | `consts.go:456–457`, `wireprotocol.go:768` (`_parse_select_items`) |

So the wire-level groundwork (quad IDs flowing per row/column) already exists; everything
above it is missing.

## 3. Reference design (fbx)

fbx implements arrays in four layers:

1. **Wire ops** (`fbproto`): `OpGetSlice` (op 58: tx handle, int64 array id, slice length, SDL,
   empty params buffer, empty slice buffer) and `OpPutSlice` (op 59: same, plus slice length
   repeated and the slice bytes). Responses: `OpSliceResponse` (op 60: element count, byte
   count, data) for get; `OpResponse` carrying the new array ID in its blob-id field for put.
2. **Metadata** (`fbconn/fbconn.go:1923–2116, 3246–3311`): at prepare time, for every field/param
   with `SQL_TYPE_ARRAY`, two system-table queries keyed by `(RDB$RELATION_NAME, RDB$FIELD_NAME)`
   build an `Array` struct (element BLR type, scale, length, charset-aware length adjustment,
   subtype, dimensions): `RDB$RELATION_FIELDS ⋈ RDB$FIELDS` for type info and
   `RDB$FIELD_DIMENSIONS` for bounds.
3. **SDL + slice length** (`fbconn/array.go`): `generateSDL()` builds the slice descriptor
   (`isc_sdl_struct` → element BLR → `sdl_relation`/`sdl_field` → per-dimension `sdl_do1/do2`
   bounds → `sdl_element/scalar/variable` loop → `sdl_eoc`); `getSliceLength()` over-allocates
   the buffer (element size × elements, +2 bytes/element for VARCHAR).
4. **Codec** (`fbtype/array.go`, 695 lines): pgx-style `ArrayGetter`/`ArraySetter` interfaces,
   `Array[T]`/`FlatArray[T]` generics, per-element layout rules, ragged-slice guards,
   actual-dimension shrinking for short buffers.

Integration points in fbx: statement description attaches `ArrayData` to fields/params
(`fbconn.go:1618–1668`); execute converts array params via `putArray` before packing
(`statement.go:263`); fetch materializes array columns via `getArray` after row buffering
(`fbconn.go:2487`).

## 4. Target design for this driver

This driver is `database/sql`-only with a direct (no codec-map) type system, so the fbx codec
is ported as **layout rules + generic wrappers**, not as a codec framework.

### 4.1 Public API (new file `array.go`)

```go
// Dimensions of one array axis.
type ArrayDimension struct {
    LowerBound int
    Length     int
}

// Metadata resolved at prepare time (element type + layout + declared dimensions).
type ArrayMeta struct {
    TableName string   // relation name as returned by describe
    FieldName string   // field name as returned by describe
    TypeID    int      // element SQL type (SQL_TYPE_*)
    BlrTypeID int      // element BLR type (for SDL)
    Scale     int32
    Length    uint32   // element storage length
    SubType   int32    // charset id for text elements
    Dimensions []ArrayDimension
}

// driver.Value bridge for array columns/params. Exported because custom
// sql.Scanner implementations receive it as-is.
type ArrayValue struct {
    Meta *ArrayMeta
    Data []byte // flat element data, server slice layout
}

// Flat 1-D array wrapper. Implements sql.Scanner and driver.Valuer.
type FlatArray[T any] []T

// Multi-dimensional array wrapper. Implements sql.Scanner and driver.Valuer.
type Array[T any] struct {
    Elements []T
    Dims     []ArrayDimension
    Valid    bool
}
```

Semantics mirrored from fbx:

- `FlatArray[T]` / `Array[T]` supported element types: `int16, int32, int64, float32, float64, string, time.Time`
- `nil` slice / `Valid=false` ⇒ SQL NULL array; NULL *elements* are not supported by Firebird ⇒ encode error `cannot encode NULL array element at index %d`
- Ragged multi-dimensional Go slices rejected on encode
- Arrays shorter than the declared dimension (buffer shrunk by the server for trailing NULL storage) are decoded with fbx's actual-dimension logic; a `FlatArray`/`Array` target receives only the present elements

### 4.2 Data flow — write (param bind)

```
sql: db.Exec("INSERT INTO t (ids) VALUES (?)", firebirdsql.FlatArray[int64]{1,2,3})
  → FlatArray.Value() returns ArrayValue{Meta: nil, Data: <flat element bytes>}   (array.go)
  → NEW: firebirdsqlStmt.CheckNamedValue lets ArrayValue pass through unconverted,
    delegates everything else to driver.DefaultParameterConverter          (driver_go18.go)
  → stmt.exec → wp.opExecute → paramsToBlr: new case ArrayValue:
      if Meta == nil, take Meta from inputXsqlda[i].arrayMeta (prepare-time lookup)
      sdl := generateSDL(meta), len := sliceLength(meta, dims from encoded data)
      id := wp.opPutSlice(transHandle, 0, len, sdl, data)   // 8-byte ID
      blr = {9,0}; v = id                                    (wireprotocol.go:1902)
```

`transHandle` is the same handle `paramsToBlr` already receives — array IDs are created
inside the statement's transaction, exactly like `createBlob` for large strings today.

### 4.3 Data flow — read (fetch/scan)

```
sql: rows.Scan(&firebirdsql.FlatArray[int64]{})
  → wp.opFetchResponse → wp.readRow: raw 8-byte quad ID arrives (BLR already quad)
  → NEW: x.sqltype == SQL_TYPE_ARRAY case:
      id := big-endian uint64 of rawValue; if 0 → nil (NULL array)
      data := wp.opGetSlice(transHandle, id, sliceLength(meta), sdl)
      r[i] = ArrayValue{Meta: x.arrayMeta, Data: data}                (wireprotocol.go:1545)
  → database/sql convertAssign sees dest implements sql.Scanner
    → FlatArray[T].Scan(ArrayValue) → SetDimensions + decodeBinary    (array.go)
```

`transHandle` for `opGetSlice` is the transaction the rows were fetched under (fbx uses an
internal transaction because its scans outlive the query tx; this driver decodes synchronously
inside `Next()`, so the fetch transaction is correct and simpler).

### 4.4 Metadata resolution (new file `array_meta.go`)

Port of fbx `getArrayMetadata` + `getArrayDimensions` + `getArrayInfo`, adapted:

- Runs at the end of `newFirebirdsqlStmt` (`statement.go:362`) **and** inside
  `ensureInputXsqlda` (`statement.go:163`): collect `(relname, fieldname)` of all
  `SQL_TYPE_ARRAY` vars in output + input XSQLVAs; if any, run the two system-table
  SELECTs on the same `wireProtocol` (plain internal prepare/exec/free — same synchronous
  round-trip pattern as `_fetchBindXsqlda`, which already runs there); attach `*ArrayMeta`
  to the matching `xSQLVAR.arrayMeta` (new field, `xsqlvar.go:131`).
- Charset length adjustment for TEXT/VARYING elements (fbx `fbconn.go:3274–3287`): if the
  element charset's max bytes/char is smaller than the connection encoding's, rescale
  `Length` and override `SubType` to the connection charset so slice buffers line up.
- Schema field is plumbed through `ArrayMeta` (reserved for FB6/RDB) but not yet queried.
- Caching: none initially (fbx has none either); a per-conn `map[metaKey]*ArrayMeta` LRU can
  follow if profiling justifies it.

### 4.5 SDL + slice length (new file `array_sdl.go`)

Near-verbatim port of fbx `fbconn/array.go:54–167`: `generateSDL`, `getDimensionBound`,
`sliceLength`. Add the needed `Isc_sdl_*` consts to `consts.go` (values from
`fbx/fbproto/consts.go`: `sdl_version1=1`, `sdl_struct=5`, `sdl_relation=6`, `sdl_field=7`,
`sdl_do1=8`, `sdl_do2=9`, `sdl_element=10`, `sdl_eoc=255`, `sdl_scalar=1`, `sdl_variable=2`,
tiny/short/long integer marks 7/8/9 — verify each against fbx consts, do not trust from memory).

**Layout rules to preserve exactly** (the part most likely to break silently):

| Element type | On-wire element layout in slice data |
|---|---|
| SHORT | **4 bytes** big-endian (not 2), scale applied |
| LONG / INT64 | 4 / 8 bytes big-endian |
| FLOAT / DOUBLE | 4 / 8 bytes IEEE big-endian |
| BOOLEAN | 1 byte value + **3 bytes padding** |
| VARYING | **4-byte big-endian length** + data, padded to 4-byte boundary |
| TEXT | data space-padded to `Length`, padded to 4-byte boundary |
| DATE/TIME/TIMESTAMP(_TZ) | same 4/8-byte encodings as scalar columns |

Slice length (upper bound for `p_slc_length`) = elements × (Length, +4 for SHORT→4 bytes,
+4 length prefix for VARYING, rounded up per alignment) — fbx over-allocates (+2/element for
VARCHAR); over-allocation is safe, under-allocation truncates. Compute generously and verify
with max-length VARCHAR elements in live tests.

## 5. Implementation phases

Each phase is independently compilable and testable; phases 1–3 already deliver working
round-trips for flat numeric arrays.

**Phase 0 — wire plumbing** (`consts.go`, `wireprotocol.go`)
- `op_get_slice=58`, `op_put_slice=59`, `op_slice_response=60` consts; `Isc_sdl_*` consts
- `opGetSlice(transHandle int32, arrayID int64, length int, sdl []byte) ([]byte, error)`
  → parse `op_slice_response` (element count, byte count, data via `recvPacketsAlignment`);
  treat `op_response` as error path (fbx pattern: error message from response)
- `opPutSlice(transHandle int32, arrayID int64, length int, sdl []byte, data []byte) (int64, error)`
  → array ID from `opResponse` blob-id field; error if 0
- Respect the existing context/deadline plumbing (`opResponseTimeout` path) so array ops
  honor statement timeouts like other round-trips

**Phase 1 — metadata** (`array_meta.go`, `xsqlvar.go`, `statement.go`)
- `ArrayMeta` struct; system-table queries (FB <6 and ≥6 dialects — port both, guard by
  `wp.getOdsVersion()`/server info already available on the conn)
- Attach to `xSQLVAR.arrayMeta` for output + input descriptors
- Unit-testable SQL builders (golden tests, no server needed)

**Phase 2 — encode** (`array.go`, `driver_go18.go`, `wireprotocol.go`)
- `FlatArray[T]`/`Array[T]` + `ArrayValue`, `driver.Valuer` implementations, element encoders
  (reuse per-type byte-building from `paramsToBlr` cases; factor small helpers rather than
  duplicating)
- `CheckNamedValue` on `firebirdsqlStmt` (pass `ArrayValue`, delegate the rest)
- `paramsToBlr` case → `opPutSlice` → quad bind
- Batch path: verify array params are rejected with a clear message (`batch_encode.go`)

**Phase 3 — decode** (`array.go`, `xsqlvar.go`, `wireprotocol.go`)
- `sql.Scanner` implementations, element decoders (reuse `xSQLVAR.value()` via a synthetic
  element `xSQLVAR{sqltype, sqlscale, sqllen}` — the decode helpers are already methods on it)
- `value()` case `SQL_TYPE_ARRAY` → `opGetSlice` → `ArrayValue`
- `scantype()` case for `SQL_TYPE_ARRAY` (currently missing → `reflect.TypeOf(ArrayValue{})`)
- NULL handling: array ID `0`/null bitmap bit ⇒ scan nil ⇒ `Valid=false`

**Phase 4 — codec depth** (`array.go`)
- Multi-dimensional + non-1-based lower bounds + partial-array shrinking (port fbx
  `getActualSliceDimensions`/`getActualSlice`/`decodeBinary` real-index mapping)
- Ragged detection; `allElementsCount` overflow guards
- Timezone-aware TIME/TIMESTAMP(_TZ) elements (pass `p.timezone` through)

**Phase 5 — tests, docs, CI**
- README section + `doc.go` example; `FBX_FEATURE_GAP.md` §8 rows updated
- Test matrix (below)

## 6. Test plan

Unit (no server, follow `wireprotocol_parse_test.go`/`dsn_test.go` style):

- SDL golden bytes for: 1-D `[1:5]` int, 2-D `[0:2,0:2]`, lower bound ≠ 1 (`sdl_do2` path),
  VARCHAR element with scale/charset, TEXT element
- `sliceLength` math per element type incl. VARCHAR/SHORT/BOOLEAN overheads
- Element codec round-trips: every in-scope type, big-endian + padding assertions,
  NULL-element and ragged-input errors, partial-array dimension shrinking
- Metadata SQL builders (quote-escaping of table/field names)
- `CheckNamedValue` pass-through behavior

Live (gated like `protocol_live_test.go`; runs in the existing FB2.5/3.0/4.0/5.0 CI matrix
from `.github/workflows/`):

- `CREATE TABLE arr_t (ids INT[1:5], nums NUMERIC(15,2)[1:3], names VARCHAR(10)[1:4],
  flag BOOLEAN[1:2], stamps TIMESTAMP[1:2], grid INT[0:2,0:2])`
- INSERT param → SELECT round-trip per element type; NULL array; array of max-length
  VARCHAR elements (slice-length boundary); multi-dim + partial grid; UPDATE via put_slice;
  array in `WHERE` (server-side slice); error paths (too-long slice, ragged slice,
  NULL element)
- Wire interop sanity: `op_get_slice`/`op_put_slice` are InterBase-era ops — expect green on
  2.5–5.0; if a server rejects SDL dialect details, gate that case by negotiated protocol
  version like the scroll/blob gates do

## 7. Risks / open questions

1. **VARYING slice-length prefix (2 vs 4 bytes)** — fbx over-allocates by 2 bytes/element in
   `getSliceLength` but encodes a 4-byte prefix; over-allocation makes this safe, but the
   *decode* side must read 4-byte prefixes (fbx does). Verify with a live VARCHAR array
   before Phase 3 sign-off.
2. **Metadata lookup cost at prepare** — two extra system-table round-trips per statement
   containing array columns (fbx pays the same). Acceptable; optimize later with a per-conn
   cache invalidated by DDL (`RDB$CONTEXT` not available — document the stale-metadata risk
   after `ALTER TABLE` on the same connection).
3. **CheckNamedValue placement** — `database/sql` prefers the stmt checker, falls back to
   conn; `firebirdsqlConn` currently implements neither. Implement on the conn so
   `db.Exec` (no explicit prepare) and prepared statements share behavior.
4. **Concurrent rows + slice fetches** — `opGetSlice` is a synchronous mid-`Next()` round-trip
   on the same wire; safe today because `readRow` is strictly sequential, but must be
   documented as incompatible with any future concurrent-fetch work.
5. **Delimited/case-sensitive identifiers** — metadata SQL must escape quotes (fbx does);
   describe-returned `relname`/`fieldname` are used verbatim as keys.
6. **Red Database / FB6 schema-qualified arrays** — struct fields reserved; queries need the
   `RDB$SCHEMA_NAME` variants (fbx lines 1958–2073). Defer with the schemas feature.

## 8. Effort estimate

| Piece | LOC (impl + unit tests) |
|---|---|
| Wire ops + SDL | ~250 + ~200 |
| Metadata | ~200 + ~150 |
| Codec + public types | ~450 + ~350 |
| Integration (exec/scan/converter) | ~150 + (covered live) |
| Live tests | ~400 |
| **Total** | **~2,150** |

Comparable in size to the merged `wire-protocol-improvements` branch. Phases 0–2 form the
first reviewable PR; 3–5 the second.
