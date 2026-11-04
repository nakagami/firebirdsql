# PR draft: Sync with upstream nakagami/firebirdsql (4f1d02a) + client sql_dialect option

Target: IBSurgeon/firebirdsql-go
Branch: upstream-sync-nakagami-4f1d02a (master + 2 commits)

## Commits
1. `d8a02db` — feat: client SQL dialect DSN option (sql_dialect).
   Client SQL dialect (1 or 3, default 3) in the attach DPB, create DPB
   and op_prepare_statement. Required for dialect-1 target databases
   (Firebird 2.5 / InterBase); upstream hardcodes dialect 3.
2. `cddc5e5` — merge nakagami/firebirdsql master 4f1d02a (v0.9.23+, as of
   2026-11-04): two months of upstream work (timezone displacement ids,
   Go 1.27 row scanning, statement record counts, deferred
   op_free_statement, cancel_hard_drop, page_size for createdb, SPB
   deferred auth, events rework, Jaybird-ported test suite). Conflicts
   resolved in favor of upstream — the fork's pre-merge wire-protocol
   features exist upstream in equivalent/newer form (#284/#285/#294/
   #296/#303/#316/#320). sql_dialect re-applied on the upstream code
   structure (wireProtocol.sqlDialect with constructor default 3, DSN
   parsing, three DPB/prepare points); wireProtocol.go.bak /
   protocol_feature_test.go.bak accidentally present in the working
   tree are excluded; new test TestCreateDPBClientSQLDialect covers
   dialect 1 and 3 in the DPB.

## Verification
- Full upstream live suite vs Firebird 5.0: 0 failures (~140 s).
- FirstAid2 recovery-engine round-trips: dialect-3 export with TIME_TZ/
  TIMESTAMP_TZ (zones Europe/Moscow, America/New_York, +05:30
  displacement) — zones and instants preserved, row-level comparison
  identical; dialect-1 target (Firebird 2.5): schema degrades TIME/
  TIMESTAMP/TZ columns to DATE, values round-trip with fractional
  seconds; 8/8 DROP/ADD regression suite, 7/7 EMPLOYEE tables identical.
