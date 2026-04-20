# Phase 2 — Silver Transformations

## Goal
Transform raw Bronze tables into cleansed, conformed, query-ready Silver tables.
Enforce types, handle nulls, deduplicate on business keys, and apply Delta
optimizations. Silver is the contract layer that Gold and the semantic model
will trust.

---

## What I built

This phase introduces **`lh_silver`**, a new Lakehouse in the same workspace as
`lh_bronze`. The choice to use separate Lakehouses (rather than one Lakehouse
with `bronze_*` and `silver_*` tables mixed together) gives each layer its own
security boundary, lifecycle, and optimization strategy — and makes the eventual
deployment pipeline in Phase 5 cleaner.

The transformation logic lives in a single PySpark notebook,
`nb_silver_from_bronze`, with both Lakehouses attached and `lh_silver` pinned as
the default. This lets the notebook read from Bronze using fully qualified names
(`lh_bronze.dbo.bronze_customer`) and write to Silver with unqualified names
(`silver_customer`). The schema-enabled Lakehouse namespace (`lakehouse.schema.table`)
is a current Fabric default and required understanding three-part naming.

Each of the 7 Silver tables goes through the same transformation recipe:
standardize column names to snake_case, widen integer surrogate keys to `bigint`,
handle nulls in analytical columns, deduplicate on business keys using a window
function, swap Bronze's audit columns (`_ingested_at`, `_source_file`) for
Silver's own `_silver_loaded_at`, write as Delta with overwrite mode, and run
`OPTIMIZE`. Fact tables additionally get `ZORDER BY` on their most-filtered
columns (`customerkey + orderdate` for sales, `orderkey` for orderrows) to
speed up the queries that Gold and Power BI will run against them.

---

### Delta Time Travel — hands-on verification

### Why it matters
Delta tables track every write as a numbered version in the _delta_log/ folder.
Until VACUUM removes old Parquet files (default 7-day retention), any version
can be queried or restored. This replaces the need for manual backups for
near-term recovery — an engineer who accidentally truncates a table on Friday
can restore it Monday without opening a support ticket.

### What I ran
```python
spark.sql("DESCRIBE HISTORY silver_customer").show()
spark.sql("SELECT COUNT(*) FROM silver_customer VERSION AS OF 0")
spark.sql("SELECT COUNT(*) FROM silver_customer")
```

### What it proved
- `DESCRIBE HISTORY` returned N versions with timestamps and operation names
  (CREATE, WRITE, OPTIMIZE), confirming Delta is actively tracking history.
- Version 0 showed 0 rows (the empty CREATE TABLE step before data load).
- The current version showed the populated row count.
- The gap between those two numbers is proof that Time Travel works — the
  engine can retrieve the table at multiple queryable states.

### What I learned
`DESCRIBE HISTORY` and `VERSION AS OF` are Spark SQL commands — they run in
notebooks, not in the T-SQL analytics endpoint. Both engines read the same
Delta files in OneLake; they just support different dialects. Delta-specific
maintenance (Time Travel, OPTIMIZE, VACUUM, MERGE) is always Spark SQL.

### Production caveat
Time Travel's recovery window is bounded by VACUUM retention. A team running
VACUUM with 1-day retention cannot recover data from 3 days ago — those
Parquet files are permanently gone. Setting retention is a trade-off between
storage cost and recovery time, and the decision should be conscious, not
default.

---

## ✅ Phase 2 — Silver transformations

**What was built:**

- **Second Lakehouse** `lh_silver` in the same workspace — physical layer
  separation for independent security, lifecycle, and optimization
- **PySpark notebook** `nb_silver_from_bronze` with both Lakehouses attached,
  reading from `lh_bronze.dbo` and writing to `lh_silver` (default)
- **7 cleansed Silver tables** with snake_case column names, widened `bigint`
  surrogate keys, null handling, and deduplication on business keys
- **Delta optimizations applied**: `OPTIMIZE` on all tables, `ZORDER BY` on
  fact tables for the columns most likely to be filtered in Gold queries
- **Verified via SQL endpoint**: row counts, schema, zero duplicates,
  referential integrity holds, Time Travel history accessible

### Screenshots

Refer in the screenshots folder. 

### Artifacts

- `notebooks/silver_from_bronze.ipynb` — exported Spark notebook
- `docs/phase-2-silver.md` — full writeup with DP-600 concepts and honest
  lessons learned

concepts this phase covered

Lakehouse schemas (schema-enabled Lakehouse)
OPTIMIZE, V-Order, and VACUUM — the Delta optimization trifecta
Delta Time Travel
Surrogate key widening (int → bigint)

---

## What I got wrong the first time

1. **Assumed the older two-part Lakehouse syntax.** My first version of the
   notebook used `spark.sql("SHOW TABLES IN lh_bronze")` which failed on a
   schema-enabled Lakehouse with a confusing `SCHEMA_NOT_FOUND` error.
   **Fix:** schema-enabled Lakehouses require three-part naming
   (`lh_bronze.dbo.bronze_customer`) and `SHOW TABLES IN lh_bronze.dbo`.

2. **Assumed column names without inspecting Bronze first.** The Contoso
   release I used has `birthday` (not `birthdate`), `countryname` (not
   `country`), and no `country` column at all on `bronze_store`. Hit three
   `UNRESOLVED_COLUMN` errors before I learned to always `printSchema()` first.
   **Lesson:** never write downstream code against assumed column names —
   always inspect the upstream schema. This is exactly the schema drift problem
   Silver is supposed to protect Gold from.

3. **Redundantly cast columns that Bronze already had typed correctly.**
   Bronze's `inferSchema=true` in Phase 1 actually produced good types for
   dates and doubles. I was pattern-matching from generic Spark tutorials
   rather than looking at what Bronze had actually stored. **Fix:** only cast
   columns that need changing — most notably, widening integer keys to long.

4. **Didn't realize `bronze_sales` and `bronze_orderrows` have near-identical
   schemas.** Both are at the line-item grain with overlapping columns. In
   Gold I'll pick one (sales, because it has `ExchangeRate` for currency
   conversion) and drop the other, but Silver carries both for completeness.

---

## What I'd do differently in production

- **Explicit schema contracts**: instead of `inferSchema=true` in Bronze, define
  a `StructType` for each source and fail loudly on drift. This would have
  caught the `birthday` vs `birthdate` issue upstream.
- **Incremental Silver loads**: replace full `overwrite` with MERGE against a
  watermark column (e.g., `_ingested_at > last_processed_ts`). Overwrite is
  wasteful at production scale.
- **SCD Type 2 on dimensions**: track historical changes to customer attributes
  (address, occupation) with effective-date columns. Current setup only keeps
  the latest row, losing history.
- **Data quality checks as a separate step**: add row-count assertions, null
  rate checks, and referential integrity checks as a dedicated notebook that
  fails the pipeline on threshold violations. Don't silently pass bad data to
  Gold.
- **Schedule VACUUM weekly with 7-day retention**: balances Time Travel window
  with storage cost. Production Silver should also have documented recovery
  procedures.
- **Consider Liquid Clustering over static ZORDER**: newer Delta feature that
  adapts to changing query patterns without re-running OPTIMIZE.

---

## Artifacts in this repo

- `notebooks/silver_from_bronze.ipynb` — exported Spark notebook
- `screenshots/phase-2-*.png` — verification proofs (row counts, schema,
  dedup, nulls, referential integrity, time travel)

   - New Lakehouse lh_silver with 7 cleansed Delta tables
   - Snake_case naming, bigint key widening, null handling, dedup
   - OPTIMIZE on all tables, ZORDER on sales and orderrows
   - Hit and documented schema-enabled Lakehouse three-part naming (lh.dbo.table)
   - Hit and documented schema-drift issue (birthday vs birthdate)
