
# Phase 1 — Bronze Ingestion

## Goal
Land all 7 Contoso source CSVs into a Fabric Lakehouse as Delta tables,
with full audit trail, using a maintainable pipeline pattern.


## What I built

I built Bronze ingestion using a metadata-driven pipeline with ForEach + dynamic expressions, so adding a new source table is one-line change to the array — not a new activity. I chose HTTP/GitHub hosting for the learning environment but designed the pipeline so swapping to ADLS Gen2 would only change the source connector.

This phase establishes the **Bronze layer** of the medallion architecture — the
raw, immutable landing zone for source data. The goal isn't transformation; it's
preservation. Every byte that arrives from the source is kept as-is so that
downstream layers can be rebuilt from scratch if business logic changes, and so
that any anomaly in a Gold report can be traced back to its original row.

The ingestion runs in two stages. **Stage one** is a Fabric Data Factory pipeline
(`pl_ingest_contoso_to_bronze`) that pulls 7 Contoso CSVs from a public GitHub
repository into the Lakehouse `Files/` area.

**Stage two** is a PySpark notebook (`bronze_load_delta.ipynb`) that reads each
CSV folder from `Files/raw/` and writes it to the Lakehouse `Tables/` area as a
Delta table. 


## Fabric concepts this phase covered

### Lakehouse vs. Warehouse
### Medallion Architecture
### Pipeline Copy activity vs. Dataflow Gen2 / Shortcut / Mirroring
### ForEach + dynamic expressions


## What I got wrong the first time (honest lessons)

1. **Assumed the Contoso dataset came as raw CSVs.** It actually ships as a
   `.7z` archive that Fabric Copy activity can't natively extract.
   Lesson:
   always inspect source format before designing the ingestion pattern.
   Solution:
   extract locally, then host the CSVs in a staging location Fabric
   can reach.

2. **Tried to pass `lh_bronze` as a string into the Lakehouse destination
   field.** Produced `InvalidExternalReferenceConnection: Invalid
   datasourceObjectId: lh_bronze passed. It should be a valid Guid.`
   Lesson:
   in Fabric Copy activities, connection references must be picked from UI
   dropdowns (they resolve to GUIDs internally) — only paths and filenames can
   be dynamic expressions.

3. **Initially considered using a Shortcut instead of a Copy activity.**
   Realized that for Bronze — which requires *preserving* a raw snapshot —
   copying is architecturally correct. Shortcuts leave data at the source and
   don't give a point-in-time audit trail, which defeats Bronze's purpose.

4. **Had to choose between ADLS Gen2 (DP-600-flavored, Azure subscription
   required) and GitHub (zero setup) for source hosting.** Picked GitHub
   because creating an Azure subscription just to host 7 CSVs was friction
   with no learning payoff for a trial-scoped project. Documented the
   trade-off openly rather than pretending the choice didn't exist.


## Key Learnings & Challenges Resolved

- **Repository Setup Issue (Repository Not Found)**
- **Understanding Empty Repository Behavior**
- **Handling Large File Upload Limitations in GitHub**
- **Managing Local File Placement for Git Tracking**
- **Resolving Fabric Notebook Execution Issue (Run Disabled)**
- **Understanding ForEach and Nested Activities in Data Factory**
- **Exporting Pipeline and Notebook Artifacts for Version Control**
- **Implementing .gitignore for Clean Repository Management**


## What I'd do differently in production
- Use ADLS Gen2 source with Managed Identity auth (skipped here for trial-account simplicity)
- Replace ForEach `sequential=true` with controlled parallelism (`batchCount: 4`)
- Add a Lookup activity reading a config table of sources, instead of hardcoded `createArray`
- Use incremental ingestion with watermark columns and MERGE, not full overwrite
- Wire a Data Activator alert on pipeline failure

- ---

## Artifacts in this repo

- `pipelines/pl_ingest_contoso_to_bronze.json` — exported pipeline definition
- `notebooks/bronze_load_delta.ipynb` — exported Spark notebook
- `screenshots/phase-1-*.png` — visual proof of the working pipeline
