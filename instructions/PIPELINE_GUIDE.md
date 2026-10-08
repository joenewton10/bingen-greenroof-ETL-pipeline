# Pipeline Guide

## Purpose

This guide describes the end-to-end data workflow from raw CSV ingestion to dashboard-ready synchronized data.

## Stages

1. Ingestion
- Inputs: CSV files in `data/raw/greenroof/` and `data/raw/parkplatz/`, `.dat` files in `data/raw/black_globes/`
- Outputs: `ingested_empower_greenroof`, `ingested_kissel_greenroof`, `ingested_parkplatz`, `ingested_black_globes`
- Incremental: greenroof/parkplatz skip any file whose size+modified-time already match what was recorded at its last ingest (a changed file is deleted and re-ingested); black-globes re-parses every file each run but upserts by timestamp, since overlapping logger exports are routine for that source. Tracking lives in the `_ingest_file_manifest` table.

2. QC Filtering
- Applies sensor and radiation bounds
- Outputs: `qc_filtered_greenroof`, `qc_filtered_parkplatz`, `qc_filtered_black_globes`

3. Harmonization
- Greenroof: maps source columns to canonical schema (`harm_greenroof`)
- Parkplatz: maps source columns to canonical/analysis schema (`harm_parkplatz`)
- Black-globes: maps to `globe_temp_<site>` columns (`harm_black_globes`)

4. Synchronization
- Minute-level `INNER JOIN` of `harm_greenroof`/`harm_parkplatz` (the backbone — both required), `LEFT JOIN` of `harm_black_globes` (enrichment — its absence never drops a row)
- Output: `synchronized_data_filtered`

5. Yearly Partitioning
- Builds partitioned structure for long-term query performance
- Output parent: `synchronized_data_yearly`

## Commands

**Normal use — updating the live database:**

```bash
python scripts/run_update.py
```

(or double-click `Run Update.bat`). This builds into a staging file, verifies it, and only then atomically swaps it in as the live database — safe to run anytime, and safe to interrupt or have fail partway through.

**Development/debugging — operates on whatever database `PIPELINE_DB_PATH` points to, or the live database directly if unset (with a confirmation prompt, since that has no staging/verification safety net):**

Full run:

```bash
python scripts/run_pipeline.py
```

Dry run:

```bash
python scripts/run_pipeline.py --dry-run
```

Resume from a stage:

```bash
python scripts/run_pipeline.py --from qc-filter
```

Run range:

```bash
python scripts/run_pipeline.py --from harmonize --to partition
```

Post-run verification:

```bash
python scripts/verify_pipeline.py
```

## Dashboard

After a successful run:

```bash
streamlit run dashboard/app.py
```
