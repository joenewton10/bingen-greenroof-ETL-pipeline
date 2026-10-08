# Quick Start

## 1. Environment Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

No database credentials needed — the pipeline uses a single DuckDB file
(`outputs/bingen_greenroof.duckdb` by default). Copy `config/.env.example` to
`config/.env` only if you want to override that path.

## 2. Pipeline

```bash
python scripts/run_pipeline.py --dry-run
python scripts/run_pipeline.py
python scripts/verify_pipeline.py
```

## 3. Dashboard

```bash
streamlit run dashboard/app.py
```

Open: `http://localhost:8501` (or the port shown by Streamlit)

## Useful Variants

```bash
python scripts/run_pipeline.py --from harmonize --to sync
python scripts/run_pipeline.py --verbose
```
