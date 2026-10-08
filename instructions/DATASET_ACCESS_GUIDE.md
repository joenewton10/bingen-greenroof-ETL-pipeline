# Dataset Access Guide

## Quick Start

The live database is already a single portable DuckDB file
(`outputs/bingen_greenroof.duckdb`) — you can query it directly with the
`duckdb` Python package or the DuckDB CLI, no server involved. The formats
below are additional export options for sharing with tools that don't speak
DuckDB (Excel-adjacent SQLite browsers, plain pandas/Parquet workflows):

---

## Option 0: Query the Live DuckDB File Directly ⭐ RECOMMENDED

**Status**: Available right now — no export step, always reflects the latest update.

This is the dataset itself, not a copy of it, so there's nothing to keep in
sync. Ways in, from most to least reliable on a locked-down institutional PC:

**The dashboard's own Browse Data / Query Data views** — the most reliable
option here. Open the dashboard (`Open Dashboard.bat`) and switch to either
in the sidebar **View** switch, running entirely on the dashboard's existing
Streamlit + `duckdb` dependencies. No extra install, no extension download,
nothing that a university/corporate Application Control Policy has anything
to object to — it's the same stack the dashboard itself already runs on.
- **📄 Browse Data** — page through the whole table, no SQL needed. Best for
  "just let me look at the data."
- **🔍 Query Data** — a SQL box, with a full (uncapped) CSV download of
  whatever your query matches. Best for a specific slice or your own
  analysis.

**DBeaver** (free desktop app, works with lots of databases, good if you
already use it and want a fuller desktop tool): install
[DBeaver Community Edition](https://dbeaver.io/), add a DuckDB connection,
point it at `outputs/bingen_greenroof.duckdb`.

**Python / pandas** (best if you're doing your own analysis rather than
browsing):
```python
import duckdb
conn = duckdb.connect("outputs/bingen_greenroof.duckdb", read_only=True)
df = conn.execute("SELECT * FROM synchronized_data_filtered WHERE timestamp > '2024-01-01'").df()
```

**DuckDB's own built-in UI** (`CALL start_ui()`) exists too, but needs to
download a native extension the first time it runs — on this project's own
deployment PC, that download was blocked outright by Application Control
Policy, and the same is likely on any similarly-managed institutional
machine. Try it if you like, but don't rely on it:
```bash
python -c "import duckdb; duckdb.connect('outputs/bingen_greenroof.duckdb', read_only=True).sql('CALL start_ui()')"
```

Always connect with `read_only=True` (or the DBeaver/UI default, which is
also read-only) — the pipeline is the only thing that should ever write to
this file, and an update always builds into a separate staging file before
swapping in, so a stray write here isn't something an update can protect
against.

---

## Option 1: SQLite Database

**Status**: Export in progress (`scripts/export_to_sqlite.py`)  
**File**: `outputs/bingen_greenroof.db` (~600-800 MB)  
**Yearly splits**: `outputs/bingen_greenroof_yearly/*.db`

### Why SQLite?
✅ Portable single file (no server needed)  
✅ Full SQL query support (like PostgreSQL)  
✅ Works on Windows/Mac/Linux  
✅ Open with Excel tools or command line  

### How to Use

**In Python**:
```python
import pandas as pd
import sqlite3

# Open database
conn = sqlite3.connect('outputs/bingen_greenroof.db')

# Load all data
df = pd.read_sql_query(
    'SELECT * FROM synchronized_data_filtered',
    conn
)

# Query with filters
df_2024 = pd.read_sql_query(
    'SELECT * FROM synchronized_data_filtered '
    'WHERE timestamp > ? AND timestamp < ?',
    conn,
    params=('2024-01-01', '2024-12-31')
)

conn.close()
```

**In Command Line**:
```bash
sqlite3 outputs/bingen_greenroof.db
> SELECT COUNT(*) FROM synchronized_data_filtered;
> .schema synchronized_data_filtered
> SELECT * FROM synchronized_data_filtered LIMIT 10;
> .quit
```

**In Excel/Database Tools**:
1. Download [DB Browser for SQLite](https://sqlitebrowser.org/) (free, open-source)
2. Open `.db` file
3. Browse tables, run queries, export to CSV/Excel

---

## Option 2: Parquet Files

**Status**: Available  
**Files**: 
- `outputs/synchronized_data_complete.parquet` (all records)
- `outputs/parquet_yearly/*.parquet` (yearly splits)

### How to Use

```python
import pandas as pd

# Load complete dataset
df = pd.read_parquet('outputs/synchronized_data_complete.parquet')

# Or load yearly
df_2024 = pd.read_parquet('outputs/parquet_yearly/2024.parquet')

# Query with pandas
greenroof_data = df[df['timestamp'] > '2024-01-01']
```

---

## Option 3: CSV Files

**Generate on demand**: `scripts/generate_csv_exports.py` (to be created)

```python
# Load complete CSV
df = pd.read_csv('outputs/synchronized_data_complete.csv')

# Works with Excel (in chunks due to size)
```

---

## Data Schema

### Main Table: `synchronized_data_filtered`

The column list below is generated from the pipeline's actual SQL
(`pipeline/sync/sync_sites.py`), not hand-maintained — if it and the code
ever disagree, trust the code and treat this as stale. One row per minute.

**Columns** (54 total):

| Column | Type | Description |
|--------|------|-------------|
| timestamp | TIMESTAMP | Minute-level synchronized timestamp |
| **Parkplatz sensor averages** (15 cols) | | |
| avg_ir1_parkplatz | DOUBLE | Incoming longwave radiation (W/m²) |
| avg_sr1_parkplatz | DOUBLE | Incoming shortwave radiation (W/m²) |
| avg_ir2_parkplatz | DOUBLE | Outgoing longwave radiation (W/m²) |
| avg_sr2_parkplatz | DOUBLE | Reflected shortwave radiation (W/m²) |
| avg_temp_parkplatz | DOUBLE | Station temperature (°C) |
| avg_air_pressure_parkplatz | DOUBLE | Air pressure (mbar) |
| avg_soil_moisture_parkplatz | DOUBLE | Soil moisture (vol%) |
| avg_air_humidity_1_parkplatz | DOUBLE | Air humidity, level 1 (%RH) |
| avg_air_temp_1_parkplatz | DOUBLE | Air temperature, level 1 (°C) |
| avg_air_humidity_2_parkplatz | DOUBLE | Air humidity, level 2 (%RH) |
| avg_air_temp_2_parkplatz | DOUBLE | Air temperature, level 2 (°C) |
| avg_wind_speed_parkplatz | DOUBLE | Wind speed (m/s) |
| avg_wind_direction_parkplatz | DOUBLE | Wind direction (°) |
| avg_soil_temp_1_parkplatz | DOUBLE | Soil temperature, sensor 1 (°C) |
| avg_soil_temp_2_parkplatz | DOUBLE | Soil temperature, sensor 2 (°C) |
| **Greenroof sensor averages** (11 cols) | | |
| avg_ir1_greenroof | DOUBLE | Incoming longwave radiation (W/m²) |
| avg_air_temperature_greenroof | DOUBLE | Air temperature (°C) |
| avg_air_temp_2_greenroof | DOUBLE | Secondary-level air temperature (°C) |
| avg_air_humidity_1_greenroof | DOUBLE | Relative humidity (%RH) |
| avg_air_humidity_2_greenroof | DOUBLE | Secondary-level humidity (%RH) |
| avg_wind_speed_greenroof | DOUBLE | Wind speed (m/s) |
| avg_soil_temperature_greenroof | DOUBLE | Soil temperature (°C) |
| avg_soil_moisture_greenroof | DOUBLE | Soil moisture (vol%) |
| avg_global_radiation_greenroof | DOUBLE | Incoming shortwave radiation (W/m²) |
| avg_sr2_greenroof | DOUBLE | Reflected shortwave radiation (W/m²) |
| avg_ir2_greenroof | DOUBLE | Outgoing longwave radiation (W/m²) |
| **Black-globe temperature averages** (4 cols) — NULL before ~Aug 2026, or for any minute the logger didn't report | | |
| avg_globe_temp_gruendach | DOUBLE | Black-globe temp at the green roof site (°C) |
| avg_globe_temp_parkplatz | DOUBLE | Black-globe temp at the parking lot site (°C) |
| avg_globe_temp_mobiga_nord | DOUBLE | Black-globe temp, MoBiGa north plot (°C) |
| avg_globe_temp_mobiga_sued | DOUBLE | Black-globe temp, MoBiGa south plot (°C) |
| **Record counts & availability** (5 cols) | | |
| parkplatz_record_count | BIGINT | Raw readings averaged into this minute (parkplatz) |
| greenroof_record_count | BIGINT | Raw readings averaged into this minute (greenroof) |
| black_globes_record_count | BIGINT | Raw readings averaged into this minute (black-globe) |
| has_dual_level_greenroof | BOOLEAN | Whether both greenroof sensor levels reported this minute |
| measurement_period | VARCHAR | `'dual_level'` or `'single_level'`, per has_dual_level_greenroof |
| **Temperature/humidity differentials** (6 cols) | | |
| temp_diff_1 | DOUBLE | Greenroof air temp − parkplatz air temp (level 1) |
| temp_diff_2 | DOUBLE | Greenroof air temp − parkplatz air temp (level 2) |
| delta_t_roof | DOUBLE | Greenroof temp, level 1 − level 2 |
| delta_rh_roof | DOUBLE | Greenroof humidity, level 1 − level 2 |
| delta_t_parkplatz | DOUBLE | Parkplatz temp, level 1 − level 2 |
| delta_rh_parkplatz | DOUBLE | Parkplatz humidity, level 1 − level 2 |
| **Energy & radiation balance** (6 cols) | | |
| energy_from_air_parkplatz | DOUBLE | Stefan-Boltzmann estimate from air-side IR (W/m²) |
| energy_from_surface_parkplatz | DOUBLE | Stefan-Boltzmann estimate from surface-side IR (W/m²) |
| radiation_balance_greenroof | DOUBLE | Net radiation: (SR1−SR2)+(IR1−IR2), greenroof (W/m²) |
| radiation_balance_parkplatz | DOUBLE | Net radiation: (SR1−SR2)+(IR1−IR2), parkplatz (W/m²) |
| albedo_greenroof | DOUBLE | Reflected/incoming shortwave ratio, greenroof |
| albedo_parkplatz | DOUBLE | Reflected/incoming shortwave ratio, parkplatz |
| **Specific humidity** (6 cols, Magnus formula, Bingen pressure 1002 hPa) | | |
| q_greenroof_50cm | DOUBLE | Specific humidity at 0.5m, greenroof (g/kg) |
| q_greenroof_2m | DOUBLE | Specific humidity at 2.0m, greenroof (g/kg) |
| q_parkplatz_50cm | DOUBLE | Specific humidity, level 1, parkplatz (g/kg) |
| q_parkplatz_2m | DOUBLE | Specific humidity, level 2, parkplatz (g/kg) |
| delta_q_roof | DOUBLE | Greenroof specific-humidity gradient (0.5m − 2m) |
| delta_q_parkplatz | DOUBLE | Parkplatz specific-humidity gradient (level 1 − level 2) |

**Time range, record count**: don't rely on a hardcoded number here — it
changes every update. Query it directly:
```sql
SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM synchronized_data_filtered;
```
**Partitioning**: by year, via `synchronized_data_yearly` (one `sync_data_<year>` table per year, unioned).

---

## Export Scripts Available

### 1. SQLite Export ✓
```bash
python scripts/export_to_sqlite.py
```
Generates: `outputs/bingen_greenroof.db` and yearly splits

### 2. Parquet Export ✓
```bash
python scripts/export_to_parquet.py
```
Generates: Complete and yearly `.parquet` files

### 3. Fast Export (DuckDB fallback) ✓
```bash
python scripts/fast_export_parquet.py
```
Uses DuckDB's PostgreSQL extension for speed

---

## Common Queries (SQLite)

### Get all 2024 data
```sql
SELECT * FROM synchronized_data_filtered
WHERE strftime('%Y', timestamp) = '2024'
LIMIT 10;
```

### Average temperature by month
```sql
SELECT 
    strftime('%Y-%m', timestamp) as month,
    AVG(avg_air_temperature_greenroof) as avg_temp_greenroof,
    AVG(avg_temp_parkplatz) as avg_temp_parkplatz
FROM synchronized_data_filtered
GROUP BY strftime('%Y-%m', timestamp)
ORDER BY month;
```

### Find high albedo events
```sql
SELECT timestamp, albedo_greenroof, albedo_parkplatz
FROM synchronized_data_filtered
WHERE albedo_greenroof > 0.3 OR albedo_parkplatz > 0.3
ORDER BY timestamp DESC
LIMIT 100;
```

### Temperature difference analysis
```sql
SELECT 
    timestamp,
    avg_air_temperature_greenroof - avg_temp_parkplatz as temp_diff
FROM synchronized_data_filtered
WHERE timestamp > '2024-06-01' AND timestamp < '2024-09-01'
ORDER BY temp_diff DESC
LIMIT 20;
```

---

## Sharing the Dataset

### Option A: SQLite (Recommended for sharing)
- Single file, portable
- No installation needed
- Download SQLite Browser to open

### Option B: Parquet
- Compressed, efficient
- Best for data science workflows
- Install: `pip install pandas pyarrow`

### Option C: CSV (if needed)
- Universal, works everywhere
- Large file size (~1.5-2 GB uncompressed)

---

## Installation for Data Analysis

### Python Setup
```bash
# If not using SQLite natively
pip install pandas sqlite3

# For advanced queries
pip install duckdb  # Optional, faster SQL processing
```

### Database Tools (Free)
- **DB Browser for SQLite**: https://sqlitebrowser.org/
- **DataGrip**: https://www.jetbrains.com/datagrip/ (free trial)
- **VS Code Extension**: SQLite Viewer

---

## File Sizes (Approximate)

| Format | Size | Best For |
|--------|------|----------|
| DuckDB | ~1-2 GB (live DB) | This is the live database — query it directly with `duckdb` |
| SQLite | 600-800 MB | Sharing, portable analysis |
| Parquet | 200-300 MB (compressed) | Python/data science |
| CSV | 1.5-2 GB | Excel, universal access |

---

## Troubleshooting

### "SQLite database is locked"
- Close other applications accessing the file
- Use read-only mode: `sqlite3 file.db <query>`

### "File too large for Excel"
- Use SQLite query to export filtered CSV
- Or use pandas with chunking

### "ODBC connection failed"
- Verify path is correct
- Use absolute paths: `C:\full\path\to\file.db`

---

## Next Steps

1. **Export**: Run `python scripts/export_to_sqlite.py`
2. **Download**: Get `outputs/bingen_greenroof.db`
3. **Open**: Use DB Browser or Python
4. **Analyze**: Query the data using SQL or pandas

---

**Just want the live data directly?**  
Query the DuckDB file straight, or reuse the dashboard's own analyzer:
```python
import duckdb
df = duckdb.connect("outputs/bingen_greenroof.duckdb", read_only=True) \
    .execute("SELECT * FROM synchronized_data_filtered WHERE timestamp > '2024-01-01'") \
    .df()

# or, for the same derived columns the dashboard uses:
from dashboard.analysis import BingenGreenRoofAnalyzer
analyzer = BingenGreenRoofAnalyzer()
df = analyzer.load_data(year=2024)
```

**Note:** `scripts/export_to_sqlite.py`, `scripts/export_to_parquet.py`, and
`scripts/fast_export_parquet.py` still connect to PostgreSQL as of this
writing and need to be re-pointed at the DuckDB file before they'll run —
this wasn't done as part of the DuckDB migration since none of them are on
the core pipeline path. Swapping their `psycopg2`/`create_engine("postgresql", ...)`
calls for a `duckdb.connect(DUCKDB_PATH)` read is a small, self-contained fix.
