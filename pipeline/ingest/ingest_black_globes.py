"""
Ingest black-globe temperature logger files into ingested_black_globes table.

Table naming convention for Bingen pipeline:
- Output: ingested_black_globes

Source format: Campbell Scientific TOA5 (CR350 logger, program "BlackGlobes.CRB").
TOA5 files have 4 fixed header rows, then data:
  1: station info line ("TOA5","<station>","<model>",...)
  2: column names ("TIMESTAMP","RECORD","MoBiGaNord_Avg",...)
  3: units ("TS","RN","Deg C",...)
  4: statistic type ("","","Avg",...)

The deployment auto-exports new .dat files into data/raw/black_globes/ over
time (LoggerNet convention: <station>_<table>_<export-timestamp>.dat), and
successive exports routinely re-cover the tail of the previous export. So
this module (a) globs every .dat file present at run time rather than
assuming a fixed file, and (b) dedupes by timestamp across all files via a
staging table, rather than trusting files to be disjoint.
"""
import os
import csv
from datetime import datetime
from pipeline.ingest.base import get_connection, bulk_insert
from config.settings import BLACK_GLOBES_DIR


# TOA5 source column name -> DB column name. Position-independent: the
# column order is read from each file's own header row rather than assumed,
# so a reordered export still maps correctly. A future file missing one of
# these names (e.g. the logger program changed) is rejected rather than
# silently mis-mapped.
TOA5_HEADER_MAP = {
    'TIMESTAMP': 'timestamp',
    'RECORD': 'record_number',
    'MoBiGaNord_Avg': 'mobiga_nord_c',
    'MoBiGaSued_Avg': 'mobiga_sued_c',
    'Gruendach_Avg': 'gruendach_c',
    'Parkplatz_Avg': 'parkplatz_c',
}

STAGING_COLUMNS = [
    'timestamp', 'record_number',
    'mobiga_nord_c', 'mobiga_sued_c', 'gruendach_c', 'parkplatz_c',
    'source_file',
]

CREATE_SEQ_SQL = 'CREATE SEQUENCE IF NOT EXISTS seq_ingested_black_globes START 1;'

# timestamp is UNIQUE (not just indexed): this source is a single logger
# emitting one composite row per timestamp across its 4 site columns, unlike
# greenroof/parkplatz where multiple distinct loggers can legitimately share
# a timestamp. That makes an upsert-by-timestamp correct here, and it's what
# lets re-running ingest on overlapping auto-exports update in place instead
# of accumulating duplicate rows across runs.
CREATE_SQL = '''
CREATE TABLE IF NOT EXISTS ingested_black_globes (
    id BIGINT PRIMARY KEY DEFAULT nextval('seq_ingested_black_globes'),
    timestamp TIMESTAMP UNIQUE,
    record_number BIGINT,
    mobiga_nord_c DOUBLE,
    mobiga_sued_c DOUBLE,
    gruendach_c DOUBLE,
    parkplatz_c DOUBLE,
    source_file TEXT,
    import_timestamp TIMESTAMP DEFAULT now()
);
'''

CREATE_STAGING_SQL = '''
CREATE TEMP TABLE _stg_ingested_black_globes (
    timestamp TIMESTAMP,
    record_number BIGINT,
    mobiga_nord_c DOUBLE,
    mobiga_sued_c DOUBLE,
    gruendach_c DOUBLE,
    parkplatz_c DOUBLE,
    source_file TEXT
);
'''

UPSERT_SQL = '''
INSERT INTO ingested_black_globes
    (timestamp, record_number, mobiga_nord_c, mobiga_sued_c, gruendach_c, parkplatz_c, source_file)
SELECT timestamp, record_number, mobiga_nord_c, mobiga_sued_c, gruendach_c, parkplatz_c, source_file
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY timestamp
            ORDER BY source_file DESC, record_number DESC
        ) AS rn
    FROM _stg_ingested_black_globes
) ranked
WHERE rn = 1
ORDER BY timestamp ASC
ON CONFLICT (timestamp) DO UPDATE SET
    record_number    = excluded.record_number,
    mobiga_nord_c     = excluded.mobiga_nord_c,
    mobiga_sued_c     = excluded.mobiga_sued_c,
    gruendach_c       = excluded.gruendach_c,
    parkplatz_c       = excluded.parkplatz_c,
    source_file       = excluded.source_file,
    import_timestamp  = now();
'''


def _read_toa5_lines(filepath):
    """Read a TOA5 file once using utf-8 or latin-1 fallback."""
    try:
        with open(filepath, encoding='utf-8') as f:
            return f.readlines()
    except UnicodeDecodeError:
        with open(filepath, encoding='latin-1') as f:
            return f.readlines()


def _parse_toa5_timestamp(raw):
    """Parse a TOA5 TIMESTAMP field ('YYYY-MM-DD HH:MM:SS'); None if invalid."""
    raw = (raw or '').strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None


def _parse_toa5_value(raw):
    """Parse a TOA5 numeric field; treats '' and 'NAN' (Campbell's missing marker) as NULL."""
    raw = (raw or '').strip()
    if raw == '' or raw.upper() == 'NAN':
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_toa5_file(filepath):
    '''Parse one TOA5 .dat file into a list of insert tuples matching STAGING_COLUMNS.'''
    lines = _read_toa5_lines(filepath)
    if len(lines) < 5:
        print(f"  WARNING: {os.path.basename(filepath)} has fewer than 5 lines (no data rows), skipping.")
        return []

    header = next(csv.reader([lines[1]]))
    try:
        col_index = {name: header.index(name) for name in TOA5_HEADER_MAP}
    except ValueError as exc:
        print(f"  WARNING: {os.path.basename(filepath)} missing expected TOA5 column ({exc}), skipping.")
        return []

    source_file = os.path.basename(filepath)
    records = []
    for row in csv.reader(lines[4:]):
        if not row or len(row) < len(header):
            continue

        timestamp = _parse_toa5_timestamp(row[col_index['TIMESTAMP']])
        if timestamp is None:
            continue

        record_number_raw = _parse_toa5_value(row[col_index['RECORD']])
        record_number = int(record_number_raw) if record_number_raw is not None else None

        records.append((
            timestamp,
            record_number,
            _parse_toa5_value(row[col_index['MoBiGaNord_Avg']]),
            _parse_toa5_value(row[col_index['MoBiGaSued_Avg']]),
            _parse_toa5_value(row[col_index['Gruendach_Avg']]),
            _parse_toa5_value(row[col_index['Parkplatz_Avg']]),
            source_file,
        ))

    return records


def ingest_black_globes():
    '''Ingest all black-globe TOA5 .dat files, upserting by timestamp.

    Unlike the greenroof/parkplatz ingest modules, this one re-parses every
    .dat file on every run rather than skipping already-seen filenames —
    cheap at this source's scale (a handful of files, sub-second each), and
    necessary because successive LoggerNet auto-exports routinely re-cover
    the tail of the previous export. The upsert (ON CONFLICT on the UNIQUE
    timestamp column) makes re-processing an already-ingested file's
    overlapping rows a no-op update rather than a duplicate insert.
    '''
    conn = get_connection()
    print('[ingest_black_globes] Ensuring table exists...')
    conn.execute(CREATE_SEQ_SQL)
    conn.execute(CREATE_SQL)

    if not os.path.isdir(BLACK_GLOBES_DIR):
        print(f'[ingest_black_globes] Directory not found, skipping: {BLACK_GLOBES_DIR}')
        conn.close()
        return

    all_files = sorted(
        os.path.join(BLACK_GLOBES_DIR, f)
        for f in os.listdir(BLACK_GLOBES_DIR)
        if f.lower().endswith('.dat') and os.path.isfile(os.path.join(BLACK_GLOBES_DIR, f))
    )
    print(f'[ingest_black_globes] Found {len(all_files)} .dat file(s)')

    if not all_files:
        conn.close()
        return

    all_records = []
    for filepath in all_files:
        records = _parse_toa5_file(filepath)
        print(f'  {os.path.basename(filepath)}: {len(records)} rows')
        all_records.extend(records)

    if not all_records:
        print('[ingest_black_globes] No valid rows parsed.')
        conn.close()
        return

    conn.execute(CREATE_STAGING_SQL)
    bulk_insert(conn, '_stg_ingested_black_globes', STAGING_COLUMNS, all_records)
    conn.execute(UPSERT_SQL)
    conn.execute('DROP TABLE _stg_ingested_black_globes;')

    total_rows = conn.execute('SELECT COUNT(*) FROM ingested_black_globes;').fetchone()[0]
    conn.close()
    print(f'[ingest_black_globes] Ingested/updated {total_rows} black-globe records total '
          f'({len(all_records)} raw rows processed this run).')
