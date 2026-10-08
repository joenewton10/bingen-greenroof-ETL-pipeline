"""
Ingest Raw/Kissel greenroof CSV files (MXmini format) into the DuckDB pipeline database.
Table: ingested_kissel_greenroof

Based on: actual file analysis of 081000388_*.csv

File format:
- Line 0: Station Name;[value]
- Line 1: Station ID;[value]
- Line 2: Serial No;[value]
- Line 3: Timezone;[value]
- Line 4: ; (empty)
- Line 5: Header row with sensor names like 'MXmini.Spannungsversorgung...'
- Line 6: Units row (V;mm;m/s;...)
- Line 7+: Data rows

Data format:
- ISO datetime (first column): YYYY-MM-DD HH:MM:SS
- German decimals: comma separator (12,67)
- Semicolon delimited
- 'nan' for missing values

Actual columns discovered (081000388 file):
0: Timestamp
1: MXmini.Spannungsversorgung Vin 4.1 (V) - battery voltage
2: MXmini.Niederschlag 4.1 (mm) - precipitation
3: Arco SDI-12.Windstaerke AVG (m/s) - wind speed avg
4: Arco SDI-12.Windstaerke MAX (m/s)
5: Arco SDI-12.Windstaerke MIN (m/s)
6: Arco SDI-12.Windrichtung (deg) - wind direction
7: Arco SDI-12.Windrichtung (deg)
8: Arco SDI-12.Windrichtung (deg)
9: Lambrecht 1.4.Lufttemperatur (C) - air temp
10: Lambrecht 1.4.Lufttemperatur MAX (C)
11: Lambrecht 1.4.Lufttemperatur MIN (C)
12: Lambrecht 1.4.Relative Feuchte (%) - air humidity
13: Lambrecht 1.4.Relative Feuchte MAX (%)
14: Lambrecht 1.4.Relative Feuchte MIN (%)
15: Lambrecht 1.10.Globalstrahlung (W/m2) - solar radiation
16: Lambrecht 1.10.Globalstrahlung MAX (W/m2)
17: Lambrecht 1.10.Globalstrahlung MIN (W/m2)
18: SMT100.2.1.Bodentemperatur (C) - soil temp
19: SMT100.2.1.Bodenfeuchte (vol%) - soil moisture
20: SMT100.2.1.Permittivitaet
21: Virtual.Bodenfeuchte_korregiert (vol%)
"""
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pipeline.ingest.base import get_connection, bulk_insert, classify_ingest_files, update_ingest_manifest
from config.settings import GREENROOF_DIR


CREATE_SEQ_SQL = 'CREATE SEQUENCE IF NOT EXISTS seq_ingested_kissel_greenroof START 1;'

CREATE_SQL = '''
CREATE TABLE IF NOT EXISTS ingested_kissel_greenroof (
    id BIGINT PRIMARY KEY DEFAULT nextval('seq_ingested_kissel_greenroof'),
    timestamp TIMESTAMP,
    serial_no TEXT,

    -- Radiation measurements (using sr1 for global radiation)
    ir1 DOUBLE,
    sr1 DOUBLE,
    ir2 DOUBLE,
    sr2 DOUBLE,

    -- Temperature & pressure
    temperature DOUBLE,
    air_pressure DOUBLE,

    -- Soil
    soil_moisture DOUBLE,
    soil_temp_1 DOUBLE,
    soil_temp_2 DOUBLE,

    -- Air sensors (pair 1)
    air_humidity_1 DOUBLE,
    air_temp_1 DOUBLE,

    -- Air sensors (pair 2)
    air_humidity_2 DOUBLE,
    air_temp_2 DOUBLE,

    -- Wind
    wind_speed DOUBLE,
    wind_direction DOUBLE,

    -- Source tracking
    file_name TEXT
);
'''

INSERT_COLUMNS = [
    'timestamp', 'serial_no',
    'ir1', 'sr1', 'ir2', 'sr2',
    'temperature', 'air_pressure',
    'soil_moisture', 'soil_temp_1', 'soil_temp_2',
    'air_humidity_1', 'air_temp_1', 'air_humidity_2', 'air_temp_2',
    'wind_speed', 'wind_direction',
    'file_name',
]


def _print_progress(prefix, current, total, width=32):
    """Render a single-line ASCII progress bar in the terminal."""
    if total <= 0:
        return
    ratio = min(max(current / total, 0.0), 1.0)
    filled = int(width * ratio)
    bar = '#' * filled + '-' * (width - filled)
    print(f"\r[{prefix}] |{bar}| {current}/{total} ({ratio * 100:5.1f}%)", end='', flush=True)


def _default_worker_count():
    """Use a conservative worker count for parallel file parsing."""
    return min(8, max(4, (os.cpu_count() or 4)))


def is_kissel_greenroof_file(file_path):
    '''Check if file is Raw/Kissel format (Station Name header + MXmini sensors).

    Both conditions are required: the 2026-onward Empower export also starts
    its first line with "Station Name;..." (same logger-brand convention),
    so that prefix alone is not enough to tell the formats apart. Only the
    legacy Kissel/MXmini export names its sensors "MXmini.*" on line 5.
    '''
    try:
        with open(file_path, 'r', encoding='latin-1', errors='ignore') as f:
            lines = [f.readline() for _ in range(7)]

        starts_with_station_name = lines[0].strip().lower().startswith('station name')
        has_mxmini_sensors = len(lines) >= 6 and 'mxmini' in lines[5].lower()

        return starts_with_station_name and has_mxmini_sensors
    except Exception:
        return False


def extract_serial_from_kissel(file_path):
    '''Extract serial number from line 2 (Serial No;value).'''
    try:
        with open(file_path, 'r', encoding='latin-1', errors='ignore') as f:
            for i, line in enumerate(f):
                if i == 2:  # Serial No line
                    parts = line.strip().split(';')
                    if len(parts) >= 2:
                        return parts[1].strip()
                if i > 3:
                    break
    except Exception:
        pass
    return 'KISSEL_UNKNOWN'


def parse_german_float(value):
    '''Parse German formatted numbers (comma decimal), handle 'nan'.'''
    if value is None:
        return None
    s = str(value).strip()
    if s == '' or s.lower() == 'nan':
        return None
    try:
        # Replace comma with decimal point
        return float(s.replace(' ', '').replace(',', '.'))
    except (ValueError, AttributeError):
        return None


def parse_datetime_iso(date_str):
    '''Parse ISO datetime: YYYY-MM-DD HH:MM:SS (used in Raw/Kissel files).'''
    if not date_str:
        return None
    s = str(date_str).strip()
    if s == '':
        return None

    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y/%m/%d %H:%M:%S'):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _parse_kissel_file(path):
    """Parse one Kissel file. Returns (matched, rows).

    matched=False means the file's content didn't sniff as Kissel format at
    all (e.g. it's actually an Empower file) — such files are deliberately
    kept out of this table's ingest manifest so they keep getting cheaply
    re-sniffed every run rather than being permanently (mis)marked "seen" by
    the wrong parser.
    """
    file = os.path.basename(path)

    if not is_kissel_greenroof_file(path):
        return False, []

    with open(path, 'r', encoding='latin-1', errors='ignore') as f:
        lines = f.readlines()

    if len(lines) < 8:
        return True, []

    serial_no = extract_serial_from_kissel(path)
    rows_to_insert = []

    for line in lines[7:]:
        line = line.strip()
        if not line:
            continue

        row = [cell.strip() for cell in line.split(';')]
        if len(row) < 10:
            continue

        timestamp = parse_datetime_iso(row[0])
        if timestamp is None:
            continue

        rows_to_insert.append((
            timestamp,
            serial_no,
            None,
            parse_german_float(row[15]) if len(row) > 15 else None,
            None,
            None,
            None,
            None,
            parse_german_float(row[19]) if len(row) > 19 else None,
            parse_german_float(row[18]) if len(row) > 18 else None,
            None,
            parse_german_float(row[12]) if len(row) > 12 else None,
            parse_german_float(row[9]) if len(row) > 9 else None,
            None,
            None,
            parse_german_float(row[3]) if len(row) > 3 else None,
            parse_german_float(row[6]) if len(row) > 6 else None,
            file,
        ))

    return True, rows_to_insert


def ingest_kissel_greenroof():
    '''Ingest Raw/Kissel (MXmini) greenroof CSVs using positional column mapping.

    Incremental: skips any file whose size+mtime already match what was
    recorded at its last ingest. A same-named file with a changed size/mtime
    has its previously-ingested rows deleted and is fully re-parsed.
    '''
    conn = get_connection()
    print('[ingest_kissel_greenroof] Ensuring table exists...')
    conn.execute(CREATE_SEQ_SQL)
    conn.execute(CREATE_SQL)

    if not os.path.isdir(GREENROOF_DIR):
        print(f'[ingest_kissel_greenroof] Directory not found: {GREENROOF_DIR}')
        conn.close()
        return

    # Recursively find all CSV files in all subfolders
    files = []
    for root, dirs, filenames in os.walk(GREENROOF_DIR):
        for f in filenames:
            if f.lower().endswith('.csv'):
                files.append(os.path.join(root, f))

    to_process, unchanged = classify_ingest_files(conn, 'ingested_kissel_greenroof', files)
    print(f'[ingest_kissel_greenroof] Found {len(files)} files, '
          f'{len(unchanged)} unchanged (skipped), {len(to_process)} new/changed — processing those.')

    if not to_process:
        conn.close()
        return

    conn.executemany(
        'DELETE FROM ingested_kissel_greenroof WHERE file_name = ?',
        [(file_name,) for _, file_name, _, _ in to_process],
    )

    total_files = len(to_process)
    _print_progress('ingest_kissel_greenroof', 0, total_files)
    all_rows = []
    matched_files = []  # only files that sniffed as Kissel format go in the manifest
    with ThreadPoolExecutor(max_workers=_default_worker_count()) as executor:
        futures = {
            executor.submit(_parse_kissel_file, path): (path, file_name, size, mtime)
            for path, file_name, size, mtime in to_process
        }

        for idx, future in enumerate(as_completed(futures), start=1):
            path, file_name, size, mtime = futures[future]
            try:
                matched, rows_to_insert = future.result()
                if rows_to_insert:
                    all_rows.extend(rows_to_insert)
                if matched:
                    matched_files.append((file_name, size, mtime))
            except Exception as e:
                print(f'[ingest_kissel_greenroof] Error in {file_name}: {e}')
            finally:
                _print_progress('ingest_kissel_greenroof', idx, total_files)

    print()

    imported_total = bulk_insert(conn, 'ingested_kissel_greenroof', INSERT_COLUMNS, all_rows)
    print(f'[ingest_kissel_greenroof] Total: {imported_total} rows ingested.')

    update_ingest_manifest(conn, 'ingested_kissel_greenroof', matched_files)
    conn.close()
