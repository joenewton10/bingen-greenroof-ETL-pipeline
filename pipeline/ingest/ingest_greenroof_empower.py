"""
Ingest Empower-Gruendach greenroof CSV files into the DuckDB pipeline database.
Table: ingested_empower_greenroof

Based on: empower_greenroof_code.py

SIMPLIFIED SCHEMA - Dropped columns:
- Device metadata (device_type, device_name, description, device_number, location_name, location_number)
- Rain/counters (counter_1, rain_abs, counter_diff, rain_rel, count_duration)
- System (battery_voltage, internal_temp, alarm_level, field_strength)
- Import tracking (source_directory, import_timestamp)

KEPT columns (17 total):
- id, timestamp, serial_no
- ir1, sr1, ir2, sr2 (radiation)
- temperature, air_pressure
- soil_moisture, soil_temp_1, soil_temp_2
- air_humidity_1, air_temp_1, air_humidity_2, air_temp_2
- wind_speed, wind_direction
- file_name
"""
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pipeline.ingest.base import get_connection, bulk_insert, classify_ingest_files, update_ingest_manifest
from config.settings import GREENROOF_DIR


CREATE_SEQ_SQL = 'CREATE SEQUENCE IF NOT EXISTS seq_ingested_empower_greenroof START 1;'

CREATE_SQL = '''
CREATE TABLE IF NOT EXISTS ingested_empower_greenroof (
    id BIGINT PRIMARY KEY DEFAULT nextval('seq_ingested_empower_greenroof'),
    timestamp TIMESTAMP,
    serial_no TEXT,

    -- Radiation measurements
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


def is_empower_greenroof_file(file_path):
    '''Verify file is Empower greenroof format (starts with Geraetetyp or
    ONLINEMESSUNG, or is the 2026-onward "Station Name;" export).

    The "Station Name;" prefix alone is ambiguous: the legacy Kissel/MXmini
    export uses the identical prefix (same logger-brand convention). Exclude
    it the same way is_kissel_greenroof_file's own check does — a file is
    only Empower's "Station Name;" format if it does NOT also carry Kissel's
    "MXmini.*" sensor-naming marker on line 6. Without this, every Kissel
    file also matches here.
    '''
    try:
        with open(file_path, 'r', encoding='latin-1', errors='ignore') as f:
            lines = [f.readline() for _ in range(7)]

        first_line = lines[0].strip()
        if first_line.startswith('Geraetetyp') or first_line.startswith('ONLINEMESSUNG'):
            return True

        if first_line.startswith('Station Name;'):
            has_mxmini_sensors = len(lines) >= 6 and 'mxmini' in lines[5].lower()
            return not has_mxmini_sensors

        return False
    except Exception:
        return False


def parse_german_float(value):
    '''Parse German formatted numbers (comma decimal, spaces).'''
    if value is None:
        return None
    s = str(value).strip()
    if s == '' or s.upper() == 'NAN':
        return None
    try:
        return float(s.replace(' ', '').replace(',', '.'))
    except (ValueError, AttributeError):
        return None


def parse_datetime_german(date_str):
    '''Parse German datetime: DD.MM.YY HH:MM:SS or DD.MM.YYYY HH:MM:SS'''
    if not date_str:
        return None
    s = str(date_str).strip()
    if s == '':
        return None
    for fmt in ('%d.%m.%y %H:%M:%S', '%d.%m.%Y %H:%M:%S'):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def parse_datetime_iso(date_str):
    '''Parse ISO datetime used by new Empower greenroof exports.'''
    if not date_str:
        return None
    s = str(date_str).strip()
    if s == '':
        return None
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def extract_serial_number(file_path):
    '''Extract serial number from header lines.'''
    try:
        with open(file_path, 'r', encoding='latin-1', errors='ignore') as f:
            first_line = f.readline().strip()

            # Online format uses default serial
            if first_line.startswith('ONLINEMESSUNG'):
                return 'A27212'

            # Regular format - find Seriennummer line
            f.seek(0)
            for _ in range(16):
                line = f.readline()
                if not line:
                    break
                if ';' in line:
                    parts = line.split(';')
                    if len(parts) >= 2 and 'Seriennummer' in parts[0]:
                        return parts[1].strip()
                    if len(parts) >= 2 and parts[0].strip() == 'Serial No':
                        return parts[1].strip()
    except Exception:
        pass
    return None


def _parse_new_empower_file(lines, serial_no, file):
    '''Parse new 2026 Empower greenroof station export format.'''
    rows_to_insert = []

    for line in lines:
        if not line.strip():
            continue

        row = [cell.strip() for cell in line.strip().split(';')]
        if len(row) < 26:
            continue

        timestamp = parse_datetime_iso(row[0])
        if timestamp is None:
            continue

        rows_to_insert.append((
            timestamp,
            serial_no,
            parse_german_float(row[24]),  # ir1 <- IR sky [W/m2]
            parse_german_float(row[22]),  # sr1 <- SR sky [W/m2]
            parse_german_float(row[25]),  # ir2 <- IR ground [W/m2]
            parse_german_float(row[23]),  # sr2 <- SR ground [W/m2]
            parse_german_float(row[12]),  # temperature <- PyrTemp [C]
            None,                         # air_pressure unavailable in new format
            None,                         # soil_moisture unavailable in new format
            parse_german_float(row[13]),  # soil_temp_1 <- BodenTemp 1cm
            parse_german_float(row[14]),  # soil_temp_2 <- BodenTemp 6cm
            # NOTE: this logger's raw channel numbering is inverted relative to
            # every other station/format in the pipeline (old Kissel, old
            # Empower, parkplatz): "oben" (upper, 2.0m) is its channel 1 and
            # "unten" (lower, 0.5m) is its channel 2. Swapped here so that
            # air_temp_1/air_humidity_1 consistently mean the 0.5m level and
            # air_temp_2/air_humidity_2 consistently mean the 2.0m level.
            parse_german_float(row[19]),  # air_humidity_1 <- RH unten (0.5m)
            parse_german_float(row[18]),  # air_temp_1 <- LuftTemp unten (0.5m)
            parse_german_float(row[17]),  # air_humidity_2 <- RH oben (2.0m)
            parse_german_float(row[16]),  # air_temp_2 <- LuftTemp oben (2.0m)
            parse_german_float(row[20]),  # wind_speed <- Windgeschwindigkeit
            parse_german_float(row[21]),  # wind_direction <- Windrichtung
            file,
        ))

    return rows_to_insert


def _parse_old_empower_file(lines, serial_no, file):
    '''Parse legacy LT6-digi-GPRS Empower greenroof export format.'''
    header_line_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith('Nr.;Datum') or ('Nr.;' in line and 'Datum' in line):
            header_line_idx = i
            break

    if header_line_idx is None:
        return []

    rows_to_insert = []
    data_start = header_line_idx + 1
    for line in lines[data_start:]:
        if not line.strip():
            continue

        row = [cell.strip() for cell in line.strip().split(';')]
        if len(row) < 25 or not row[0].isdigit():
            continue

        timestamp = parse_datetime_german(row[1]) if len(row) > 1 else None
        if timestamp is None:
            continue

        rows_to_insert.append((
            timestamp,
            serial_no,
            parse_german_float(row[2]) if len(row) > 2 else None,
            parse_german_float(row[3]) if len(row) > 3 else None,
            parse_german_float(row[4]) if len(row) > 4 else None,
            parse_german_float(row[5]) if len(row) > 5 else None,
            parse_german_float(row[6]) if len(row) > 6 else None,
            parse_german_float(row[7]) if len(row) > 7 else None,
            parse_german_float(row[8]) if len(row) > 8 else None,
            parse_german_float(row[15]) if len(row) > 15 else None,
            parse_german_float(row[16]) if len(row) > 16 else None,
            parse_german_float(row[9]) if len(row) > 9 else None,
            parse_german_float(row[10]) if len(row) > 10 else None,
            parse_german_float(row[11]) if len(row) > 11 else None,
            parse_german_float(row[12]) if len(row) > 12 else None,
            parse_german_float(row[13]) if len(row) > 13 else None,
            parse_german_float(row[14]) if len(row) > 14 else None,
            file,
        ))

    return rows_to_insert


def _parse_empower_file(path):
    """Parse one Empower file. Returns (matched, rows).

    matched=False means the file's content didn't sniff as Empower format at
    all (e.g. it's actually a Kissel file) — such files are deliberately kept
    out of this table's ingest manifest so they keep getting cheaply
    re-sniffed every run rather than being permanently (mis)marked "seen" by
    the wrong parser.
    """
    file = os.path.basename(path)

    if not is_empower_greenroof_file(path):
        return False, []

    with open(path, 'r', encoding='latin-1', errors='ignore') as f:
        lines = f.readlines()

    if not lines:
        return True, []

    # No blanket line-count minimum: the new format only needs its 7 header
    # rows plus 1 data row (8 total) to be valid, and a short logger
    # re-export with just a couple of rows is common and legitimate — e.g.
    # 081000387_20260127_095311.csv (9 lines) has real data that a flat "< 18
    # lines" cutoff used to silently drop. Both _parse_new_empower_file and
    # _parse_old_empower_file already validate structure per-row (and, for
    # the old format, only proceed if a header line is actually found), so
    # they handle short or malformed content correctly without this guard.
    serial_no = extract_serial_number(path)

    if lines[0].startswith('Station Name;'):
        return True, _parse_new_empower_file(lines, serial_no, file)

    return True, _parse_old_empower_file(lines, serial_no, file)


def ingest_empower_greenroof():
    '''Ingest Empower-Gruendach CSVs using positional column mapping.

    Incremental: skips any file whose size+mtime already match what was
    recorded at its last ingest. A same-named file with a changed size/mtime
    has its previously-ingested rows deleted and is fully re-parsed.
    '''
    conn = get_connection()
    print('[ingest_empower_greenroof] Ensuring table exists...')
    conn.execute(CREATE_SEQ_SQL)
    conn.execute(CREATE_SQL)

    if not os.path.isdir(GREENROOF_DIR):
        print(f'[ingest_empower_greenroof] Directory not found: {GREENROOF_DIR}')
        conn.close()
        return

    # Recursively find all CSV files in all subfolders
    files = []
    for root, dirs, filenames in os.walk(GREENROOF_DIR):
        for f in filenames:
            if f.lower().endswith('.csv'):
                files.append(os.path.join(root, f))

    to_process, unchanged = classify_ingest_files(conn, 'ingested_empower_greenroof', files)
    print(f'[ingest_empower_greenroof] Found {len(files)} files, '
          f'{len(unchanged)} unchanged (skipped), {len(to_process)} new/changed — processing those.')

    if not to_process:
        conn.close()
        return

    # A "changed" file (same name, different size/mtime) may already have
    # stale rows from its previous version — clear them before reinserting.
    # No-op for a genuinely new filename.
    conn.executemany(
        'DELETE FROM ingested_empower_greenroof WHERE file_name = ?',
        [(file_name,) for _, file_name, _, _ in to_process],
    )

    total_files = len(to_process)
    _print_progress('ingest_empower_greenroof', 0, total_files)
    all_rows = []
    matched_files = []  # only files that sniffed as Empower format go in the manifest
    with ThreadPoolExecutor(max_workers=_default_worker_count()) as executor:
        futures = {
            executor.submit(_parse_empower_file, path): (path, file_name, size, mtime)
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
                print(f'[ingest_empower_greenroof] Error in {file_name}: {e}')
            finally:
                _print_progress('ingest_empower_greenroof', idx, total_files)

    print()

    imported_total = bulk_insert(conn, 'ingested_empower_greenroof', INSERT_COLUMNS, all_rows)
    print(f'[ingest_empower_greenroof] Total: {imported_total} rows ingested.')

    update_ingest_manifest(conn, 'ingested_empower_greenroof', matched_files)
    conn.close()
