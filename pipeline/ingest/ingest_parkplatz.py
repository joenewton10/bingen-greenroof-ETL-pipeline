"""
Ingest Parkplatz CSV files into ingested_parkplatz table.
Based on: parkplatz_code_combine.py

Table naming convention for Bingen pipeline:
- Output: ingested_parkplatz

Handles BOTH Empower and Kissel Parkplatz formats:
- Empower: 27 columns, clean German decimals
- Kissel: 54 columns with Min/Max, corrupted numeric format (-83,511,000)

Key features ported from parkplatz_code_combine.py:
- find_data_start() - finds semicolon divider or Nr.;Datum header
- convert_column_numeric() - handles corrupt comma format (first comma = decimal)
- normalize_scales() - detects 1000x multiplication issues
- Header translation from German to English
- Min/Max column support
"""
import os
import re
import csv
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd
import numpy as np
from pipeline.ingest.base import get_connection, bulk_insert, classify_ingest_files, update_ingest_manifest
from config.settings import PARKPLATZ_DIR


# --- FINAL COLUMN SET ---
FINAL_COLS = [
    'No.', 'Date / Time', 'IR1 [W/m2]', 'IR1 Min [W/m2]', 'IR1 Max [W/m2]',
    'SR1 [W/m2]', 'SR1 Min [W/m2]', 'SR1 Max [W/m2]', 'IR2 [W/m2]', 'IR2 Min [W/m2]', 'IR2 Max [W/m2]',
    'SR2 [W/m2]', 'SR2 Min [W/m2]', 'SR2 Max [W/m2]', 'Temp [C]', 'Temp Min [C]', 'Temp Max [C]',
    'Air Pressure [mbar]', 'Air Pressure Min [mbar]', 'Air Pressure Max [mbar]',
    'Soil Moisture [vol%]', 'Soil Moisture Min [vol%]', 'Soil Moisture Max [vol%]',
    'Air Humidity 1 [%RH]', 'Air Humidity 1 Min [%RH]', 'Air Humidity 1 Max [%RH]',
    'Air Temp 1 [C]', 'Air Temp 1 Min [C]', 'Air Temp 1 Max [C]',
    'Air Humidity 2 [%RH]', 'Air Humidity 2 Min [%RH]', 'Air Humidity 2 Max [%RH]',
    'Air Temp 2 [C]', 'Air Temp 2 Min [C]', 'Air Temp 2 Max [C]',
    'Wind Speed [m/s]', 'Wind Speed Min [m/s]', 'Wind Speed Max [m/s]',
    'Wind Direction [deg]', 'Wind Direction Min [deg]', 'Wind Direction Max [deg]',
    'Soil Temp 1 [C]', 'Soil Temp 1 Min [C]', 'Soil Temp 1 Max [C]',
    'Soil Temp 2 [C]', 'Soil Temp 2 Min [C]', 'Soil Temp 2 Max [C]',
    'Counter 1 [Impulses]', 'Rain Abs [mm]', 'Counter 1 Diff [Impulses]', 'Rain Rel [mm]',
    'Duration Counting', 'Battery Voltage [V]', 'Internal Temp [C]', 'Alarm Level', 'Field Strength 1',
    'Serial Number', 'source_file'
]


CREATE_SEQ_SQL = 'CREATE SEQUENCE IF NOT EXISTS seq_ingested_parkplatz START 1;'

CREATE_SQL = '''
CREATE TABLE IF NOT EXISTS ingested_parkplatz (
    id BIGINT PRIMARY KEY DEFAULT nextval('seq_ingested_parkplatz'),
    timestamp TIMESTAMP,
    record_number TEXT,

    -- Radiation (with min/max)
    ir1_wm2 DOUBLE, ir1_min_wm2 DOUBLE, ir1_max_wm2 DOUBLE,
    sr1_wm2 DOUBLE, sr1_min_wm2 DOUBLE, sr1_max_wm2 DOUBLE,
    ir2_wm2 DOUBLE, ir2_min_wm2 DOUBLE, ir2_max_wm2 DOUBLE,
    sr2_wm2 DOUBLE, sr2_min_wm2 DOUBLE, sr2_max_wm2 DOUBLE,

    -- Temperature & Pressure (with min/max)
    temp_c DOUBLE, temp_min_c DOUBLE, temp_max_c DOUBLE,
    air_pressure_mbar DOUBLE, air_pressure_min_mbar DOUBLE, air_pressure_max_mbar DOUBLE,

    -- Soil Moisture (with min/max)
    soil_moisture_vol DOUBLE, soil_moisture_min_vol DOUBLE, soil_moisture_max_vol DOUBLE,

    -- Air Humidity/Temp Sensor 1 (with min/max)
    air_humidity_1_rh DOUBLE, air_humidity_1_min_rh DOUBLE, air_humidity_1_max_rh DOUBLE,
    air_temp_1_c DOUBLE, air_temp_1_min_c DOUBLE, air_temp_1_max_c DOUBLE,

    -- Air Humidity/Temp Sensor 2 (with min/max)
    air_humidity_2_rh DOUBLE, air_humidity_2_min_rh DOUBLE, air_humidity_2_max_rh DOUBLE,
    air_temp_2_c DOUBLE, air_temp_2_min_c DOUBLE, air_temp_2_max_c DOUBLE,

    -- Wind (with min/max)
    wind_speed_ms DOUBLE, wind_speed_min_ms DOUBLE, wind_speed_max_ms DOUBLE,
    wind_direction_deg DOUBLE, wind_direction_min_deg DOUBLE, wind_direction_max_deg DOUBLE,

    -- Soil Temperature (with min/max)
    soil_temp_1_c DOUBLE, soil_temp_1_min_c DOUBLE, soil_temp_1_max_c DOUBLE,
    soil_temp_2_c DOUBLE, soil_temp_2_min_c DOUBLE, soil_temp_2_max_c DOUBLE,

    -- Counters & Rain (no min/max)
    counter_1_impulses DOUBLE,
    rain_abs_mm DOUBLE,
    counter_diff_impulses DOUBLE,
    rain_rel_mm DOUBLE,
    duration_counting DOUBLE,

    -- System
    battery_voltage_v DOUBLE,
    internal_temp_c DOUBLE,
    alarm_level DOUBLE,
    field_strength_1 DOUBLE,
    serial_number TEXT,

    -- Import tracking
    filename TEXT,
    import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
'''


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


def _read_text_lines(filepath):
    """Read a CSV file once using utf-8 or latin-1 fallback."""
    try:
        with open(filepath, encoding='utf-8') as f:
            return f.readlines()
    except UnicodeDecodeError:
        with open(filepath, encoding='latin-1') as f:
            return f.readlines()


def _find_data_start_in_lines(lines):
    """Find line number where data starts using already-loaded lines."""
    def is_semicolon_line(line):
        return bool(re.match(r'^\s*;{5,}\s*$', line))

    for i, line in enumerate(lines):
        if is_semicolon_line(line):
            return i + 2
        if re.search(r'\bNr\.\s*;.*\bDatum\b', line, flags=re.IGNORECASE) or line.strip().startswith('Nr.;'):
            return i + 1

    for i, line in enumerate(lines):
        if line.strip() == '':
            continue
        if 'Nr' in line or ';' in line:
            return i + 1
    return None


def clean_colname(name):
    '''Clean column name for comparison/mapping.'''
    return re.sub(r'[\s/\[\]\(\)\.,:;"\'-]', '_', name)


def find_data_start(filepath):
    '''Find line number where data starts.'''
    return _find_data_start_in_lines(_read_text_lines(filepath))


def extract_name_unit(h):
    '''Extract name and unit from header like "IR1 [W/m2] = m1 /E1/800016"
    Preserves "= Min" and "= Max" suffixes for 2020 format headers.
    '''
    h = h.strip().strip('"')

    # Check for Min/Max suffix before the unit bracket
    min_max_suffix = ''
    if ' = Min' in h:
        min_max_suffix = ' Min'
        h = h.replace(' = Min', '')
    elif ' = Max' in h:
        min_max_suffix = ' Max'
        h = h.replace(' = Max', '')

    match = re.match(r'^([^\[]+)\[([^\]]+)\]', h)
    if match:
        name = match.group(1).strip()
        unit = match.group(2).strip()
        return f"{name}{min_max_suffix} [{unit}]"
    return h.split('[')[0].split('=')[0].strip()


def convert_column_numeric(series):
    '''
    Vectorized numeric conversion.
        - Any comma-formatted value uses first comma as decimal separator.
            Examples: "42,406,000" -> 42.406, "-120,199,000.0" -> -120.199
        - Single comma values (e.g. "1,5") remain standard decimal comma.
    '''
    s = series.fillna('').astype(str).str.strip()
    s = s.str.replace('\xa0', '', regex=False).str.replace(' ', '', regex=False)

    has_comma = s.str.contains(',', regex=False)
    if has_comma.any():
        # Use first comma as decimal separator and strip the rest.
        # This handles corrupted forms like "6,333,000,000" -> 6.333 and "1,44,0,000" -> 1.44.
        parts = s[has_comma].str.split(',', n=1)
        left = parts.str[0].fillna('')
        right = parts.str[1].fillna('')
        right = right.str.replace(',', '', regex=False).str.replace('.', '', regex=False)
        s = s.copy()
        s.loc[has_comma] = left + '.' + right

    s = s.str.replace(r'[^0-9\.\-]', '', regex=True)

    multi_dot_mask = s.str.count(r'\.') > 1
    if multi_dot_mask.any():
        def collapse_dots(x):
            if '.' not in x:
                return x
            parts = x.split('.', 1)
            return parts[0] + '.' + parts[1].replace('.', '')
        s.loc[multi_dot_mask] = s.loc[multi_dot_mask].apply(collapse_dots)

    return pd.to_numeric(s.replace('', pd.NA), errors='coerce')


def convert_scalar_numeric(value):
    '''Convert a single scalar to float using the same cleanup logic as column conversion.'''
    if value is None:
        return None

    s = str(value).strip()
    if s == '':
        return None

    s = s.replace('\xa0', '').replace(' ', '')

    if ',' in s:
        # Use first comma as decimal separator and strip the rest.
        left, right = s.split(',', 1)
        right = right.replace(',', '').replace('.', '')
        s = left + '.' + right

    s = re.sub(r'[^0-9\.\-]', '', s)

    if s.count('.') > 1:
        first, rest = s.split('.', 1)
        s = first + '.' + rest.replace('.', '')

    if s in ('', '-', '.', '-.'):
        return None

    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def normalize_scales(df):
    '''Detect and fix scale differences (1000x multiplication issues).'''
    if df is None or df.empty:
        return df

    exclude_kw = ('Impulse', 'Counter', 'Rain', 'Zaehler', 'Regen', 'source_file', 'Serial', 'No.', 'Date')
    for col in df.columns:
        if col in ('source_file', 'Serial Number', 'No.', 'Date / Time'):
            continue
        if not pd.api.types.is_numeric_dtype(df[col]):
            continue
        if any(kw.lower() in col.lower() for kw in exclude_kw):
            continue

        med = df[col].abs().median(skipna=True)
        if pd.isna(med):
            continue

        if 1000.0 < med < 1e9:
            med_after = med / 1000.0
            if med_after < 1000.0:
                print(f"  [normalize_scales] Dividing '{col}' by 1000 (median {med:.1f} -> {med_after:.3f})")
                df[col] = df[col] / 1000.0

    return df


def normalize_variant_header(col):
    '''Normalize header variants (RH-unten -> Luftfeu-1, etc.)'''
    main = col.split('=')[0].strip()
    l = main.lower()

    if ('rh' in l and 'unten' in l) or 'rh-unten' in l or 'rh unten' in l:
        return 'Luftfeu-1 [%RH]'
    if ('lt' in l and 'unten' in l) or 'lt-unten' in l or 'lt unten' in l:
        return 'Lufttemp-1 [degC]'
    if ('rh' in l and 'oben' in l) or 'rh-oben' in l or 'rh oben' in l:
        return 'Luftfeu-2 [%RH]'
    if ('lt' in l and 'oben' in l) or 'lt-oben' in l or 'lt oben' in l:
        return 'Lufttemp-2 [degC]'

    # Handle typo in older files: "Luftemp-1" instead of "Lufttemp-1"
    if 'luftemp-1' in l or 'luftemp 1' in l:
        return 'Lufttemp-1 [degC]'
    if 'luftemp-2' in l or 'luftemp 2' in l:
        return 'Lufttemp-2 [degC]'

    return main


def parse_any_date(val):
    '''Parse various date formats found in Parkplatz files.'''
    if pd.isna(val):
        return None
    val = str(val).strip()
    if not val:
        return None

    m1 = re.match(r'^(\d{2})(\d{2})(\d{4})[\sT](\d{2}):(\d{2})(?::(\d{2}))?$', val)
    if m1:
        day, month, year, hour, minute, second = m1.groups()
        second = second or '00'
        return f"{year}-{month}-{day} {hour}:{minute}:{second}"

    m2 = re.match(r'^(\d{2})\.(\d{2})\.(\d{2,4})[\sT](\d{2}):(\d{2})(?::(\d{2}))?$', val)
    if m2:
        day, month, year, hour, minute, second = m2.groups()
        if len(year) == 2:
            year = '20' + year if int(year) < 50 else '19' + year
        second = second or '00'
        return f"{year}-{month}-{day} {hour}:{minute}:{second}"

    m3 = re.match(r'^(\d{4})-(\d{2})-(\d{2})[\sT](\d{2}):(\d{2}):(\d{2})$', val)
    if m3:
        return val.replace('T', ' ')

    try:
        dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
        if pd.isna(dt):
            return None
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return None


# Header translation map (German -> English)
HEADER_MAP = {
    clean_colname('Nr.'): 'No.',
    clean_colname('Datum / Uhrzeit'): 'Date / Time',
    # IR1 - both numbered suffix format (2024) and explicit Min/Max (2020)
    clean_colname('IR1 [W/m2]'): 'IR1 [W/m2]',
    clean_colname('IR1 [W/m2]_1'): 'IR1 Min [W/m2]',
    clean_colname('IR1 [W/m2]_2'): 'IR1 Max [W/m2]',
    clean_colname('IR1 Min [W/m2]'): 'IR1 Min [W/m2]',
    clean_colname('IR1 Max [W/m2]'): 'IR1 Max [W/m2]',
    # SR1
    clean_colname('SR1 [W/m2]'): 'SR1 [W/m2]',
    clean_colname('SR1 [W/m2]_1'): 'SR1 Min [W/m2]',
    clean_colname('SR1 [W/m2]_2'): 'SR1 Max [W/m2]',
    clean_colname('SR1 Min [W/m2]'): 'SR1 Min [W/m2]',
    clean_colname('SR1 Max [W/m2]'): 'SR1 Max [W/m2]',
    # IR2
    clean_colname('IR2 [W/m2]'): 'IR2 [W/m2]',
    clean_colname('IR2 [W/m2]_1'): 'IR2 Min [W/m2]',
    clean_colname('IR2 [W/m2]_2'): 'IR2 Max [W/m2]',
    clean_colname('IR2 Min [W/m2]'): 'IR2 Min [W/m2]',
    clean_colname('IR2 Max [W/m2]'): 'IR2 Max [W/m2]',
    clean_colname('SR2 [W/m2]'): 'SR2 [W/m2]',
    clean_colname('SR2 [W/m2]_1'): 'SR2 Min [W/m2]',
    clean_colname('SR2 [W/m2]_2'): 'SR2 Max [W/m2]',
    clean_colname('SR2 Min [W/m2]'): 'SR2 Min [W/m2]',
    clean_colname('SR2 Max [W/m2]'): 'SR2 Max [W/m2]',
    # Temperature
    clean_colname('Temp [degC]'): 'Temp [C]',
    clean_colname('Temp [degC]_1'): 'Temp Min [C]',
    clean_colname('Temp [degC]_2'): 'Temp Max [C]',
    clean_colname('Temp Min [degC]'): 'Temp Min [C]',
    clean_colname('Temp Max [degC]'): 'Temp Max [C]',
    # Air Pressure
    clean_colname('Luftdruck [mbar]'): 'Air Pressure [mbar]',
    clean_colname('Luftdruck [mbar]_1'): 'Air Pressure Min [mbar]',
    clean_colname('Luftdruck [mbar]_2'): 'Air Pressure Max [mbar]',
    clean_colname('Luftdruck Min [mbar]'): 'Air Pressure Min [mbar]',
    clean_colname('Luftdruck Max [mbar]'): 'Air Pressure Max [mbar]',
    # Soil Moisture
    clean_colname('Bodenfeucht [vol%]'): 'Soil Moisture [vol%]',
    clean_colname('Bodenfeucht [vol%]_1'): 'Soil Moisture Min [vol%]',
    clean_colname('Bodenfeucht [vol%]_2'): 'Soil Moisture Max [vol%]',
    clean_colname('Bodenfeucht Min [vol%]'): 'Soil Moisture Min [vol%]',
    clean_colname('Bodenfeucht Max [vol%]'): 'Soil Moisture Max [vol%]',
    # Air Humidity 1
    clean_colname('Luftfeu-1 [%RH]'): 'Air Humidity 1 [%RH]',
    clean_colname('Luftfeu-1 [%RH]_1'): 'Air Humidity 1 Min [%RH]',
    clean_colname('Luftfeu-1 [%RH]_2'): 'Air Humidity 1 Max [%RH]',
    clean_colname('Luftfeu-1 Min [%RH]'): 'Air Humidity 1 Min [%RH]',
    clean_colname('Luftfeu-1 Max [%RH]'): 'Air Humidity 1 Max [%RH]',
    # Air Temp 1 (note: header says Luftemp-1 not Lufttemp-1)
    clean_colname('Lufttemp-1 [degC]'): 'Air Temp 1 [C]',
    clean_colname('Lufttemp-1 [degC]_1'): 'Air Temp 1 Min [C]',
    clean_colname('Lufttemp-1 [degC]_2'): 'Air Temp 1 Max [C]',
    clean_colname('Luftemp-1 [degC]'): 'Air Temp 1 [C]',
    clean_colname('Luftemp-1 Min [degC]'): 'Air Temp 1 Min [C]',
    clean_colname('Luftemp-1 Max [degC]'): 'Air Temp 1 Max [C]',
    # Air Humidity 2
    clean_colname('Luftfeu-2 [%RH]'): 'Air Humidity 2 [%RH]',
    clean_colname('Luftfeu-2 [%RH]_1'): 'Air Humidity 2 Min [%RH]',
    clean_colname('Luftfeu-2 [%RH]_2'): 'Air Humidity 2 Max [%RH]',
    clean_colname('Luftfeu-2 Min [%RH]'): 'Air Humidity 2 Min [%RH]',
    clean_colname('Luftfeu-2 Max [%RH]'): 'Air Humidity 2 Max [%RH]',
    # Air Temp 2
    clean_colname('Lufttemp-2 [degC]'): 'Air Temp 2 [C]',
    clean_colname('Lufttemp-2 [degC]_1'): 'Air Temp 2 Min [C]',
    clean_colname('Lufttemp-2 [degC]_2'): 'Air Temp 2 Max [C]',
    clean_colname('Lufttemp-2 Min [degC]'): 'Air Temp 2 Min [C]',
    clean_colname('Lufttemp-2 Max [degC]'): 'Air Temp 2 Max [C]',
    # Wind Speed
    clean_colname('Windgeschw [m/s]'): 'Wind Speed [m/s]',
    clean_colname('Windgeschw [m/s]_1'): 'Wind Speed Min [m/s]',
    clean_colname('Windgeschw [m/s]_2'): 'Wind Speed Max [m/s]',
    clean_colname('Windgeschw Min [m/s]'): 'Wind Speed Min [m/s]',
    clean_colname('Windgeschw Max [m/s]'): 'Wind Speed Max [m/s]',
    # Wind Direction
    clean_colname('Windricht [grad]'): 'Wind Direction [deg]',
    clean_colname('Windricht [grad]_1'): 'Wind Direction Min [deg]',
    clean_colname('Windricht [grad]_2'): 'Wind Direction Max [deg]',
    clean_colname('Windricht Min [grad]'): 'Wind Direction Min [deg]',
    clean_colname('Windricht Max [grad]'): 'Wind Direction Max [deg]',
    # Soil Temp 1
    clean_colname('Bodentemp-1 [degC]'): 'Soil Temp 1 [C]',
    clean_colname('Bodentemp-1 [degC]_1'): 'Soil Temp 1 Min [C]',
    clean_colname('Bodentemp-1 [degC]_2'): 'Soil Temp 1 Max [C]',
    clean_colname('Bodentemp-1 Min [degC]'): 'Soil Temp 1 Min [C]',
    clean_colname('Bodentemp-1 Max [degC]'): 'Soil Temp 1 Max [C]',
    # Soil Temp 2
    clean_colname('Bodentemp-2 [degC]'): 'Soil Temp 2 [C]',
    clean_colname('Bodentemp-2 [degC]_1'): 'Soil Temp 2 Min [C]',
    clean_colname('Bodentemp-2 [degC]_2'): 'Soil Temp 2 Max [C]',
    clean_colname('Bodentemp-2 Min [degC]'): 'Soil Temp 2 Min [C]',
    clean_colname('Bodentemp-2 Max [degC]'): 'Soil Temp 2 Max [C]',
    # Counters and misc
    clean_colname('Zaehler1 [Impulse]'): 'Counter 1 [Impulses]',
    clean_colname('Regen-abs [mm]'): 'Rain Abs [mm]',
    clean_colname('Zaehler1-Differenz [Impulse]'): 'Counter 1 Diff [Impulses]',
    clean_colname('Regen-rel [mm]'): 'Rain Rel [mm]',
    clean_colname('Dauer Zahlvorgang'): 'Duration Counting',
    clean_colname('Dauer Zählvorgang'): 'Duration Counting',  # with umlaut
    clean_colname('Vbatt [V]'): 'Battery Voltage [V]',
    clean_colname('Tintern [degC]'): 'Internal Temp [C]',
    clean_colname('Alarmstufe'): 'Alarm Level',
    clean_colname('Feldstarke 1'): 'Field Strength 1',
    clean_colname('Feldstärke 1'): 'Field Strength 1',  # with umlaut
    clean_colname('Seriennummer'): 'Serial Number',
}


DB_COL_MAP = {
    'No.': 'record_number',
    'Date / Time': 'timestamp',
    'IR1 [W/m2]': 'ir1_wm2', 'IR1 Min [W/m2]': 'ir1_min_wm2', 'IR1 Max [W/m2]': 'ir1_max_wm2',
    'SR1 [W/m2]': 'sr1_wm2', 'SR1 Min [W/m2]': 'sr1_min_wm2', 'SR1 Max [W/m2]': 'sr1_max_wm2',
    'IR2 [W/m2]': 'ir2_wm2', 'IR2 Min [W/m2]': 'ir2_min_wm2', 'IR2 Max [W/m2]': 'ir2_max_wm2',
    'SR2 [W/m2]': 'sr2_wm2', 'SR2 Min [W/m2]': 'sr2_min_wm2', 'SR2 Max [W/m2]': 'sr2_max_wm2',
    'Temp [C]': 'temp_c', 'Temp Min [C]': 'temp_min_c', 'Temp Max [C]': 'temp_max_c',
    'Air Pressure [mbar]': 'air_pressure_mbar', 'Air Pressure Min [mbar]': 'air_pressure_min_mbar', 'Air Pressure Max [mbar]': 'air_pressure_max_mbar',
    'Soil Moisture [vol%]': 'soil_moisture_vol', 'Soil Moisture Min [vol%]': 'soil_moisture_min_vol', 'Soil Moisture Max [vol%]': 'soil_moisture_max_vol',
    'Air Humidity 1 [%RH]': 'air_humidity_1_rh', 'Air Humidity 1 Min [%RH]': 'air_humidity_1_min_rh', 'Air Humidity 1 Max [%RH]': 'air_humidity_1_max_rh',
    'Air Temp 1 [C]': 'air_temp_1_c', 'Air Temp 1 Min [C]': 'air_temp_1_min_c', 'Air Temp 1 Max [C]': 'air_temp_1_max_c',
    'Air Humidity 2 [%RH]': 'air_humidity_2_rh', 'Air Humidity 2 Min [%RH]': 'air_humidity_2_min_rh', 'Air Humidity 2 Max [%RH]': 'air_humidity_2_max_rh',
    'Air Temp 2 [C]': 'air_temp_2_c', 'Air Temp 2 Min [C]': 'air_temp_2_min_c', 'Air Temp 2 Max [C]': 'air_temp_2_max_c',
    'Wind Speed [m/s]': 'wind_speed_ms', 'Wind Speed Min [m/s]': 'wind_speed_min_ms', 'Wind Speed Max [m/s]': 'wind_speed_max_ms',
    'Wind Direction [deg]': 'wind_direction_deg', 'Wind Direction Min [deg]': 'wind_direction_min_deg', 'Wind Direction Max [deg]': 'wind_direction_max_deg',
    'Soil Temp 1 [C]': 'soil_temp_1_c', 'Soil Temp 1 Min [C]': 'soil_temp_1_min_c', 'Soil Temp 1 Max [C]': 'soil_temp_1_max_c',
    'Soil Temp 2 [C]': 'soil_temp_2_c', 'Soil Temp 2 Min [C]': 'soil_temp_2_min_c', 'Soil Temp 2 Max [C]': 'soil_temp_2_max_c',
    'Counter 1 [Impulses]': 'counter_1_impulses',
    'Rain Abs [mm]': 'rain_abs_mm',
    'Counter 1 Diff [Impulses]': 'counter_diff_impulses',
    'Rain Rel [mm]': 'rain_rel_mm',
    'Duration Counting': 'duration_counting',
    'Battery Voltage [V]': 'battery_voltage_v',
    'Internal Temp [C]': 'internal_temp_c',
    'Alarm Level': 'alarm_level',
    'Field Strength 1': 'field_strength_1',
    'Serial Number': 'serial_number',
    'source_file': 'filename',
}


def read_csv_clean(filepath):
    '''Read and clean a Parkplatz CSV file.'''
    lines = _read_text_lines(filepath)
    data_start = _find_data_start_in_lines(lines)
    if data_start is None:
        raise ValueError(f"Could not find data start in {filepath}")

    header_line = lines[data_start - 1].strip()

    # Handle quoted header line (2020 Kissel format - entire line wrapped in quotes)
    if header_line.startswith('"'):
        # Remove outer quotes and unescape internal doubled quotes
        # The line is wrapped in quotes with "" used for escaping inside
        header_line = header_line[1:]  # Remove leading quote
        if header_line.endswith('"'):
            header_line = header_line[:-1]  # Remove trailing quote
        header_line = header_line.replace('""', '"')  # Unescape doubled quotes

    header_raw = [extract_name_unit(h) for h in header_line.split(';')]

    while header_raw and (header_raw[-1] == '' or header_raw[-1] is None):
        header_raw.pop()
    header = [h for h in header_raw if h]

    # Make unique column names
    counts = {}
    unique_header = []
    for n in header:
        if n not in counts:
            counts[n] = 0
            unique_header.append(n)
        else:
            counts[n] += 1
            unique_header.append(f"{n}_{counts[n]}")
    header = unique_header

    # Read data rows
    raw_rows = []
    reader = csv.reader(lines[data_start:], delimiter=';')
    for row in reader:
        while row and (row[-1] == '' or row[-1] is None):
            row.pop()
        if not raw_rows and row:
            row[0] = row[0].lstrip('﻿')
        if len(row) != len(header):
            if len(row) < len(header):
                row = row + [''] * (len(header) - len(row))
            else:
                row = row[:len(header)]
        raw_rows.append(row)

    if not raw_rows:
        return pd.DataFrame(columns=FINAL_COLS)

    df = pd.DataFrame(raw_rows, columns=header)
    df['source_file'] = os.path.basename(filepath)

    # Parse date column FIRST (before any other processing)
    date_col_candidates = [col for col in header if 'Datum' in col or 'Uhrzeit' in col or 'Date' in col]
    date_col = date_col_candidates[0] if date_col_candidates else header[1]
    parsed_dates = df[date_col].apply(parse_any_date)

    # Now drop the original date column to avoid confusion
    if date_col in df.columns:
        df = df.drop(columns=[date_col])

    # Normalize variant headers
    df.columns = [normalize_variant_header(col) for col in df.columns]

    # Clean column names for mapping
    df.columns = [clean_colname(col) for col in df.columns]

    # Handle Seriennummer columns
    seriennummer_cols = [col for col in df.columns if 'seriennummer' in col.lower()]
    if seriennummer_cols:
        df['Seriennummer'] = df[seriennummer_cols[0]].astype(str).str.strip()
        for col in seriennummer_cols:
            if col != 'Seriennummer' and col in df.columns:
                df = df.drop(columns=[col])

    # Convert numeric columns (EXCLUDING date-related columns)
    exclude_cols = ['source_file', 'Seriennummer', 'Nr_']
    for col in df.columns:
        if any(ex in col for ex in exclude_cols):
            continue
        # Check for both object dtype and StringDtype (pandas extension type)
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            try:
                df[col] = convert_column_numeric(df[col])
            except Exception:
                pass

    # Apply header translation
    df.rename(columns=HEADER_MAP, inplace=True)

    # Apply scale normalization
    df = normalize_scales(df)

    # Handle duplicate columns (2020 format has multiple variants mapping to same column)
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]

    # Align to FINAL_COLS
    df = df.reindex(columns=FINAL_COLS)

    # NOW set the Date / Time column with parsed dates
    df['Date / Time'] = parsed_dates.values

    # Filter out rows without valid dates
    df = df[df['Date / Time'].notna()]

    if not df.empty:
        df['Date / Time'] = pd.to_datetime(df['Date / Time'], errors='coerce')
        df = df[df['Date / Time'].notna()].sort_values('Date / Time', kind='stable')

    return df


_NON_NUMERIC_COLS = {'No.', 'Date / Time', 'Serial Number', 'source_file'}


def _prepare_insert_records(df):
    '''Prepare insert records in FINAL_COL order with minimal Python overhead.'''
    if df is None or df.empty:
        return []

    prepared = df.reindex(columns=FINAL_COLS).copy()
    prepared['Date / Time'] = pd.to_datetime(prepared['Date / Time'], errors='coerce')
    prepared = prepared[prepared['Date / Time'].notna()].sort_values('Date / Time', kind='stable')

    for col in ('No.', 'Serial Number', 'source_file'):
        if col in prepared.columns:
            prepared[col] = prepared[col].map(lambda value: None if pd.isna(value) else str(value).strip())

    # Safety net: ensure no raw strings reach the DB for numeric columns.
    # This catches any column that slipped through convert_column_numeric in read_csv_clean.
    for col in prepared.columns:
        if col not in _NON_NUMERIC_COLS:
            if pd.api.types.is_object_dtype(prepared[col]) or pd.api.types.is_string_dtype(prepared[col]):
                prepared[col] = convert_column_numeric(prepared[col])

    prepared = prepared.where(pd.notna(prepared), None)
    return list(prepared.itertuples(index=False, name=None))


def _insert_records(conn, records):
    '''Insert prepared tuple records into ingested_parkplatz.'''
    if not records:
        return 0

    db_cols = [DB_COL_MAP[col] for col in FINAL_COLS if col in DB_COL_MAP]
    return bulk_insert(conn, 'ingested_parkplatz', db_cols, records)


def _parse_parkplatz_file(filepath):
    '''Parse one Parkplatz CSV and return timestamp metadata plus ordered insert records.'''
    df = read_csv_clean(filepath)
    records = _prepare_insert_records(df)
    if not records:
        return filepath, None, records
    return filepath, records[0][1], records


def ingest_parkplatz():
    '''Ingest all Parkplatz CSVs from both Empower and Kissel subfolders.

    Incremental: skips any file whose size+mtime already match what was
    recorded at its last ingest. A same-named file with a changed size/mtime
    has its previously-ingested rows deleted and is fully re-parsed.
    '''
    conn = get_connection()
    print('[ingest_parkplatz] Ensuring table exists...')
    conn.execute(CREATE_SEQ_SQL)
    conn.execute(CREATE_SQL)

    subfolders = ['Empower_Parkplatz_data', 'Kissel_Parkplatz_data']
    all_files = []

    for subfolder in subfolders:
        folder_path = os.path.join(PARKPLATZ_DIR, subfolder)
        if os.path.isdir(folder_path):
            count_before = len(all_files)
            for root, dirs, files in os.walk(folder_path):
                for f in files:
                    if f.lower().endswith('.csv'):
                        all_files.append(os.path.join(root, f))
            print(f"[ingest_parkplatz] Found {len(all_files) - count_before} files in {subfolder}")

    if os.path.isdir(PARKPLATZ_DIR):
        for f in os.listdir(PARKPLATZ_DIR):
            fpath = os.path.join(PARKPLATZ_DIR, f)
            if f.lower().endswith('.csv') and os.path.isfile(fpath):
                all_files.append(fpath)

    all_files = sorted(all_files)

    to_process, unchanged = classify_ingest_files(conn, 'ingested_parkplatz', all_files)
    print(f"[ingest_parkplatz] Found {len(all_files)} files, "
          f"{len(unchanged)} unchanged (skipped), {len(to_process)} new/changed — processing those.")

    if not to_process:
        conn.close()
        return

    # A "changed" file (same name, different size/mtime) may already have
    # stale rows from its previous version — clear them before reinserting.
    # No-op for a genuinely new filename.
    conn.executemany(
        'DELETE FROM ingested_parkplatz WHERE filename = ?',
        [(file_name,) for _, file_name, _, _ in to_process],
    )

    total_files = len(to_process)
    print(f"[ingest_parkplatz] Processing {total_files} files...")
    _print_progress('ingest_parkplatz', 0, total_files)

    total_rows = 0
    processed_files = 0
    chunk_size = _default_worker_count() * 4

    for chunk_start in range(0, total_files, chunk_size):
        chunk = to_process[chunk_start:chunk_start + chunk_size]
        parsed_results = []

        with ThreadPoolExecutor(max_workers=_default_worker_count()) as executor:
            futures = {
                executor.submit(_parse_parkplatz_file, path): (path, file_name, size, mtime)
                for path, file_name, size, mtime in chunk
            }
            for future in as_completed(futures):
                path, file_name, size, mtime = futures[future]
                try:
                    parsed_results.append(future.result())
                except Exception as e:
                    print(f"  ERROR in {file_name}: {e}")
                finally:
                    processed_files += 1
                    _print_progress('ingest_parkplatz', processed_files, total_files)

        parsed_results.sort(key=lambda item: (
            pd.Timestamp.max if item[1] is None else pd.Timestamp(item[1]),
            item[0],
        ))

        # Insert the whole chunk in one bulk_insert call rather than one per
        # file — parsed_results already holds every file's records in memory,
        # so batching avoids a separate DataFrame-construction + view
        # register/unregister round trip per file.
        chunk_records = [record for _, _, records in parsed_results for record in records]
        total_rows += _insert_records(conn, chunk_records)

        # Parkplatz has no "wrong format" rejection path (Empower/Kissel
        # sub-format is detected and handled internally) — every attempted
        # file is recorded, whether or not it produced rows. Updating the
        # manifest per chunk (not just once at the end) means a mid-run
        # crash doesn't waste time reprocessing already-committed chunks.
        update_ingest_manifest(conn, 'ingested_parkplatz', [(fn, sz, mt) for _, fn, sz, mt in chunk])

        gc.collect()

    print()
    conn.close()
    print(f"[ingest_parkplatz] Ingested {total_rows} parkplatz records total.")
