"""
Quality-control filter for greenroof ingested tables from Empower and Kissel.
Uses simplified schema (no metadata/counter/system columns).

Table naming convention for Bingen pipeline:
- Source: ingested_empower_greenroof, ingested_kissel_greenroof
- Output: qc_filtered_greenroof

NOTE: Known data quirks:
- soil_temp_2: Often -50.0 (sentinel for missing/disconnected sensor)
- This is handled by allowing values equal to -50 to pass through
"""
import time
from pipeline.ingest.base import get_connection

# Quality-control ranges based on Bingen am Rhein climate and sensor capabilities
QC_FILTER_RANGES = {
    'air_temperature': (-25.0, 45.0),
    'soil_temperature': (-20.0, 70.0),  # surfaces can exceed 50 C in summer
    'relative_humidity': (0.0, 100.0),
    'wind_speed': (0.0, 50.0),
    'wind_direction': (0.0, 360.0),
    'soil_moisture': (0.0, 60.0),
    'sr1_in_sw': (-15.0, 1400.0),  # Incoming shortwave with modest negative tolerance
    'sr2_out_sw': (-15.0, 1400.0),  # Reflected shortwave with modest negative tolerance
    'ir1_in_lw': (-200.0, 100.0),  # Incoming longwave pre-recalc signal range
    'ir2_out_lw': (-200.0, 150.0),  # Outgoing longwave pre-recalc signal range
    'temperature': (-25.0, 45.0),  # Generic temperature sensor
    'air_pressure': (900.0, 1100.0),  # hPa
}

# Quality-filter greenroof ingested tables into a single QC output table.
# soil_temp_2 allows -50 as sentinel value for missing sensor.
QC_FILTER_GREENROOF_SQL = '''
CREATE TABLE IF NOT EXISTS qc_filtered_greenroof AS

-- Empower source
SELECT
    id,
    timestamp,
    serial_no,
    ir1, sr1, ir2, sr2,
    temperature,
    air_pressure,
    soil_moisture,
    soil_temp_1, soil_temp_2,
    air_humidity_1, air_temp_1,
    air_humidity_2, air_temp_2,
    wind_speed, wind_direction,
    file_name,
    'empower'::text AS source
FROM ingested_empower_greenroof
WHERE
    -- Air temperature checks
    (air_temp_1 IS NULL OR (air_temp_1 BETWEEN -25.0 AND 45.0))
    AND (air_temp_2 IS NULL OR (air_temp_2 BETWEEN -25.0 AND 45.0))
    AND (temperature IS NULL OR (temperature BETWEEN -25.0 AND 45.0))

    -- Soil temperature checks (allow -50 sentinel)
    AND (soil_temp_1 IS NULL OR (soil_temp_1 BETWEEN -20.0 AND 70.0))
    AND (soil_temp_2 IS NULL OR soil_temp_2 = -50.0 OR (soil_temp_2 BETWEEN -20.0 AND 70.0))

    -- Humidity checks
    AND (air_humidity_1 IS NULL OR (air_humidity_1 BETWEEN 0.0 AND 100.0))
    AND (air_humidity_2 IS NULL OR (air_humidity_2 BETWEEN 0.0 AND 100.0))

    -- Wind checks
    AND (wind_speed IS NULL OR (wind_speed BETWEEN 0.0 AND 50.0))
    AND (wind_direction IS NULL OR (wind_direction BETWEEN 0.0 AND 360.0))

    -- Soil moisture check
    AND (soil_moisture IS NULL OR (soil_moisture BETWEEN 0.0 AND 60.0))

    -- Radiation checks
    -- Incoming channels should not be materially negative; outgoing channels may be strongly negative.
    AND (sr1 IS NULL OR (sr1 BETWEEN -15.0 AND 1400.0))
    AND (sr2 IS NULL OR (sr2 BETWEEN -15.0 AND 1400.0))
    AND (ir1 IS NULL OR (ir1 BETWEEN -200.0 AND 100.0))
    AND (ir2 IS NULL OR (ir2 BETWEEN -200.0 AND 150.0))

    -- Pressure check
    AND (air_pressure IS NULL OR (air_pressure BETWEEN 900.0 AND 1100.0))

UNION ALL

-- Kissel source
SELECT
    id,
    timestamp,
    serial_no,
    ir1, sr1, ir2, sr2,
    temperature,
    air_pressure,
    soil_moisture,
    soil_temp_1, soil_temp_2,
    air_humidity_1, air_temp_1,
    air_humidity_2, air_temp_2,
    wind_speed, wind_direction,
    file_name,
    'kissel'::text AS source
FROM ingested_kissel_greenroof
WHERE
    -- Air temperature checks
    (air_temp_1 IS NULL OR (air_temp_1 BETWEEN -25.0 AND 45.0))
    AND (air_temp_2 IS NULL OR (air_temp_2 BETWEEN -25.0 AND 45.0))
    AND (temperature IS NULL OR (temperature BETWEEN -25.0 AND 45.0))

    -- Soil temperature checks (allow -50 sentinel)
    AND (soil_temp_1 IS NULL OR (soil_temp_1 BETWEEN -20.0 AND 70.0))
    AND (soil_temp_2 IS NULL OR soil_temp_2 = -50.0 OR (soil_temp_2 BETWEEN -20.0 AND 70.0))

    -- Humidity checks
    AND (air_humidity_1 IS NULL OR (air_humidity_1 BETWEEN 0.0 AND 100.0))
    AND (air_humidity_2 IS NULL OR (air_humidity_2 BETWEEN 0.0 AND 100.0))

    -- Wind checks
    AND (wind_speed IS NULL OR (wind_speed BETWEEN 0.0 AND 50.0))
    AND (wind_direction IS NULL OR (wind_direction BETWEEN 0.0 AND 360.0))

    -- Soil moisture check
    AND (soil_moisture IS NULL OR (soil_moisture BETWEEN 0.0 AND 60.0))

    -- Radiation checks
    -- Incoming channels should not be materially negative; outgoing channels may be strongly negative.
    AND (sr1 IS NULL OR (sr1 BETWEEN -15.0 AND 1400.0))
    AND (sr2 IS NULL OR (sr2 BETWEEN -15.0 AND 1400.0))
    AND (ir1 IS NULL OR (ir1 BETWEEN -200.0 AND 100.0))
    AND (ir2 IS NULL OR (ir2 BETWEEN -200.0 AND 150.0))

    -- Pressure check (allow NULL since Kissel doesn't have pressure)
    AND (air_pressure IS NULL OR (air_pressure BETWEEN 900.0 AND 1100.0))
ORDER BY timestamp ASC;
'''

INDEX_SQL = '''
CREATE INDEX IF NOT EXISTS idx_qc_filtered_greenroof_ts ON qc_filtered_greenroof(timestamp);
CREATE INDEX IF NOT EXISTS idx_qc_filtered_greenroof_serial ON qc_filtered_greenroof(serial_no);
CREATE INDEX IF NOT EXISTS idx_qc_filtered_greenroof_source ON qc_filtered_greenroof(source);
'''


def qc_filter_greenroof():
    '''Apply QC filtering to both greenroof ingested tables into qc_filtered_greenroof.'''
    conn = get_connection()

    conn.execute('DROP TABLE IF EXISTS qc_filtered_greenroof;')
    print('[qc_filter_greenroof] QC filtering Greenroof records (Empower + Kissel)...', flush=True)
    t0 = time.time()
    conn.execute(QC_FILTER_GREENROOF_SQL)
    for stmt in INDEX_SQL.strip().split(';'):
        if stmt.strip():
            conn.execute(stmt)

    source_counts = dict(conn.execute(
        "SELECT source, COUNT(*) FROM qc_filtered_greenroof GROUP BY source ORDER BY source"
    ).fetchall())
    empower_count = source_counts.get('empower', 0)
    kissel_count = source_counts.get('kissel', 0)
    total = empower_count + kissel_count
    print(f'[qc_filter_greenroof] Empower: {empower_count} records retained after QC filtering')
    print(f'[qc_filter_greenroof] Kissel: {kissel_count} records retained after QC filtering')
    print(f'[qc_filter_greenroof] Total: {total} greenroof records retained after QC filtering ({time.time() - t0:.1f}s)')

    conn.close()
