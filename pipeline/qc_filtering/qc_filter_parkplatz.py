"""
Quality-control filter for parkplatz staging data.
Uses permissive ranges that accommodate observed data characteristics.

Table naming convention for Bingen pipeline:
- Source: ingested_parkplatz
- Output: qc_filtered_parkplatz

NOTE: Known data quirks:
- air_pressure_mbar: Consistently ~800 (possibly uncalibrated or high elevation)
- rain_abs_mm: Cumulative counter that can exceed 600mm
"""
import time
from pipeline.ingest.base import get_connection

# QC filtering ranges - made permissive based on actual data analysis
QC_FILTER_RANGES = {
    'air_temp_1_c': (-25.0, 45.0),
    'air_temp_2_c': (-25.0, 45.0),
    'temp_c': (-25.0, 45.0),
    'soil_temp_1_c': (-20.0, 70.0),  # paved surfaces routinely exceed 50 C in summer
    'soil_temp_2_c': (-20.0, 70.0),  # paved surfaces routinely exceed 50 C in summer
    'internal_temp_c': (-30.0, 80.0),  # Device internal can run hot
    'air_humidity_1_rh': (0.0, 100.0),
    'air_humidity_2_rh': (0.0, 100.0),
    'air_pressure_mbar': (700.0, 1100.0),  # Relaxed to include observed ~800
    'soil_moisture_vol': (0.0, 100.0),
    'ir1_wm2': (-200.0, 100.0),  # Incoming longwave pre-recalc signal range
    'ir2_wm2': (-200.0, 150.0),  # Outgoing longwave pre-recalc signal range
    'sr1_wm2': (-15.0, 1400.0),  # Incoming shortwave with modest negative tolerance
    'sr2_wm2': (-15.0, 1400.0),  # Reflected shortwave with modest negative tolerance
    'wind_speed_ms': (0.0, 50.0),
    'wind_direction_deg': (0.0, 360.0),
    'rain_abs_mm': (0.0, 10000.0),  # Cumulative can be very high
    'rain_rel_mm': (0.0, 200.0),  # Per-interval stays reasonable
    'battery_voltage_v': (8.0, 16.0),  # Slightly higher for charging
}

# Permissive quality-control filtering SQL
QC_FILTER_SQL = '''
CREATE TABLE IF NOT EXISTS qc_filtered_parkplatz AS
SELECT *
FROM ingested_parkplatz
WHERE (air_temp_1_c IS NULL OR (air_temp_1_c BETWEEN -25.0 AND 45.0))
  AND (air_temp_2_c IS NULL OR (air_temp_2_c BETWEEN -25.0 AND 45.0))
  AND (temp_c IS NULL OR (temp_c BETWEEN -25.0 AND 45.0))
  AND (soil_temp_1_c IS NULL OR (soil_temp_1_c BETWEEN -20.0 AND 70.0))
  AND (soil_temp_2_c IS NULL OR (soil_temp_2_c BETWEEN -20.0 AND 70.0))
  AND (internal_temp_c IS NULL OR (internal_temp_c BETWEEN -30.0 AND 80.0))
  AND (air_humidity_1_rh IS NULL OR (air_humidity_1_rh BETWEEN 0.0 AND 100.0))
  AND (air_humidity_2_rh IS NULL OR (air_humidity_2_rh BETWEEN 0.0 AND 100.0))
  AND (air_pressure_mbar IS NULL OR (air_pressure_mbar BETWEEN 700.0 AND 1100.0))
  AND (soil_moisture_vol IS NULL OR (soil_moisture_vol BETWEEN 0.0 AND 100.0))
  AND (ir1_wm2 IS NULL OR (ir1_wm2 BETWEEN -200.0 AND 100.0))
  AND (ir2_wm2 IS NULL OR (ir2_wm2 BETWEEN -200.0 AND 150.0))
  AND (sr1_wm2 IS NULL OR (sr1_wm2 BETWEEN -15.0 AND 1400.0))
  AND (sr2_wm2 IS NULL OR (sr2_wm2 BETWEEN -15.0 AND 1400.0))
  AND (wind_speed_ms IS NULL OR (wind_speed_ms BETWEEN 0.0 AND 50.0))
  AND (wind_direction_deg IS NULL OR (wind_direction_deg BETWEEN 0.0 AND 360.0))
  AND (rain_abs_mm IS NULL OR (rain_abs_mm BETWEEN 0.0 AND 10000.0))
  AND (rain_rel_mm IS NULL OR (rain_rel_mm BETWEEN 0.0 AND 200.0))
  AND (battery_voltage_v IS NULL OR (battery_voltage_v BETWEEN 8.0 AND 16.0))
ORDER BY timestamp ASC;
'''

INDEX_SQL = '''
CREATE INDEX IF NOT EXISTS idx_qc_filtered_parkplatz_timestamp ON qc_filtered_parkplatz(timestamp);
CREATE INDEX IF NOT EXISTS idx_qc_filtered_parkplatz_serial ON qc_filtered_parkplatz(serial_number);
'''


def qc_filter_parkplatz():
    '''Apply QC filtering to parkplatz data, then add performance indexes.'''
    conn = get_connection()

    # Drop and recreate qc_filtered_parkplatz to ensure clean QC filtering state
    conn.execute('DROP TABLE IF EXISTS qc_filtered_parkplatz;')
    print('[qc_filter_parkplatz] QC filtering parkplatz records...', flush=True)
    t0 = time.time()
    conn.execute(QC_FILTER_SQL)
    for stmt in INDEX_SQL.strip().split(';'):
        if stmt.strip():
            conn.execute(stmt)

    count = conn.execute('SELECT COUNT(*) FROM qc_filtered_parkplatz;').fetchone()[0]
    print(f'[qc_filter_parkplatz] {count} records retained after QC filtering ({time.time() - t0:.1f}s)')

    conn.close()
