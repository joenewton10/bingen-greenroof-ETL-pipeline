'''
Harmonize Parkplatz Data into analysis schema.

Table naming convention for Bingen pipeline:
- Source: qc_filtered_parkplatz
- Output: harm_parkplatz
'''
import time
from pipeline.ingest.base import get_connection


HARM_PARKPLATZ_SQL = '''
CREATE TABLE IF NOT EXISTS harm_parkplatz AS
SELECT
    id,
    timestamp,
    ir1_wm2 AS "IR1 [W/m2]",
    sr1_wm2 AS "SR1 [W/m2]",
    ir2_wm2 AS "IR2 [W/m2]",
    sr2_wm2 AS "SR2 [W/m2]",
    temp_c AS "Temp [C]",
    air_pressure_mbar AS "Air Pressure [mbar]",
    soil_moisture_vol AS "Soil Moisture [vol%]",
    air_humidity_1_rh AS "Air Humidity 1 [%RH]",
    air_temp_1_c AS "Air Temp 1 [C]",
    air_humidity_2_rh AS "Air Humidity 2 [%RH]",
    air_temp_2_c AS "Air Temp 2 [C]",
    wind_speed_ms AS "Wind Speed [m/s]",
    wind_direction_deg AS "Wind Direction [deg]",
    soil_temp_1_c AS "Soil Temp 1 [C]",
    soil_temp_2_c AS "Soil Temp 2 [C]",
    serial_number
FROM qc_filtered_parkplatz
ORDER BY timestamp ASC;
'''

INDEX_HARM_PARKPLATZ_SQL = '''
CREATE INDEX IF NOT EXISTS idx_harm_parkplatz_ts ON harm_parkplatz(timestamp);
'''


def harmonize_parkplatz():
    '''Harmonize qc_filtered_parkplatz into harm_parkplatz.'''
    conn = get_connection()

    conn.execute('DROP TABLE IF EXISTS harm_parkplatz;')
    print('[harmonize_parkplatz] Building harm_parkplatz...', flush=True)
    t0 = time.time()
    conn.execute(HARM_PARKPLATZ_SQL)
    conn.execute(INDEX_HARM_PARKPLATZ_SQL)

    count = conn.execute('SELECT COUNT(*) FROM harm_parkplatz;').fetchone()[0]
    print(f'[harmonize_parkplatz] Harmonized {count} parkplatz records ({time.time() - t0:.1f}s)')

    conn.close()
