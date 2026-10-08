'''
Harmonize Greenroof Data - Combine BOTH QC-filtered sources into canonical schema.

Based on: combine_greenroof.py

Table naming convention for Bingen pipeline:
- Source: qc_filtered_greenroof
- Output: harm_greenroof

QC-filtered table schema:
  id, timestamp, serial_no, ir1, sr1, ir2, sr2, temperature, air_pressure,
    soil_moisture, soil_temp_1, soil_temp_2, air_humidity_1, air_temp_1,
    air_humidity_2, air_temp_2, wind_speed, wind_direction, file_name, source

Canonical columns (17 total):
1. id, 2. timestamp, 3. serial_number,
4. ir1_in_lw, 5. sr1_in_sw, 6. ir2_out_lw, 7. sr2_ref_sw,
8. air_temperature, 9. relative_humidity, 10. air_temp_2, 11. air_humidity_2,
12. wind_speed_avg, 13. wind_direction_1, 14. soil_temperature,
15. soil_moisture, 16. source, 17. filename
'''
import time
from pipeline.ingest.base import get_connection


# Harmonize qc_filtered_greenroof with column mapping to canonical names
HARM_SQL = '''
CREATE TABLE IF NOT EXISTS harm_greenroof AS
SELECT
    id,
    timestamp,
    serial_no AS serial_number,
    ir1 AS ir1_in_lw,
    sr1 AS sr1_in_sw,
    ir2 AS ir2_out_lw,
    sr2 AS sr2_ref_sw,
    air_temp_1 AS air_temperature,
    air_humidity_1 AS relative_humidity,
    air_temp_2 AS air_temp_2,
    air_humidity_2 AS air_humidity_2,
    wind_speed AS wind_speed_avg,
    wind_direction AS wind_direction_1,
    soil_temp_1 AS soil_temperature,
    soil_moisture,
    source,
    file_name AS filename
FROM qc_filtered_greenroof

ORDER BY timestamp ASC;
'''

INDEX_SQL = '''
CREATE INDEX IF NOT EXISTS idx_harm_greenroof_ts ON harm_greenroof(timestamp);
'''


def harmonize_greenroof():
    '''Harmonize qc_filtered_greenroof into canonical harm_greenroof table.'''
    conn = get_connection()

    conn.execute('DROP TABLE IF EXISTS harm_greenroof;')
    print('[harmonize_greenroof] Combining Empower + Kissel into canonical schema...', flush=True)
    t0 = time.time()
    conn.execute(HARM_SQL)
    conn.execute(INDEX_SQL)

    count = conn.execute('SELECT COUNT(*) FROM harm_greenroof;').fetchone()[0]
    print(f'[harmonize_greenroof] Combined {count} greenroof records into canonical schema ({time.time() - t0:.1f}s)')

    # Show source breakdown
    for row in conn.execute('''
        SELECT source, COUNT(*) AS records
        FROM harm_greenroof
        GROUP BY 1;
    ''').fetchall():
        print(f'  {row[0]}: {row[1]} records')

    conn.close()
