"""
Quality-control filter for black-globe staging data.

Table naming convention for Bingen pipeline:
- Source: ingested_black_globes
- Output: qc_filtered_black_globes

Range rationale: black globes absorb solar radiation and run well above
ambient air temperature in direct sun (commonly 20-40 C above ambient), so
the ceiling is set higher than the existing air-temp range (-25 to 45 C)
used elsewhere in this pipeline. The floor is widened slightly too, since a
globe can radiate to a clear night sky and read a little below ambient.
"""
import time
from pipeline.ingest.base import get_connection

QC_FILTER_RANGES = {
    'mobiga_nord_c': (-30.0, 90.0),
    'mobiga_sued_c': (-30.0, 90.0),
    'gruendach_c': (-30.0, 90.0),
    'parkplatz_c': (-30.0, 90.0),
}

QC_FILTER_SQL = '''
CREATE TABLE IF NOT EXISTS qc_filtered_black_globes AS
SELECT *
FROM ingested_black_globes
WHERE (mobiga_nord_c IS NULL OR (mobiga_nord_c BETWEEN -30.0 AND 90.0))
  AND (mobiga_sued_c IS NULL OR (mobiga_sued_c BETWEEN -30.0 AND 90.0))
  AND (gruendach_c   IS NULL OR (gruendach_c   BETWEEN -30.0 AND 90.0))
  AND (parkplatz_c   IS NULL OR (parkplatz_c   BETWEEN -30.0 AND 90.0))
ORDER BY timestamp ASC;
'''

INDEX_SQL = '''
CREATE INDEX IF NOT EXISTS idx_qc_filtered_black_globes_ts ON qc_filtered_black_globes(timestamp);
'''


def qc_filter_black_globes():
    '''Apply QC filtering to black-globe data, then add performance indexes.'''
    conn = get_connection()

    conn.execute('DROP TABLE IF EXISTS qc_filtered_black_globes;')
    print('[qc_filter_black_globes] QC filtering black-globe records...', flush=True)
    t0 = time.time()
    conn.execute(QC_FILTER_SQL)
    conn.execute(INDEX_SQL)

    count = conn.execute('SELECT COUNT(*) FROM qc_filtered_black_globes;').fetchone()[0]
    print(f'[qc_filter_black_globes] {count} records retained after QC filtering ({time.time() - t0:.1f}s)')

    conn.close()
