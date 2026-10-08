'''
Harmonize black-globe data into analysis schema.

Table naming convention for Bingen pipeline:
- Source: qc_filtered_black_globes
- Output: harm_black_globes

Columns are renamed to globe_temp_<site> so they can't collide with the
existing avg_*_greenroof / avg_*_parkplatz columns once joined in
pipeline/sync/sync_sites.py.
'''
import time
from pipeline.ingest.base import get_connection


HARM_BLACK_GLOBES_SQL = '''
CREATE TABLE IF NOT EXISTS harm_black_globes AS
SELECT
    id,
    timestamp,
    gruendach_c   AS globe_temp_gruendach,
    parkplatz_c   AS globe_temp_parkplatz,
    mobiga_nord_c AS globe_temp_mobiga_nord,
    mobiga_sued_c AS globe_temp_mobiga_sued,
    record_number,
    source_file
FROM qc_filtered_black_globes
ORDER BY timestamp ASC;
'''

INDEX_HARM_BLACK_GLOBES_SQL = '''
CREATE INDEX IF NOT EXISTS idx_harm_black_globes_ts ON harm_black_globes(timestamp);
'''


def harmonize_black_globes():
    '''Harmonize qc_filtered_black_globes into harm_black_globes.'''
    conn = get_connection()

    conn.execute('DROP TABLE IF EXISTS harm_black_globes;')
    print('[harmonize_black_globes] Building harm_black_globes...', flush=True)
    t0 = time.time()
    conn.execute(HARM_BLACK_GLOBES_SQL)
    conn.execute(INDEX_HARM_BLACK_GLOBES_SQL)

    count = conn.execute('SELECT COUNT(*) FROM harm_black_globes;').fetchone()[0]
    print(f'[harmonize_black_globes] Harmonized {count} black-globe records ({time.time() - t0:.1f}s)')

    conn.close()
