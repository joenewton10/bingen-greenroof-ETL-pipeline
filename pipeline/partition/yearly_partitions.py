"""
Yearly Tables for synchronized_data_filtered.

PostgreSQL declarative PARTITION BY RANGE doesn't exist in DuckDB, and nothing
downstream actually queries this table directly — the dashboard reads
synchronized_data_filtered exclusively. So rather than reproducing true
partitioning, this stage builds one physical table per calendar year
(sync_data_2020, sync_data_2021, ...) — useful for anyone who wants to poke at
a single year without writing a WHERE clause — plus a `synchronized_data_yearly`
view that unions them back together, preserving "query this for everything".

Adding a new sensor year: just run the pipeline — this stage detects years
present in synchronized_data_filtered and (re)builds tables for all of them
each run.
"""
import sys
import time
from pathlib import Path

# Allow running this file directly from any working directory
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from pipeline.ingest.base import get_connection


YEARS_IN_DATA_SQL = """
SELECT DISTINCT EXTRACT(YEAR FROM timestamp)::INT AS yr
FROM synchronized_data_filtered
WHERE timestamp IS NOT NULL
ORDER BY yr;
"""


def _drop_existing_year_tables(conn):
    """Drop the view and any previously-built per-year tables."""
    conn.execute("DROP VIEW IF EXISTS synchronized_data_yearly;")
    existing = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'sync_data_%'"
    ).fetchall()
    for (table_name,) in existing:
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}";')


def create_yearly_partitions():
    """
    Build one table per calendar year from synchronized_data_filtered, plus a
    `synchronized_data_yearly` view that unions them.

    Steps:
      1. Detect all years present in synchronized_data_filtered.
      2. Drop previously-built year tables/view.
      3. Create one table per year.
      4. Create the union view.
    """
    conn = get_connection()

    print('[yearly_partitions] Reading years in synchronized_data_filtered...')
    years = [row[0] for row in conn.execute(YEARS_IN_DATA_SQL).fetchall()]

    if not years:
        print('[yearly_partitions] No data found in synchronized_data_filtered — skipping.')
        conn.close()
        return

    print(f'[yearly_partitions] Years detected: {years}')
    t0 = time.time()

    print('[yearly_partitions] Dropping previous year tables/view...')
    _drop_existing_year_tables(conn)

    for year in years:
        table = f'sync_data_{year}'
        print(f'  Building {table}...')
        conn.execute(f'''
            CREATE TABLE "{table}" AS
            SELECT * FROM synchronized_data_filtered
            WHERE EXTRACT(YEAR FROM timestamp) = {year}
            ORDER BY timestamp ASC;
        ''')

    union_sql = ' UNION ALL '.join(f'SELECT * FROM "sync_data_{year}"' for year in years)
    conn.execute(f'CREATE VIEW synchronized_data_yearly AS {union_sql};')

    # Summary
    rows = conn.execute("""
        SELECT
            EXTRACT(YEAR FROM timestamp)::INT AS yr,
            COUNT(*) AS rows,
            MIN(timestamp) AS first_ts,
            MAX(timestamp) AS last_ts
        FROM synchronized_data_yearly
        GROUP BY yr
        ORDER BY yr;
    """).fetchall()
    print(f'\n[yearly_partitions] Year summary ({time.time() - t0:.1f}s):')
    print(f'  {"Year":<6}  {"Rows":>10}  {"First":>20}  {"Last":>20}')
    print('  ' + '-' * 62)
    for yr, count, first, last in rows:
        print(f'  {yr:<6}  {count:>10,}  {str(first)[:19]:>20}  {str(last)[:19]:>20}')

    total = conn.execute("SELECT COUNT(*) FROM synchronized_data_yearly;").fetchone()[0]
    print(f'\n  Total rows across all years: {total:,}')

    conn.close()
