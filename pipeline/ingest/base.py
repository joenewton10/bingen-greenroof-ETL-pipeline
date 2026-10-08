"""
DuckDB connection helper for the pipeline.

The pipeline writes to a single DuckDB file. The path can be overridden per-run
via the PIPELINE_DB_PATH environment variable (used by scripts/run_update.py
to build into a staging file before atomically swapping it into place) — stage
modules never need to know about this, they just call get_connection().
"""
import os
from datetime import datetime
import duckdb
from config.settings import DUCKDB_PATH


def get_db_path():
    """Return the DuckDB file path this process should read/write."""
    return os.getenv("PIPELINE_DB_PATH", DUCKDB_PATH)


def get_connection(read_only: bool = False):
    """Return a DuckDB connection to the configured database file."""
    path = get_db_path()
    parent_dir = os.path.dirname(path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    return duckdb.connect(database=path, read_only=read_only)


def bulk_insert(conn, table, columns, rows):
    """Insert a list of row-tuples into `table` via a registered DataFrame.

    Much faster than row-by-row inserts and avoids psycopg2-specific APIs —
    DuckDB ingests columnar data natively.
    """
    if not rows:
        return 0

    import pandas as pd

    df = pd.DataFrame(rows, columns=columns)
    view_name = f"_bulk_insert_{table}"
    conn.register(view_name, df)
    try:
        col_list = ", ".join(columns)
        conn.execute(f"INSERT INTO {table} ({col_list}) SELECT {col_list} FROM {view_name}")
    finally:
        conn.unregister(view_name)
    return len(rows)


_INGEST_MANIFEST_TABLE = "_ingest_file_manifest"

_CREATE_INGEST_MANIFEST_SQL = f'''
CREATE TABLE IF NOT EXISTS {_INGEST_MANIFEST_TABLE} (
    table_name  TEXT,
    file_name   TEXT,
    file_size   BIGINT,
    file_mtime  TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (table_name, file_name)
);
'''


def _ensure_ingest_manifest_table(conn):
    conn.execute(_CREATE_INGEST_MANIFEST_SQL)


def get_ingest_manifest(conn, table_name):
    """Return {file_name: (file_size, file_mtime)} previously recorded for table_name."""
    _ensure_ingest_manifest_table(conn)
    rows = conn.execute(
        f"SELECT file_name, file_size, file_mtime FROM {_INGEST_MANIFEST_TABLE} WHERE table_name = ?",
        [table_name],
    ).fetchall()
    return {file_name: (size, mtime) for file_name, size, mtime in rows}


def update_ingest_manifest(conn, table_name, file_records):
    """Upsert (file_size, file_mtime) for each (file_name, file_size, file_mtime) tuple."""
    if not file_records:
        return
    _ensure_ingest_manifest_table(conn)
    conn.executemany(
        f'''
        INSERT INTO {_INGEST_MANIFEST_TABLE} (table_name, file_name, file_size, file_mtime, ingested_at)
        VALUES (?, ?, ?, ?, now())
        ON CONFLICT (table_name, file_name) DO UPDATE SET
            file_size   = excluded.file_size,
            file_mtime  = excluded.file_mtime,
            ingested_at = excluded.ingested_at
        ''',
        [(table_name, name, size, mtime) for name, size, mtime in file_records],
    )


def _stat_file(path):
    """Return (size, mtime) for a file, mtime truncated to whole seconds."""
    st = os.stat(path)
    return st.st_size, datetime.fromtimestamp(int(st.st_mtime))


def classify_ingest_files(conn, table_name, file_paths):
    """Split file_paths into (to_process, unchanged) by comparing size+mtime
    against table_name's manifest. Only touches filesystem metadata (os.stat)
    — never opens file contents — so this is safe/cheap to call before any
    format-sniffing or parsing, including for candidates that will turn out
    to belong to a different parser entirely.

    Each returned entry is (path, file_name, size, mtime).
    """
    manifest = get_ingest_manifest(conn, table_name)
    seen_names = {}
    to_process, unchanged = [], []
    for path in file_paths:
        file_name = os.path.basename(path)
        size, mtime = _stat_file(path)

        if file_name in seen_names:
            # Two files in this run's scan share a basename. The manifest and
            # any delete-before-reinsert logic key on file_name alone, so this
            # would silently corrupt tracking for both. Force both to
            # reprocess rather than risk one masking the other's changes.
            print(f"[WARN] duplicate basename '{file_name}' in this run's scan "
                  f"({seen_names[file_name]} and {path}) — forcing both to reprocess.")
            to_process.append((path, file_name, size, mtime))
            continue

        seen_names[file_name] = path
        prev = manifest.get(file_name)
        if prev is not None and prev == (size, mtime):
            unchanged.append((path, file_name, size, mtime))
        else:
            to_process.append((path, file_name, size, mtime))

    return to_process, unchanged
