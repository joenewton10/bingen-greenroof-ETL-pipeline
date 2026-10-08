"""
Ad-hoc SQL query page for the dashboard.

DuckDB's own web UI (CALL start_ui()) needs to download a native extension at
runtime — this is blocked by Application Control Policy on locked-down
institutional PCs (confirmed on this exact deployment). This page gives the
same "browse the data + run your own SQL" capability using only what's
already proven to work here: Streamlit and the core duckdb package, no
extension involved.
"""
import os
import tempfile

import duckdb
import streamlit as st

from analysis import _default_db_path

MAX_ROWS = 10_000
EXCEL_ROW_LIMIT = 1_048_576
DEFAULT_QUERY = "SELECT * FROM synchronized_data_filtered ORDER BY timestamp DESC LIMIT 100"


def _export_full_csv(conn, query):
    """Run `query` unbounded and return (row_count, csv_bytes).

    Uses DuckDB's own COPY TO rather than pandas' to_csv(): on the full
    synchronized_data_filtered table (2.15M rows x 49 cols), DuckDB's native
    CSV writer finished in ~13s versus pandas taking several minutes for the
    same export — pandas round-trips through Python objects per cell, DuckDB's
    writer is vectorized and never leaves compiled code.
    """
    row_count = conn.sql(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]

    fd, tmp_path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    try:
        conn.sql(f"COPY ({query}) TO '{tmp_path}' (HEADER, DELIMITER ',')")
        with open(tmp_path, "rb") as f:
            csv_bytes = f.read()
    finally:
        os.remove(tmp_path)

    return row_count, csv_bytes


def render_query_page():
    st.markdown('<p style="font-size:2rem;font-weight:bold;">🔍 Query Data</p>', unsafe_allow_html=True)
    st.write(
        "Run your own SQL queries directly against the database. The connection is "
        "read-only, so nothing typed here can modify the data."
    )

    db_path = _default_db_path()
    try:
        conn = duckdb.connect(database=db_path, read_only=True)
    except Exception as exc:
        st.error(f"Could not open the database: {exc}")
        return

    with st.expander("Available tables"):
        try:
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables ORDER BY table_name"
            ).fetchall()
            st.write([t[0] for t in tables])
        except Exception as exc:
            st.caption(f"Could not list tables: {exc}")

    query = st.text_area("SQL query", value=DEFAULT_QUERY, height=120)

    if st.button("▶ Run Query", type="primary"):
        st.session_state.query_error = None
        st.session_state.full_csv_bytes = None
        try:
            # .limit() on the relation happens before the result is pulled into
            # a DataFrame, so a query with no LIMIT of its own (e.g. a bare
            # SELECT * FROM synchronized_data_filtered, 2M+ rows) doesn't first
            # materialize the whole thing just to truncate it afterwards.
            relation = conn.sql(query)
            preview = relation.limit(MAX_ROWS + 1).df()
            truncated = len(preview) > MAX_ROWS
            st.session_state.preview_result = preview.head(MAX_ROWS) if truncated else preview
            st.session_state.preview_truncated = truncated
            st.session_state.last_query = query
        except Exception as exc:
            st.session_state.query_error = str(exc)
            st.session_state.preview_result = None

    if st.session_state.get("query_error"):
        st.error(f"Query failed: {st.session_state.query_error}")

    preview = st.session_state.get("preview_result")
    if preview is not None:
        if st.session_state.get("preview_truncated"):
            st.warning(
                f"Showing the first {MAX_ROWS:,} rows. Add your own LIMIT (or narrow "
                f"the query, e.g. with a date range) to see a different slice."
            )
        st.dataframe(preview, use_container_width=True)
        st.caption(f"{len(preview):,} row{'s' if len(preview) != 1 else ''} shown")

        if st.session_state.get("preview_truncated"):
            st.markdown("---")
            st.caption(
                "The preview above is capped for display, but you can still download "
                "the complete result of this query — every row, not just the preview."
            )
            if st.button("📥 Prepare full result for download"):
                with st.spinner("Running the full query (no row limit)..."):
                    try:
                        row_count, csv_bytes = _export_full_csv(conn, st.session_state.last_query)
                        st.session_state.full_csv_bytes = csv_bytes
                        st.session_state.full_csv_rows = row_count
                    except Exception as exc:
                        st.error(f"Could not prepare the full result: {exc}")

        csv_bytes = st.session_state.get("full_csv_bytes")
        if csv_bytes is not None:
            row_count = st.session_state.get("full_csv_rows", 0)
            if row_count > EXCEL_ROW_LIMIT:
                st.warning(
                    f"This result has {row_count:,} rows — more than Excel can open in "
                    f"one sheet ({EXCEL_ROW_LIMIT:,} max). The CSV will contain everything; "
                    f"Excel will just silently cut it off at its own limit if you open it there."
                )
            st.download_button(
                "⬇ Download full result as CSV",
                data=csv_bytes,
                file_name="query_result.csv",
                mime="text/csv",
            )
            st.caption(f"{row_count:,} rows, {len(csv_bytes) / (1024 * 1024):.1f} MB")

    conn.close()
