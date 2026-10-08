"""
Paginated, no-SQL table browser for the dashboard.

Query Data (query_tool.py) covers "run your own SQL" and "download a full
result." This covers the simpler, more common need: just look at the whole
dataset, page by page, without typing anything. Excel itself can't hold more
than ~1.05M rows in one sheet anyway, so "viewing everything at once" was
never really the goal here — paging through the full table, landing anywhere
in it, is.

A live timing check against the real 2.19M-row database showed DuckDB does a
full vectorized scan for either an OFFSET-based or a keyset-style paged
query at this size, and both land in the same ~0.04-0.2s regardless of page
depth — so plain OFFSET pagination (simpler, and it supports jumping to any
page directly) is used rather than keyset/cursor pagination.
"""
import duckdb
import streamlit as st

from analysis import _default_db_path

PAGE_SIZE_OPTIONS = [100, 500, 1000, 5000]
DEFAULT_PAGE_SIZE = 1000
DEFAULT_TABLE = "synchronized_data_filtered"
PAGE_KEY = "browse_page_num"


def _go_to_page(page_num, total_pages):
    st.session_state[PAGE_KEY] = max(1, min(total_pages, page_num))


def render_browse_page():
    st.markdown('<p style="font-size:2rem;font-weight:bold;">📄 Browse Data</p>', unsafe_allow_html=True)
    st.write("Page through the full dataset, no SQL required. Read-only.")

    db_path = _default_db_path()
    try:
        conn = duckdb.connect(database=db_path, read_only=True)
    except Exception as exc:
        st.error(f"Could not open the database: {exc}")
        return

    try:
        tables = [
            row[0] for row in conn.execute(
                "SELECT table_name FROM information_schema.tables ORDER BY table_name"
            ).fetchall()
        ]
    except Exception as exc:
        st.error(f"Could not list tables: {exc}")
        conn.close()
        return

    default_index = tables.index(DEFAULT_TABLE) if DEFAULT_TABLE in tables else 0
    col_table, col_size, col_sort = st.columns([3, 2, 2])
    with col_table:
        table = st.selectbox("Table", options=tables, index=default_index)
    with col_size:
        page_size = st.selectbox(
            "Rows per page",
            options=PAGE_SIZE_OPTIONS,
            index=PAGE_SIZE_OPTIONS.index(DEFAULT_PAGE_SIZE),
        )
    with col_sort:
        newest_first = st.toggle("Newest first", value=True)

    # Reset to page 1 whenever the selection changes — "page 50" from a
    # different table/page-size/sort doesn't mean anything here.
    selection_key = (table, page_size, newest_first)
    if st.session_state.get("browse_selection_key") != selection_key:
        st.session_state.browse_selection_key = selection_key
        st.session_state[PAGE_KEY] = 1

    try:
        total_rows = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    except Exception as exc:
        st.error(f"Could not read table: {exc}")
        conn.close()
        return

    if total_rows == 0:
        st.info("This table has no rows.")
        conn.close()
        return

    total_pages = max(1, -(-total_rows // page_size))  # ceil division
    if PAGE_KEY not in st.session_state:
        st.session_state[PAGE_KEY] = 1
    if st.session_state[PAGE_KEY] > total_pages:
        st.session_state[PAGE_KEY] = total_pages

    col_prev, col_jump, col_next, col_info = st.columns([1, 2, 1, 3])
    current_page = st.session_state[PAGE_KEY]
    with col_prev:
        st.button(
            "◀ Previous",
            on_click=_go_to_page,
            args=(current_page - 1, total_pages),
            disabled=current_page <= 1,
            use_container_width=True,
        )
    with col_next:
        st.button(
            "Next ▶",
            on_click=_go_to_page,
            args=(current_page + 1, total_pages),
            disabled=current_page >= total_pages,
            use_container_width=True,
        )
    with col_jump:
        st.number_input("Jump to page", min_value=1, max_value=total_pages, key=PAGE_KEY)
    with col_info:
        st.markdown(f"<div style='padding-top:1.8rem'>of {total_pages:,} pages</div>", unsafe_allow_html=True)

    current_page = st.session_state[PAGE_KEY]
    offset = (current_page - 1) * page_size
    order = "DESC" if newest_first else "ASC"

    try:
        page_df = conn.execute(
            f'SELECT * FROM "{table}" ORDER BY timestamp {order} LIMIT ? OFFSET ?',
            [page_size, offset],
        ).df()
    except Exception:
        # Fall back for the rare table with no timestamp column.
        page_df = conn.execute(
            f'SELECT * FROM "{table}" LIMIT ? OFFSET ?', [page_size, offset]
        ).df()

    row_start = offset + 1
    row_end = min(offset + page_size, total_rows)
    st.caption(f"Rows {row_start:,}–{row_end:,} of {total_rows:,} total")
    st.dataframe(page_df, use_container_width=True)

    conn.close()
