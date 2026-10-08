"""
Operations page for the Bingen Green Roof dashboard.

Lets a non-technical operator (the professor's assistant) update the dataset
and check on the pipeline without ever opening a terminal:
- Run Update Now: drop new sensor CSVs into data/raw/..., then click this.
- View last run status/log: see whether the last update succeeded and why,
  if it didn't.

This is a thin UI wrapper around scripts that already exist and are already
tested on their own (scripts/run_update.py, which itself builds into
a staging file and only swaps it in once verified) — no new pipeline logic
lives here.
"""
import glob
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import streamlit as st

from analysis import _default_db_path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _db_path():
    # Reuse analysis.py's resolution (checks st.secrets before config.settings)
    # so this page and the dashboard itself always agree on which file is live.
    return _default_db_path()


def _describe_database():
    """Return a short status dict for the live database file, without holding it open."""
    db_path = Path(_db_path())
    if not db_path.exists():
        return {"exists": False, "path": str(db_path)}

    info = {
        "exists": True,
        "path": str(db_path),
        "size_mb": db_path.stat().st_size / (1024 ** 2),
        "modified": datetime.fromtimestamp(db_path.stat().st_mtime),
    }
    try:
        import duckdb
        conn = duckdb.connect(database=str(db_path), read_only=True)
        row = conn.execute(
            "SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM synchronized_data_filtered"
        ).fetchone()
        conn.close()
        info["row_count"], info["min_ts"], info["max_ts"] = row
    except Exception as exc:
        info["read_error"] = str(exc)
    return info


def _recent_rows(n=20):
    """Return the N most-recently-added rows of synchronized_data_filtered.

    Lets an operator glance at the actual new data right after an update,
    without navigating to Query Data or Browse Data first.
    """
    db_path = _db_path()
    try:
        import duckdb
        conn = duckdb.connect(database=db_path, read_only=True)
        df = conn.execute(
            "SELECT * FROM synchronized_data_filtered ORDER BY timestamp DESC LIMIT ?", [n]
        ).df()
        conn.close()
        return df
    except Exception:
        return None


def _latest_log_file():
    logs_dir = PROJECT_ROOT / "logs"
    candidates = sorted(
        glob.glob(str(logs_dir / "pipeline_run_*.log")),
        key=os.path.getmtime,
        reverse=True,
    )
    return Path(candidates[0]) if candidates else None


def _run_update_streaming():
    """Run scripts/run_update.py, streaming output live into the page."""
    cmd = [sys.executable, "scripts/run_update.py"]
    output_area = st.empty()
    lines = []

    # run_update.py is a plain subprocess — it can't see st.secrets, only
    # environment variables. Pass the dashboard's actual resolved path explicitly
    # so the update always targets the same file the dashboard is reading, even
    # when DUCKDB_PATH is configured via Streamlit secrets rather than the env.
    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(_db_path())

    process = subprocess.Popen(
        cmd,
        cwd=str(PROJECT_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    for line in process.stdout:
        lines.append(line.rstrip("\n"))
        # Keep the visible log from growing unbounded in the browser.
        output_area.code("\n".join(lines[-200:]), language=None)
    process.wait()

    return process.returncode, "\n".join(lines)


def render_operations_page():
    st.markdown('<p style="font-size:2rem;font-weight:bold;">⚙️ Operations</p>', unsafe_allow_html=True)
    st.write(
        "Update the dataset after new sensor files arrive, and check whether the "
        "last update succeeded."
    )

    st.subheader("1. Add new sensor data")
    st.markdown(
        "Download the new CSV files from Seafile and place them in the matching "
        "folder under `data/raw/` (greenroof or parkplatz, by vendor) — see "
        "`instructions/ASSISTANT_RUNBOOK.md` for exact folder names. "
        "Then click **Run Update Now** below."
    )

    st.subheader("2. Current database")
    info = _describe_database()
    if not info["exists"]:
        st.warning(f"No database found yet at `{info['path']}`. Run an update to build it.")
    else:
        cols = st.columns(4)
        cols[0].metric("Size", f"{info['size_mb']:.0f} MB")
        cols[1].metric("Last updated", info["modified"].strftime("%Y-%m-%d %H:%M"))
        if "row_count" in info:
            cols[2].metric("Rows", f"{info['row_count']:,}")
            cols[3].metric("Latest data", str(info["max_ts"])[:10] if info["max_ts"] else "—")

            recent = _recent_rows(20)
            if recent is not None and not recent.empty:
                with st.expander("Preview the 20 most recent rows"):
                    st.dataframe(recent, use_container_width=True)
                    st.caption(
                        "For more — paging through everything or your own SQL — see the "
                        "🔍 Query Data and 📄 Browse Data views."
                    )
        elif "read_error" in info:
            st.error(f"Database file exists but could not be read: {info['read_error']}")

    st.subheader("3. Run update")
    st.caption(
        "This rebuilds the dataset into a staging file, verifies it, and only then "
        "replaces the live database — the dashboard keeps working the whole time, "
        "and a failed update never breaks it. This can take a few minutes."
    )
    if st.button("▶ Run Update Now", type="primary"):
        with st.spinner("Running pipeline update — this can take a few minutes..."):
            returncode, full_output = _run_update_streaming()
        if returncode == 0:
            st.success("Update completed and the live database was updated.")
            st.cache_resource.clear()
        else:
            st.error(
                f"Update failed (exit code {returncode}). The live database was NOT changed — "
                "the dashboard is still showing the previous data. See the output above for details."
            )

    st.subheader("4. Last run log")
    log_file = _latest_log_file()
    if log_file is None:
        st.caption("No pipeline logs yet.")
    else:
        st.caption(f"`{log_file.name}` — {datetime.fromtimestamp(log_file.stat().st_mtime).strftime('%Y-%m-%d %H:%M')}")
        with st.expander("Show log"):
            st.code(log_file.read_text(encoding="utf-8", errors="replace")[-20000:], language=None)
