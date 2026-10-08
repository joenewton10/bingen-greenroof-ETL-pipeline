"""
Launch the Streamlit dashboard from a single Python entry point.

Usage:
    python scripts/run_dashboard.py
    python scripts/run_dashboard.py -- --server.port 8502
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the Streamlit dashboard app.",
        epilog="Pass extra Streamlit args after '--', e.g. -- --server.port 8502",
    )
    parser.add_argument(
        "streamlit_args",
        nargs=argparse.REMAINDER,
        help="Additional arguments forwarded to Streamlit.",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    app_path = project_root / "dashboard" / "app.py"

    if not app_path.exists():
        print(f"ERROR: dashboard app not found at {app_path}")
        return 1

    forwarded_args = args.streamlit_args
    if forwarded_args and forwarded_args[0] == "--":
        forwarded_args = forwarded_args[1:]

    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        *forwarded_args,
    ]

    result = subprocess.run(cmd, cwd=project_root)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
