"""
One-command update runner for the Bingen pipeline.

Builds the full pipeline into a STAGING DuckDB file, verifies it, and only
then atomically swaps it into place as the live database. If anything fails
along the way, the live file is left completely untouched — the dashboard
keeps serving the last known-good data for the whole duration of the run,
and a bad run never corrupts it.

Safe to run anytime — there is no required schedule. Thanks to incremental
ingest, a run with no new/changed raw files completes in seconds; a run with
new files only processes those.

Default flow:
0) seed the staging file with a copy of the live database (so incremental
   ingest can see what's already been ingested and only process new/changed
   raw files)
1) run_pipeline.py --dry-run --verbose
2) run_pipeline.py
3) verify_pipeline.py --db-path <staging file>
4) build_30min_table.py
5) atomic swap: staging file -> live file

Usage:
    python scripts/run_update.py

Optional flags:
    --skip-dry-run        Skip the dry-run precheck
    --skip-30min          Skip 30-minute table build
    --continue-on-30min-error
                          Continue even if 30-minute build fails
"""

from __future__ import annotations

import argparse
import datetime
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import DUCKDB_PATH


def _acquire_lock(lock_path: Path) -> bool:
    """Atomically create the lock file; return False if one already exists.

    Guards against two updates (e.g. a scheduled Task Scheduler run and a
    manual "Run Update Now" click) racing to build the same staging file at
    once. Without this, the second one would eventually hit a raw DuckDB
    file-lock error deep inside a pipeline stage instead of a clear message.
    """
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w") as f:
            f.write(datetime.datetime.now().isoformat())
        return True
    except FileExistsError:
        return False


def _run_step(label: str, cmd: list[str], env: dict) -> float:
    """Run one pipeline step and return elapsed seconds."""
    print("\n" + "=" * 72)
    print(f"STEP: {label}")
    print(f"CMD : {' '.join(cmd)}")
    print("=" * 72)

    t0 = time.time()
    completed = subprocess.run(cmd, cwd=str(PROJECT_ROOT), env=env, check=False)
    elapsed = time.time() - t0

    if completed.returncode != 0:
        raise RuntimeError(f"Step failed ({label}) with exit code {completed.returncode}")

    print(f"[OK] {label} completed in {elapsed:.1f}s")
    return elapsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run full pipeline update: build into a staging file, verify, then swap live."
    )
    parser.add_argument(
        "--skip-dry-run",
        action="store_true",
        help="Skip dry-run precheck",
    )
    parser.add_argument(
        "--skip-30min",
        action="store_true",
        help="Skip 30-minute aggregate table build",
    )
    parser.add_argument(
        "--continue-on-30min-error",
        action="store_true",
        help="Do not fail the run if 30-minute table build fails",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    python_exe = sys.executable

    live_path = Path(DUCKDB_PATH)
    staging_path = live_path.with_suffix(live_path.suffix + ".new")
    lock_path = live_path.parent / ".update.lock"

    print("=" * 72)
    print("BINGEN GREENROOF - PIPELINE UPDATE RUNNER")
    print("=" * 72)
    print(f"Project root  : {PROJECT_ROOT}")
    print(f"Python        : {python_exe}")
    print(f"Live database : {live_path}")
    print(f"Staging build : {staging_path}")
    print("(the live database is left untouched until the staging build verifies clean)")

    if not _acquire_lock(lock_path):
        started = "an unknown time"
        try:
            started = lock_path.read_text().strip()
        except OSError:
            pass
        print(
            f"\n[ERROR] An update already appears to be running (started {started}).\n"
            f"If you're sure nothing is actually running — e.g. a previous run crashed "
            f"without cleaning up — delete this file and try again:\n  {lock_path}"
        )
        return 1

    try:
        return _run_update(args, python_exe, live_path, staging_path)
    finally:
        try:
            lock_path.unlink()
        except OSError:
            pass


def _run_update(args: argparse.Namespace, python_exe: str, live_path: Path, staging_path: Path) -> int:
    # Seed staging from the live database so incremental ingest (which compares
    # each raw file's size/mtime against what's already recorded in the DB) has
    # something to compare against. Without this, staging starts empty every
    # run and every file looks "new", silently regressing back to a full
    # reparse every time. This unconditionally overwrites any stale leftover
    # .new from a prior crashed run — a half-built staging file isn't a valid
    # base to resume from, so every run starts fresh from the known-good live
    # snapshot instead.
    if live_path.exists():
        print(f"\n[SETUP] Seeding staging file from live database...")
        print(f"        {live_path} -> {staging_path}")
        t0 = time.time()
        shutil.copy2(live_path, staging_path)
        print(f"[OK] Staging file seeded in {time.time() - t0:.1f}s")
    else:
        print(f"\n[SETUP] No live database yet at {live_path} — staging starts empty (first-ever run).")

    env = os.environ.copy()
    env["PIPELINE_DB_PATH"] = str(staging_path)

    timings: list[tuple[str, float]] = []

    try:
        if not args.skip_dry_run:
            timings.append((
                "dry-run",
                _run_step(
                    "Pipeline dry-run",
                    [python_exe, "scripts/run_pipeline.py", "--dry-run", "--verbose"],
                    env,
                ),
            ))

        timings.append((
            "pipeline",
            _run_step(
                "Full pipeline (building staging file)",
                [python_exe, "scripts/run_pipeline.py"],
                env,
            ),
        ))

        timings.append((
            "verify",
            _run_step(
                "Pipeline verification (staging file)",
                [python_exe, "scripts/verify_pipeline.py", "--db-path", str(staging_path)],
                env,
            ),
        ))

        if not args.skip_30min:
            try:
                timings.append((
                    "30min",
                    _run_step(
                        "Build 30-minute table (staging file)",
                        [python_exe, "scripts/build_30min_table.py"],
                        env,
                    ),
                ))
            except Exception as exc:
                if args.continue_on_30min_error:
                    print(f"[WARN] 30-minute build failed but continuing: {exc}")
                else:
                    raise

    except Exception as exc:
        print(f"\n[ERROR] Update failed before the staging build could be verified: {exc}")
        print(f"[ERROR] Live database was NOT touched: {live_path}")
        print(f"Tip: inspect latest logs in logs/, and the staging file at {staging_path}, then rerun.")
        return 1

    # Staging build verified clean — swap it into place as the live database.
    # A dashboard query can transiently hold the live file open on Windows, so
    # retry briefly before giving up — most contention clears within seconds.
    print("\n" + "=" * 72)
    print("STEP: Swapping staging build into place as the live database")
    print("=" * 72)
    swap_attempts = 5
    for attempt in range(1, swap_attempts + 1):
        try:
            os.replace(staging_path, live_path)
            break
        except OSError as exc:
            if attempt == swap_attempts:
                print(f"[ERROR] Could not swap staging file into place: {exc}")
                print("Tip: close the dashboard (it may be holding the database file open) and rerun.")
                print(f"The verified staging build is still available at: {staging_path}")
                return 1
            print(f"[WARN] Live database is in use, retrying swap ({attempt}/{swap_attempts})...")
            time.sleep(3)

    print(f"[OK] Live database updated: {live_path}")

    total = sum(sec for _, sec in timings)
    print("\n" + "=" * 72)
    print("UPDATE COMPLETED")
    print("=" * 72)
    for name, sec in timings:
        print(f"{name:10s}: {sec:8.1f}s")
    print(f"total     : {total:8.1f}s")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
