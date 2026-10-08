"""
Pre-flight checks for pipeline: DB file accessibility, data folder checks.
"""
import logging
from pathlib import Path
from pipeline.ingest.base import get_connection, get_db_path


def run_preflight_checks(logger: logging.Logger, project_root: Path) -> bool:
    """
    Run all pre-flight checks before pipeline starts.

    Args:
        logger: Logger instance
        project_root: Root directory of project

    Returns:
        True if all checks pass, False otherwise
    """
    logger.info('\n' + '=' * 60)
    logger.info('PRE-FLIGHT CHECKS'.center(60))
    logger.info('=' * 60)

    checks_passed = 0
    checks_failed = 0

    # Check 1: DuckDB database file is reachable/creatable (no server, no credentials needed)
    try:
        db_path = get_db_path()
        conn = get_connection()
        version = conn.execute('SELECT version();').fetchone()[0]
        conn.close()
        logger.info(f'[OK] DuckDB database OK: {db_path} (duckdb {version})')
        checks_passed += 1
    except Exception as exc:
        logger.error(f'[ERROR] Could not open DuckDB database: {exc}')
        checks_failed += 1
        return False

    # Check 2: Data directories exist
    greenroof_dir = project_root / "data" / "raw" / "greenroof"
    parkplatz_dir = project_root / "data" / "raw" / "parkplatz"

    if greenroof_dir.exists():
        csv_count = len(list(greenroof_dir.rglob('*.csv')))
        logger.info(f'[OK] Greenroof data directory exists: {csv_count} CSV files')
        checks_passed += 1
    else:
        logger.error(f'[ERROR] Greenroof directory not found: {greenroof_dir}')
        checks_failed += 1

    if parkplatz_dir.exists():
        csv_count = len(list(parkplatz_dir.rglob('*.csv')))
        logger.info(f'[OK] Parkplatz data directory exists: {csv_count} CSV files')
        checks_passed += 1
    else:
        logger.error(f'[ERROR] Parkplatz directory not found: {parkplatz_dir}')
        checks_failed += 1

    # Black-globe data is an optional enrichment source (joined via LEFT JOIN in
    # sync_sites.py), so its absence is informational only and never fails the
    # pipeline the way a missing greenroof/parkplatz directory does.
    black_globes_dir = project_root / "data" / "raw" / "black_globes"
    if black_globes_dir.exists():
        dat_count = len(list(black_globes_dir.glob('*.dat')))
        logger.info(f'[OK] Black-globe data directory exists: {dat_count} .dat files (optional source)')
    else:
        logger.info('[INFO] Black-globe directory not found (optional data source, skipping)')

    # Check 3: Logs directory writable
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)
    try:
        test_file = logs_dir / ".write_test"
        test_file.write_text("test")
        test_file.unlink()
        logger.info(f'[OK] Logs directory writable: {logs_dir}')
        checks_passed += 1
    except Exception as exc:
        logger.error(f'[ERROR] Cannot write to logs directory: {exc}')
        checks_failed += 1
        return False

    # Summary
    logger.info('-' * 60)
    logger.info(f'Pre-flight checks: {checks_passed} passed, {checks_failed} failed')

    if checks_failed > 0:
        logger.error('Pre-flight checks FAILED. Please fix issues above.')
        return False

    logger.info('[OK] All pre-flight checks passed!\n')
    return True
