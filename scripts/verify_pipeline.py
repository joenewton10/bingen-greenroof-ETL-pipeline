"""
Verify pipeline output - post-run QC-check script.

Run after pipeline completes to check:
- All tables exist and have data
- Timestamp ranges are reasonable
- Data freshness

Usage:
    python scripts/verify_pipeline.py                 # verify the live DB
    python scripts/verify_pipeline.py --db-path PATH  # verify a specific file
                                                        # (used to check a staging
                                                        # build before it's swapped in)
"""
import argparse
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import duckdb
from pipeline.ingest.base import get_db_path
from pipeline.utils.logger import setup_logger, log_section, log_success, log_warning, log_error


def get_timestamp_range(conn, table_name):
    """Get min/max timestamp from table."""
    try:
        return conn.execute(f'''
            SELECT
                MIN(timestamp) as min_ts,
                MAX(timestamp) as max_ts,
                COUNT(*) as total_count
            FROM {table_name}
            WHERE timestamp IS NOT NULL
        ''').fetchone()
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser(description='Verify pipeline output tables.')
    parser.add_argument(
        '--db-path',
        default=None,
        help='DuckDB file to verify (default: the configured live database).',
    )
    args = parser.parse_args()

    logger = setup_logger('verify_pipeline', verbose=False)

    log_section(logger, 'PIPELINE VERIFICATION')

    db_path = args.db_path or get_db_path()

    if not os.path.exists(db_path):
        log_error(logger, f'Database file not found: {db_path}')
        return 1

    try:
        conn = duckdb.connect(database=db_path, read_only=True)

        logger.info(f'\nDatabase: {db_path}')
        logger.info('\nChecking tables...\n')

        # Define all expected tables
        tables_to_check = {
            'Ingested': [
                'ingested_empower_greenroof',
                'ingested_kissel_greenroof',
                'ingested_parkplatz',
                'ingested_black_globes',
            ],
            'QC-filtered': [
                'qc_filtered_greenroof',
                'qc_filtered_parkplatz',
                'qc_filtered_black_globes',
            ],
            'Harmonized': [
                'harm_greenroof',
                'harm_parkplatz',
                'harm_black_globes',
            ],
            'Final': [
                'synchronized_data_filtered',
                'synchronized_data_yearly',
            ],
        }

        all_good = True
        total_rows = 0

        for category, tables in tables_to_check.items():
            logger.info(f'{category} Tables:')
            logger.info('-' * 40)

            for table_name in tables:
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                except Exception as exc:
                    logger.error(f"  ✗ {table_name}: {exc}")
                    all_good = False
                    continue

                if count == 0:
                    logger.warning(f"  ⚠ {table_name}: {count:,} rows (empty)")
                    all_good = False
                else:
                    log_success(logger, f'{table_name}: {count:,} rows')
                    total_rows += count

                    # Get timestamp range for final synchronized table
                    if table_name == 'synchronized_data_filtered':
                        ts_result = get_timestamp_range(conn, table_name)
                        if ts_result:
                            min_ts, max_ts, count = ts_result
                            logger.info(f'    Time range: {min_ts} to {max_ts}')

                            # Check data freshness
                            if max_ts:
                                now = datetime.now()
                                max_ts_dt = max_ts if isinstance(max_ts, datetime) else datetime.fromisoformat(str(max_ts))
                                age_days = (now - max_ts_dt).days

                                if age_days == 0:
                                    logger.info('    Freshness: TODAY [OK]')
                                elif age_days <= 7:
                                    logger.info(f'    Freshness: {age_days} days old [OK]')
                                else:
                                    log_warning(logger, f'Data is {age_days} days old')

            logger.info('')

        # Final summary
        conn.close()

        log_section(logger, 'VERIFICATION COMPLETE')

        if all_good:
            logger.info('\n[OK] All tables verified successfully')
            logger.info(f'[OK] Total rows across all tables: {total_rows:,}')
            logger.info(f'\nPipeline is ready for dashboard:')
            logger.info(f'  streamlit run dashboard/app.py')
            return 0
        else:
            log_error(logger, 'Some tables are missing or empty. Check logs above.')
            return 1

    except Exception as exc:
        log_error(logger, f'Verification failed: {exc}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
