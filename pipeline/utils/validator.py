"""
Data quality checks: row count checks and quality assessment per stage.
"""
import logging
from pipeline.ingest.base import get_connection


def validate_stage_output(logger: logging.Logger, stage_name: str, table_names: list) -> bool:
    """
    Check that output tables exist and have reasonable row counts.

    Args:
        logger: Logger instance
        stage_name: Name of stage (e.g., "INGEST", "QC FILTERING")
        table_names: List of (table_name, expected_min_rows) tuples

    Returns:
        True if all checks pass
    """
    logger.info(f'\nValidating {stage_name} output tables...')

    try:
        conn = get_connection()

        all_valid = True

        for table_name, expected_min in table_names:
            try:
                row_count = conn.execute(f'SELECT COUNT(*) as cnt FROM {table_name}').fetchone()[0]

                if row_count >= expected_min:
                    logger.info(f'  [OK] {table_name}: {row_count:,} rows')
                else:
                    logger.warning(
                        f'  ⚠ {table_name}: {row_count:,} rows (expected ≥ {expected_min:,})'
                    )
                    all_valid = False

            except Exception as exc:
                logger.error(f'  [ERROR] {table_name}: {exc}')
                all_valid = False

        conn.close()

        return all_valid

    except Exception as exc:
        logger.error(f'QC check error: {exc}')
        return False


def get_table_stats(logger: logging.Logger, table_name: str) -> dict:
    """
    Get basic statistics for a table (row count, timestamp range, NULL %).

    Args:
        logger: Logger instance
        table_name: Name of table to inspect

    Returns:
        Dictionary with stats or empty dict if error
    """
    try:
        conn = get_connection()

        # Row count
        row_count = conn.execute(f'SELECT COUNT(*) FROM {table_name}').fetchone()[0]

        # Timestamp range
        result = conn.execute(f'''
            SELECT
                MIN(timestamp) as min_ts,
                MAX(timestamp) as max_ts
            FROM {table_name}
            WHERE timestamp IS NOT NULL
        ''').fetchone()
        min_ts, max_ts = result if result else (None, None)

        conn.close()

        return {
            'row_count': row_count,
            'min_timestamp': min_ts,
            'max_timestamp': max_ts
        }

    except Exception as exc:
        logger.error(f'Error getting stats for {table_name}: {exc}')
        return {}


def validate_data_quality(logger: logging.Logger, table_name: str,
                         required_columns: list = None) -> bool:
    """
    Quick data quality check: verify required columns exist.

    Args:
        logger: Logger instance
        table_name: Name of table
        required_columns: List of column names to verify

    Returns:
        True if all checks pass
    """
    if not required_columns:
        return True

    try:
        conn = get_connection()

        rows = conn.execute('''
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ?
        ''', (table_name,)).fetchall()

        existing_columns = {row[0] for row in rows}
        conn.close()

        missing = set(required_columns) - existing_columns

        if missing:
            logger.error(f'[ERROR] {table_name} missing columns: {missing}')
            return False

        logger.info(f'[OK] {table_name} has all required columns')
        return True

    except Exception as exc:
        logger.error(f'Error validating columns in {table_name}: {exc}')
        return False
