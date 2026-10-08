"""
Bingen Greenroof Data Pipeline - Main Orchestrator (Enhanced)

Table naming convention for Bingen pipeline:
- Ingested: ingested_empower_greenroof, ingested_kissel_greenroof, ingested_parkplatz, ingested_black_globes
- QC-filtered: qc_filtered_greenroof, qc_filtered_parkplatz, qc_filtered_black_globes
- Harmonized: harm_greenroof, harm_parkplatz, harm_black_globes
- Final: synchronized_data_filtered

Pipeline stages:
1. INGEST: CSV files -> ingested_empower_greenroof, ingested_kissel_greenroof, ingested_parkplatz
2. QC FILTER: Apply sensor range filtering -> qc_filtered_* tables
3. HARMONIZE: Map to canonical columns -> harm_greenroof, harm_parkplatz
4. SYNC: Minute-level join with filters -> synchronized_data_filtered
5. PARTITION: Split synchronized_data_filtered into yearly tables -> synchronized_data_yearly

Usage:
    python run_pipeline.py [OPTIONS]

Options:
    --from STAGE       Start from stage (ingest, qc-filter, harmonize, sync, partition)
    --to STAGE         End at stage
    --dry-run          Print SQL without executing
    --verbose          Enable debug logging
    --yes, -y          Skip the confirmation prompt when targeting the live database directly
    --help             Show this help message

NOTE: running this script directly (no PIPELINE_DB_PATH set) writes straight
to the live database with no staging/verification safety net. Prefer
scripts/run_update.py (or "Run Update.bat") for normal use — this script is
meant for development/debugging (e.g. re-running a single stage). Running it
directly against the live DB prompts for confirmation unless --dry-run or
--yes is given.
"""
import os
import sys
import argparse
import time
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.ingest.base import get_db_path
from pipeline.ingest.ingest_greenroof import ingest_greenroof
from pipeline.ingest.ingest_parkplatz import ingest_parkplatz
from pipeline.ingest.ingest_black_globes import ingest_black_globes
from pipeline.qc_filtering.qc_filter_greenroof import qc_filter_greenroof
from pipeline.qc_filtering.qc_filter_parkplatz import qc_filter_parkplatz
from pipeline.qc_filtering.qc_filter_black_globes import qc_filter_black_globes
from pipeline.harmonize.harmonize_greenroof import harmonize_greenroof
from pipeline.harmonize.harmonize_parkplatz import harmonize_parkplatz
from pipeline.harmonize.harmonize_black_globes import harmonize_black_globes
from pipeline.sync.sync_sites import sync_data
from pipeline.partition.yearly_partitions import create_yearly_partitions
from pipeline.utils.logger import setup_logger, log_section, log_stage, log_success, log_error
from pipeline.utils.preflight import run_preflight_checks
from pipeline.utils.validator import validate_stage_output, get_table_stats


# Stage definitions with dependencies
STAGES = {
    'ingest': {'order': 1, 'name': 'INGESTION', 'func': None},
    'qc-filter': {'order': 2, 'name': 'QC FILTERING', 'func': None},
    'harmonize': {'order': 3, 'name': 'HARMONIZATION', 'func': None},
    'sync': {'order': 4, 'name': 'SYNCHRONIZATION', 'func': None},
    'partition': {'order': 5, 'name': 'YEARLY PARTITIONS', 'func': None},
}

LEGACY_STAGE_ALIASES = {
    'validate': 'qc-filter',
}


def run_ingest_stage(logger, dry_run=False):
    """STAGE 1: INGEST"""
    try:
        log_stage(logger, 1, 'INGESTION')
        
        logger.info('Ingesting Greenroof data (Empower + Kissel)...')
        if not dry_run:
            ingest_greenroof()
        log_success(logger, 'Greenroof ingest completed')
        
        logger.info('Ingesting Parkplatz data...')
        if not dry_run:
            ingest_parkplatz()
        log_success(logger, 'Parkplatz ingest completed')

        logger.info('Ingesting Black-globe data...')
        if not dry_run:
            ingest_black_globes()
        log_success(logger, 'Black-globe ingest completed')

        if not dry_run:
            validate_stage_output(logger, 'INGEST', [
                ('ingested_empower_greenroof', 100),
                ('ingested_kissel_greenroof', 100),
                ('ingested_parkplatz', 100),
                ('ingested_black_globes', 0),
            ])
        
        return True
    except Exception as exc:
        log_error(logger, f'Ingest stage failed: {exc}')
        return False


def run_qc_filter_stage(logger, dry_run=False):
    """STAGE 2: QC FILTER"""
    try:
        log_stage(logger, 2, 'QC FILTERING')
        
        logger.info('Applying QC filtering to Greenroof data...')
        if not dry_run:
            qc_filter_greenroof()
        log_success(logger, 'Greenroof QC filtering completed')
        
        logger.info('Applying QC filtering to Parkplatz data...')
        if not dry_run:
            qc_filter_parkplatz()
        log_success(logger, 'Parkplatz QC filtering completed')

        logger.info('Applying QC filtering to Black-globe data...')
        if not dry_run:
            qc_filter_black_globes()
        log_success(logger, 'Black-globe QC filtering completed')

        if not dry_run:
            validate_stage_output(logger, 'QC FILTERING', [
                ('qc_filtered_greenroof', 50),
                ('qc_filtered_parkplatz', 50),
                ('qc_filtered_black_globes', 0),
            ])
        
        return True
    except Exception as exc:
        log_error(logger, f'QC filtering stage failed: {exc}')
        return False


def run_harmonize_stage(logger, dry_run=False):
    """STAGE 3: HARMONIZE"""
    try:
        log_stage(logger, 3, 'HARMONIZATION')
        
        logger.info('Harmonizing Greenroof data (combining sources)...')
        if not dry_run:
            harmonize_greenroof()
        log_success(logger, 'Greenroof harmonization completed')
        
        logger.info('Harmonizing Parkplatz data...')
        if not dry_run:
            harmonize_parkplatz()
        log_success(logger, 'Parkplatz harmonization completed')

        logger.info('Harmonizing Black-globe data...')
        if not dry_run:
            harmonize_black_globes()
        log_success(logger, 'Black-globe harmonization completed')

        if not dry_run:
            validate_stage_output(logger, 'HARMONIZE', [
                ('harm_greenroof', 50),
                ('harm_parkplatz', 50),
                ('harm_black_globes', 0),
            ])
        
        return True
    except Exception as exc:
        log_error(logger, f'Harmonization stage failed: {exc}')
        return False


def run_sync_stage(logger, dry_run=False):
    """STAGE 4: SYNCHRONIZE"""
    try:
        log_stage(logger, 4, 'SYNCHRONIZATION')
        
        logger.info('Synchronizing Greenroof + Parkplatz per minute...')
        if not dry_run:
            sync_data()
        log_success(logger, 'Data synchronization completed')
        
        if not dry_run:
            # Get stats for final synchronized table
            stats = get_table_stats(logger, 'synchronized_data_filtered')
            if stats.get('row_count', 0) > 0:
                logger.info(f"  Rows: {stats['row_count']:,}")
                if stats.get('min_timestamp'):
                    logger.info(f"  Time range: {stats['min_timestamp']} to {stats['max_timestamp']}")
            
            validate_stage_output(logger, 'SYNC', [
                ('synchronized_data_filtered', 10),
            ])
        
        return True
    except Exception as exc:
        log_error(logger, f'Synchronization stage failed: {exc}')
        return False


def run_partition_stage(logger, dry_run=False):
    """STAGE 5: PARTITION"""
    try:
        log_stage(logger, 5, 'YEARLY PARTITIONS')
        
        logger.info('Building yearly tables...')
        if not dry_run:
            create_yearly_partitions()
        log_success(logger, 'Partitioning completed')

        return True
    except Exception as exc:
        log_error(logger, f'Partitioning stage failed: {exc}')
        return False


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Bingen Greenroof Data Pipeline - Main Orchestrator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python run_pipeline.py                    # Run all stages
  python run_pipeline.py --from harmonize   # Start from harmonize stage
  python run_pipeline.py --from harmonize --to sync    # Run harmonize and sync only
    python run_pipeline.py --from qc-filter --to sync    # Run QC filtering through sync
  python run_pipeline.py --dry-run          # Preview without executing
  python run_pipeline.py --verbose          # Enable debug logging
        '''
    )
    
    parser.add_argument(
        '--from',
        dest='from_stage',
        choices=['ingest', 'qc-filter', 'validate', 'harmonize', 'sync', 'partition'],
        default='ingest',
        help='Start from stage (default: ingest). validate is accepted as a legacy alias for qc-filter.'
    )
    
    parser.add_argument(
        '--to',
        dest='to_stage',
        choices=['ingest', 'qc-filter', 'validate', 'harmonize', 'sync', 'partition'],
        default='partition',
        help='End at stage (default: partition). validate is accepted as a legacy alias for qc-filter.'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Print what would happen without executing'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable debug-level logging'
    )

    parser.add_argument(
        '--yes', '-y',
        action='store_true',
        help='Skip the confirmation prompt when targeting the live database directly'
    )

    args = parser.parse_args()
    args.from_stage = LEGACY_STAGE_ALIASES.get(args.from_stage, args.from_stage)
    args.to_stage = LEGACY_STAGE_ALIASES.get(args.to_stage, args.to_stage)
    return args


def _warn_if_targeting_live_db(logger, dry_run, skip_confirmation):
    """Refuse to run directly against the live database without confirmation.

    scripts/run_update.py is the safe entry point: it builds into a staging
    file and only swaps it into place after verification. Running THIS
    script directly, with no PIPELINE_DB_PATH override, writes straight to
    the live file with no such safety net — a failed run partway through
    can leave it partially rebuilt and the dashboard broken. This only
    triggers when PIPELINE_DB_PATH isn't set (run_update.py always sets it,
    so it never sees this prompt) and real writes would happen (--dry-run
    is always safe and skips this too).
    """
    if os.getenv('PIPELINE_DB_PATH') or dry_run:
        return

    live_path = get_db_path()
    logger.warning('')
    logger.warning('=' * 60)
    logger.warning('WARNING: about to run directly against the LIVE database:')
    logger.warning(f'  {live_path}')
    logger.warning('This has no staging/verification safety net - a failed run')
    logger.warning('partway through can leave the live database broken.')
    logger.warning('Recommended: use "Run Update.bat" (scripts/run_update.py) instead.')
    logger.warning('=' * 60)

    if skip_confirmation:
        return

    try:
        answer = input('Continue anyway? [y/N]: ').strip().lower()
    except EOFError:
        answer = ''
    if answer != 'y':
        logger.info('Aborted.')
        sys.exit(1)


def validate_stage_range(from_stage, to_stage, logger):
    """Check that --from comes before --to."""
    from_order = STAGES[from_stage]['order']
    to_order = STAGES[to_stage]['order']
    
    if from_order > to_order:
        log_error(logger, f'Invalid stage range: --from {from_stage} comes after --to {to_stage}')
        return False
    
    return True


def main():
    """Main pipeline orchestrator."""
    args = parse_arguments()
    
    # Setup logging
    logger = setup_logger(__name__, verbose=args.verbose)

    _warn_if_targeting_live_db(logger, args.dry_run, args.yes)

    # Pre-flight checks
    if not run_preflight_checks(logger, PROJECT_ROOT):
        logger.critical('Pre-flight checks failed. Aborting pipeline.')
        sys.exit(1)
    
    # Log pipeline configuration
    log_section(logger, 'BINGEN GREENROOF PIPELINE START')
    logger.info(f'Start stage: {args.from_stage.upper()}')
    logger.info(f'End stage: {args.to_stage.upper()}')
    if args.dry_run:
        logger.info('Mode: DRY-RUN (no database changes)')
    if args.verbose:
        logger.info('Logging: VERBOSE (debug level)')
    logger.info('')
    
    # Check stage range
    if not validate_stage_range(args.from_stage, args.to_stage, logger):
        sys.exit(1)
    
    # Map stage names to functions
    stage_functions = {
        'ingest': run_ingest_stage,
        'qc-filter': run_qc_filter_stage,
        'harmonize': run_harmonize_stage,
        'sync': run_sync_stage,
        'partition': run_partition_stage,
    }
    
    # Execute stages in order
    start_time = time.time()
    stage_times = {}
    failed_stage = None
    
    for stage_name, stage_info in sorted(STAGES.items(), key=lambda x: x[1]['order']):
        from_order = STAGES[args.from_stage]['order']
        to_order = STAGES[args.to_stage]['order']
        stage_order = stage_info['order']
        
        # Skip if outside range
        if stage_order < from_order or stage_order > to_order:
            logger.info(f'\n[STAGE {stage_order}] {stage_info["name"]} — SKIPPED')
            continue
        
        # Run stage
        stage_start = time.time()
        stage_func = stage_functions[stage_name]
        success = stage_func(logger, dry_run=args.dry_run)
        stage_elapsed = time.time() - stage_start
        stage_times[stage_name] = stage_elapsed
        
        if not success:
            failed_stage = stage_name
            break
        
        logger.info(f'Stage {stage_order} completed in {stage_elapsed:.1f}s\n')
    
    # Final summary
    total_elapsed = time.time() - start_time
    
    if failed_stage:
        logger.error('')
        logger.error('=' * 60)
        logger.error('PIPELINE FAILED'.center(60))
        logger.error('=' * 60)
        logger.error(f'Failed at stage: {failed_stage.upper()}')
        logger.error(f'Total elapsed: {total_elapsed:.1f}s')
        logger.error('Check logs above for details.')
        sys.exit(1)
    else:
        log_section(logger, 'PIPELINE COMPLETED SUCCESSFULLY')
        
        logger.info('\nStage timings:')
        for stage_name, elapsed in stage_times.items():
            logger.info(f'  {stage_name.capitalize():<15}: {elapsed:7.1f}s')
        logger.info(f'  {"Total":<15}: {total_elapsed:7.1f}s')
        
        logger.info('\nOutput tables created:')
        logger.info('  Ingested:')
        logger.info('    - ingested_empower_greenroof (Empower source)')
        logger.info('    - ingested_kissel_greenroof (Kissel source)')
        logger.info('    - ingested_parkplatz')
        logger.info('    - ingested_black_globes')
        logger.info('  QC-filtered:')
        logger.info('    - qc_filtered_greenroof')
        logger.info('    - qc_filtered_parkplatz')
        logger.info('    - qc_filtered_black_globes')
        logger.info('  Harmonized:')
        logger.info('    - harm_greenroof (12 canonical columns)')
        logger.info('    - harm_parkplatz')
        logger.info('    - harm_black_globes')
        logger.info('  Final:')
        logger.info('    - synchronized_data_filtered (54 columns, minute-level)')
        logger.info('    - synchronized_data_yearly (partitioned by year)')
        
        logger.info(f'\nLog file: {PROJECT_ROOT}/logs/ (auto-rotated, last 5 runs retained)')
        logger.info('\n[OK] Ready for dashboard: streamlit run dashboard/app.py')
        
        sys.exit(0)


if __name__ == '__main__':
    main()


