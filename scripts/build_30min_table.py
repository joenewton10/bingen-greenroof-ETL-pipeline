"""
Build 30-Minute Aggregate Table
================================
Standalone script to create `synchronized_data_30min` from the minute-level
`synchronized_data_filtered` table.

Run this AFTER a full pipeline run (or at minimum after the sync stage).

Usage:
    python scripts/build_30min_table.py

Output table:
    synchronized_data_30min
        - Same columns as synchronized_data_filtered
        - Clock-aligned 30-minute buckets (:00 and :30)
        - All numeric metrics rounded to 3 decimal places
        - Extra column: minute_rows_in_bin (completeness indicator)
"""

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.aggregate.aggregate_30min import aggregate_30min


def main():
    print('=' * 60)
    print('BINGEN GREENROOF — 30-MINUTE AGGREGATE TABLE BUILDER')
    print('=' * 60)
    print(f'Source : synchronized_data_filtered  (minute-level)')
    print(f'Output : synchronized_data_30min     (30-min clock-aligned)')
    print()

    t_start = time.time()
    aggregate_30min()
    total = time.time() - t_start

    print()
    print(f'Completed in {total:.1f}s')
    print('=' * 60)


if __name__ == '__main__':
    main()
