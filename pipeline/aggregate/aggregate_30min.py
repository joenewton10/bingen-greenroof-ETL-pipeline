"""
Aggregate 30-Minute Table
=========================
Builds `synchronized_data_30min` from the minute-level `synchronized_data_filtered`
table using clock-aligned 30-minute buckets (i.e. timestamps floor to :00 or :30).

All numeric metrics are rounded to 3 decimal places (including albedo).
Record-count columns are summed. Boolean/categorical availability columns
use deterministic majority rules per bucket.

Usage (standalone):
    python scripts/build_30min_table.py

Table naming convention:
- Source : synchronized_data_filtered
- Output : synchronized_data_30min
"""

import time
from pipeline.ingest.base import get_connection

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

AGGREGATE_30MIN_SQL = """
CREATE TABLE IF NOT EXISTS synchronized_data_30min AS
SELECT
    -- Clock-aligned 30-minute bucket timestamp
    date_trunc('hour', s.timestamp)
        + INTERVAL '30 min' * FLOOR(EXTRACT(MINUTE FROM s.timestamp) / 30) AS timestamp,

    -- ── Parkplatz sensor averages ──────────────────────────────────────────
    ROUND(AVG(s.avg_ir1_parkplatz),              3) AS avg_ir1_parkplatz,
    ROUND(AVG(s.avg_sr1_parkplatz),              3) AS avg_sr1_parkplatz,
    ROUND(AVG(s.avg_ir2_parkplatz),              3) AS avg_ir2_parkplatz,
    ROUND(AVG(s.avg_sr2_parkplatz),              3) AS avg_sr2_parkplatz,
    ROUND(AVG(s.avg_temp_parkplatz),             3) AS avg_temp_parkplatz,
    ROUND(AVG(s.avg_air_pressure_parkplatz),     3) AS avg_air_pressure_parkplatz,
    ROUND(AVG(s.avg_soil_moisture_parkplatz),    3) AS avg_soil_moisture_parkplatz,
    ROUND(AVG(s.avg_air_humidity_1_parkplatz),   3) AS avg_air_humidity_1_parkplatz,
    ROUND(AVG(s.avg_air_temp_1_parkplatz),       3) AS avg_air_temp_1_parkplatz,
    ROUND(AVG(s.avg_air_humidity_2_parkplatz),   3) AS avg_air_humidity_2_parkplatz,
    ROUND(AVG(s.avg_air_temp_2_parkplatz),       3) AS avg_air_temp_2_parkplatz,
    ROUND(AVG(s.avg_wind_speed_parkplatz),       3) AS avg_wind_speed_parkplatz,
    ROUND(AVG(s.avg_wind_direction_parkplatz),   3) AS avg_wind_direction_parkplatz,
    ROUND(AVG(s.avg_soil_temp_1_parkplatz),      3) AS avg_soil_temp_1_parkplatz,
    ROUND(AVG(s.avg_soil_temp_2_parkplatz),      3) AS avg_soil_temp_2_parkplatz,

    -- ── Greenroof sensor averages ──────────────────────────────────────────
    ROUND(AVG(s.avg_ir1_greenroof),              3) AS avg_ir1_greenroof,
    ROUND(AVG(s.avg_air_temperature_greenroof),  3) AS avg_air_temperature_greenroof,
    ROUND(AVG(s.avg_air_temp_2_greenroof),       3) AS avg_air_temp_2_greenroof,
    ROUND(AVG(s.avg_air_humidity_1_greenroof),   3) AS avg_air_humidity_1_greenroof,
    ROUND(AVG(s.avg_air_humidity_2_greenroof),   3) AS avg_air_humidity_2_greenroof,
    ROUND(AVG(s.avg_wind_speed_greenroof),       3) AS avg_wind_speed_greenroof,
    ROUND(AVG(s.avg_soil_temperature_greenroof), 3) AS avg_soil_temperature_greenroof,
    ROUND(AVG(s.avg_soil_moisture_greenroof),    3) AS avg_soil_moisture_greenroof,
    ROUND(AVG(s.avg_global_radiation_greenroof), 3) AS avg_global_radiation_greenroof,
    ROUND(AVG(s.avg_sr2_greenroof),              3) AS avg_sr2_greenroof,
    ROUND(AVG(s.avg_ir2_greenroof),              3) AS avg_ir2_greenroof,

    -- ── Record counts (summed across constituent minutes) ──────────────────
    SUM(s.parkplatz_record_count)                   AS parkplatz_record_count,
    SUM(s.greenroof_record_count)                   AS greenroof_record_count,
    COUNT(*)                                         AS minute_rows_in_bin,

    -- ── Dual-level availability ────────────────────────────────────────────
    -- True if ANY minute in the 30-min bin had dual-level data
    BOOL_OR(s.has_dual_level_greenroof)              AS has_dual_level_greenroof,
    CASE
        WHEN BOOL_OR(s.measurement_period = 'dual_level') THEN 'dual_level'
        ELSE 'single_level'
    END                                              AS measurement_period,

    -- ── Temperature difference metrics ────────────────────────────────────
    ROUND(AVG(s.temp_diff_1),        3) AS temp_diff_1,
    ROUND(AVG(s.temp_diff_2),        3) AS temp_diff_2,
    ROUND(AVG(s.delta_t_roof),       3) AS delta_t_roof,
    ROUND(AVG(s.delta_rh_roof),      3) AS delta_rh_roof,
    ROUND(AVG(s.delta_t_parkplatz),  3) AS delta_t_parkplatz,
    ROUND(AVG(s.delta_rh_parkplatz), 3) AS delta_rh_parkplatz,

    -- ── Energy / radiation metrics ────────────────────────────────────────
    ROUND(AVG(s.energy_from_air_parkplatz),     3) AS energy_from_air_parkplatz,
    ROUND(AVG(s.energy_from_surface_parkplatz), 3) AS energy_from_surface_parkplatz,
    ROUND(AVG(s.radiation_balance_greenroof),   3) AS radiation_balance_greenroof,
    ROUND(AVG(s.radiation_balance_parkplatz),   3) AS radiation_balance_parkplatz,

    -- ── Albedo (3 decimal places, consistent with all other metrics) ───────
    ROUND(AVG(s.albedo_greenroof),  3) AS albedo_greenroof,
    ROUND(AVG(s.albedo_parkplatz),  3) AS albedo_parkplatz

FROM synchronized_data_filtered s
GROUP BY
    date_trunc('hour', s.timestamp)
    + INTERVAL '30 min' * FLOOR(EXTRACT(MINUTE FROM s.timestamp) / 30)
ORDER BY 1 ASC;
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_sync_30min_timestamp
    ON synchronized_data_30min (timestamp);
"""


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def aggregate_30min():
    """Build synchronized_data_30min from synchronized_data_filtered."""
    conn = get_connection()

    print('[aggregate_30min] Dropping existing table...', flush=True)
    conn.execute('DROP TABLE IF EXISTS synchronized_data_30min;')

    print('[aggregate_30min] Building 30-minute aggregate table...', flush=True)
    t0 = time.time()
    conn.execute(AGGREGATE_30MIN_SQL)

    print('[aggregate_30min] Creating timestamp index...', flush=True)
    conn.execute(INDEX_SQL)

    elapsed = time.time() - t0

    # ── Summary ──────────────────────────────────────────────────────────────
    total_rows = conn.execute('SELECT COUNT(*) FROM synchronized_data_30min;').fetchone()[0]

    row = conn.execute("""
        SELECT
            MIN(timestamp)              AS earliest,
            MAX(timestamp)              AS latest,
            AVG(minute_rows_in_bin)     AS avg_minutes_per_bin,
            MIN(minute_rows_in_bin)     AS min_minutes_per_bin,
            MAX(minute_rows_in_bin)     AS max_minutes_per_bin
        FROM synchronized_data_30min;
    """).fetchone()

    print(f'\n[aggregate_30min] Done in {elapsed:.1f}s')
    print(f'  Rows in synchronized_data_30min : {total_rows:,}')
    if row:
        print(f'  Earliest bucket : {row[0]}')
        print(f'  Latest bucket   : {row[1]}')
        print(f'  Avg minutes/bin : {float(row[2]):.1f}')
        print(f'  Min minutes/bin : {row[3]}')
        print(f'  Max minutes/bin : {row[4]}')

    conn.close()
