'''
Sync Sites - Synchronize greenroof and parkplatz data per minute.

Based on: synchronize+filter_data_code.py

Table naming convention for Bingen pipeline:
- Parkplatz source: harm_parkplatz
- Greenroof source: harm_greenroof
- Black-globe source: harm_black_globes (LEFT JOIN — optional/enrichment, never gates a minute)
- Output: synchronized_data_filtered

Final output table: synchronized_data_filtered
- Includes dual-level availability flags and thesis-derived metrics
- Minute-level aggregation with AVG and ROUND(..., 3)
- HAVING filters for outlier removal

Casts use ::DOUBLE rather than ::NUMERIC: these are floating-point physical
sensor measurements, not exact-decimal data, and DuckDB's bare NUMERIC/DECIMAL
defaults to DECIMAL(18,3) — which would silently round every value to 3 decimal
places *before* averaging. DOUBLE preserves full precision through AVG(), same
as PostgreSQL's arbitrary-precision NUMERIC did.
'''
import time
from pipeline.ingest.base import get_connection

# Synchronized data: minute-level aggregation matching reference exactly
# Output columns match synchronize+filter_data_code.py
SYNC_SQL = '''
CREATE TABLE IF NOT EXISTS synchronized_data_filtered AS
SELECT
    date_trunc('minute', g.timestamp) AS timestamp,

    -- Parkplatz sensor averages (15 columns)
    ROUND(AVG(p."IR1 [W/m2]"::DOUBLE), 3) AS avg_ir1_parkplatz,
    ROUND(AVG(p."SR1 [W/m2]"::DOUBLE), 3) AS avg_sr1_parkplatz,
    ROUND(AVG(p."IR2 [W/m2]"::DOUBLE), 3) AS avg_ir2_parkplatz,
    ROUND(AVG(p."SR2 [W/m2]"::DOUBLE), 3) AS avg_sr2_parkplatz,
    ROUND(AVG(p."Temp [C]"::DOUBLE), 3) AS avg_temp_parkplatz,
    ROUND(AVG(p."Air Pressure [mbar]"::DOUBLE), 3) AS avg_air_pressure_parkplatz,
    ROUND(AVG(p."Soil Moisture [vol%]"::DOUBLE), 3) AS avg_soil_moisture_parkplatz,
    ROUND(AVG(p."Air Humidity 1 [%RH]"::DOUBLE), 3) AS avg_air_humidity_1_parkplatz,
    ROUND(AVG(p."Air Temp 1 [C]"::DOUBLE), 3) AS avg_air_temp_1_parkplatz,
    ROUND(AVG(p."Air Humidity 2 [%RH]"::DOUBLE), 3) AS avg_air_humidity_2_parkplatz,
    ROUND(AVG(p."Air Temp 2 [C]"::DOUBLE), 3) AS avg_air_temp_2_parkplatz,
    ROUND(AVG(p."Wind Speed [m/s]"::DOUBLE), 3) AS avg_wind_speed_parkplatz,
    ROUND(AVG(p."Wind Direction [deg]"::DOUBLE), 3) AS avg_wind_direction_parkplatz,
    ROUND(AVG(p."Soil Temp 1 [C]"::DOUBLE), 3) AS avg_soil_temp_1_parkplatz,
    ROUND(AVG(p."Soil Temp 2 [C]"::DOUBLE), 3) AS avg_soil_temp_2_parkplatz,

    -- Greenroof sensor averages
    ROUND(AVG(g.ir1_in_lw::DOUBLE), 3) AS avg_ir1_greenroof,
    ROUND(AVG(g.air_temperature::DOUBLE), 3) AS avg_air_temperature_greenroof,
    ROUND(AVG(g.air_temp_2::DOUBLE), 3) AS avg_air_temp_2_greenroof,
    ROUND(AVG(g.relative_humidity::DOUBLE), 3) AS avg_air_humidity_1_greenroof,
    ROUND(AVG(g.air_humidity_2::DOUBLE), 3) AS avg_air_humidity_2_greenroof,
    ROUND(AVG(g.wind_speed_avg::DOUBLE), 3) AS avg_wind_speed_greenroof,
    ROUND(AVG(g.soil_temperature::DOUBLE), 3) AS avg_soil_temperature_greenroof,
    ROUND(AVG(g.soil_moisture::DOUBLE), 3) AS avg_soil_moisture_greenroof,
    ROUND(AVG(g.sr1_in_sw::DOUBLE), 3) AS avg_global_radiation_greenroof,
    ROUND(AVG(g.sr2_ref_sw::DOUBLE), 3) AS avg_sr2_greenroof,
    ROUND(AVG(g.ir2_out_lw::DOUBLE), 3) AS avg_ir2_greenroof,

    -- Black-globe sensor averages (NULL before the Aug 2026 deployment, or for any
    -- minute the logger didn't report; co-located at greenroof/parkplatz plus two
    -- new standalone plots, MoBiGa Nord/Sued)
    ROUND(AVG(b.globe_temp_gruendach::DOUBLE), 3) AS avg_globe_temp_gruendach,
    ROUND(AVG(b.globe_temp_parkplatz::DOUBLE), 3) AS avg_globe_temp_parkplatz,
    ROUND(AVG(b.globe_temp_mobiga_nord::DOUBLE), 3) AS avg_globe_temp_mobiga_nord,
    ROUND(AVG(b.globe_temp_mobiga_sued::DOUBLE), 3) AS avg_globe_temp_mobiga_sued,

    -- Record counts for quality assessment
    COUNT(p.id) AS parkplatz_record_count,
    COUNT(g.id) AS greenroof_record_count,
    COUNT(b.id) AS black_globes_record_count,

    -- Data availability: dual-level instrumentation on greenroof
    (AVG(g.air_temp_2::DOUBLE) IS NOT NULL AND AVG(g.air_humidity_2::DOUBLE) IS NOT NULL) AS has_dual_level_greenroof,
    CASE
        WHEN AVG(g.air_temp_2::DOUBLE) IS NOT NULL AND AVG(g.air_humidity_2::DOUBLE) IS NOT NULL THEN 'dual_level'
        ELSE 'single_level'
    END AS measurement_period,

    -- Computed columns: Temperature differences (cooling effect indicators)
    ROUND(AVG(g.air_temperature::DOUBLE) - AVG(p."Air Temp 1 [C]"::DOUBLE), 3) AS temp_diff_1,
    ROUND(AVG(g.air_temperature::DOUBLE) - AVG(p."Air Temp 2 [C]"::DOUBLE), 3) AS temp_diff_2,
    ROUND(AVG(g.air_temperature::DOUBLE) - AVG(g.air_temp_2::DOUBLE), 3) AS delta_t_roof,
    ROUND(AVG(g.relative_humidity::DOUBLE) - AVG(g.air_humidity_2::DOUBLE), 3) AS delta_rh_roof,
    ROUND(AVG(p."Air Temp 1 [C]"::DOUBLE) - AVG(p."Air Temp 2 [C]"::DOUBLE), 3) AS delta_t_parkplatz,
    ROUND(AVG(p."Air Humidity 1 [%RH]"::DOUBLE) - AVG(p."Air Humidity 2 [%RH]"::DOUBLE), 3) AS delta_rh_parkplatz,

    -- Computed columns: Energy calculations (Stefan-Boltzmann: σ=5.67e-8, ε=0.95)
    ROUND(AVG(p."IR1 [W/m2]"::DOUBLE) + POWER(AVG(p."Temp [C]"::DOUBLE) + 273.15, 4) * 5.67e-8 * 0.95, 3) AS energy_from_air_parkplatz,
    ROUND(AVG(p."IR2 [W/m2]"::DOUBLE) + POWER(AVG(p."Temp [C]"::DOUBLE) + 273.15, 4) * 5.67e-8 * 0.95, 3) AS energy_from_surface_parkplatz,

    -- Computed columns: Radiation balance and albedo
    ROUND(AVG(g.sr1_in_sw::DOUBLE) - AVG(g.sr2_ref_sw::DOUBLE) + AVG(g.ir1_in_lw::DOUBLE) - AVG(g.ir2_out_lw::DOUBLE), 3) AS radiation_balance_greenroof,
    ROUND(AVG(p."SR1 [W/m2]"::DOUBLE) - AVG(p."SR2 [W/m2]"::DOUBLE) + AVG(p."IR1 [W/m2]"::DOUBLE) - AVG(p."IR2 [W/m2]"::DOUBLE), 3) AS radiation_balance_parkplatz,
    ROUND(
        AVG(g.sr2_ref_sw::DOUBLE)
        / NULLIF(CASE WHEN AVG(g.sr1_in_sw::DOUBLE) >= 10 THEN AVG(g.sr1_in_sw::DOUBLE) END, 0),
        4
    ) AS albedo_greenroof,
    ROUND(
        AVG(p."SR2 [W/m2]"::DOUBLE)
        / NULLIF(CASE WHEN AVG(p."SR1 [W/m2]"::DOUBLE) >= 10 THEN AVG(p."SR1 [W/m2]"::DOUBLE) END, 0),
        4
    ) AS albedo_parkplatz,

    -- Computed columns: specific humidity (Magnus over water, Bingen pressure p = 1002 hPa, q in g/kg)
    -- e_s(T) = 6.112 * exp(17.62 * T / (243.12 + T))  [hPa]
    -- e      = (RH/100) * e_s
    -- q      = 0.622 * e / (p - 0.378 * e)  [kg/kg], multiplied by 1000 -> g/kg
    -- Reference: WMO No. 8 (Annex 4.B); Alduchov & Eskridge (1996); Allen et al. (1998) FAO-56
    ROUND(
        (0.622 * (AVG(g.relative_humidity::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(g.air_temperature::DOUBLE) / (243.12 + AVG(g.air_temperature::DOUBLE))))
        / (1002.0 - 0.378 * (AVG(g.relative_humidity::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(g.air_temperature::DOUBLE) / (243.12 + AVG(g.air_temperature::DOUBLE))))
        * 1000.0,
        4
    ) AS q_greenroof_50cm,
    ROUND(
        (0.622 * (AVG(g.air_humidity_2::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(g.air_temp_2::DOUBLE) / (243.12 + AVG(g.air_temp_2::DOUBLE))))
        / (1002.0 - 0.378 * (AVG(g.air_humidity_2::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(g.air_temp_2::DOUBLE) / (243.12 + AVG(g.air_temp_2::DOUBLE))))
        * 1000.0,
        4
    ) AS q_greenroof_2m,
    ROUND(
        (0.622 * (AVG(p."Air Humidity 1 [%RH]"::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(p."Air Temp 1 [C]"::DOUBLE) / (243.12 + AVG(p."Air Temp 1 [C]"::DOUBLE))))
        / (1002.0 - 0.378 * (AVG(p."Air Humidity 1 [%RH]"::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(p."Air Temp 1 [C]"::DOUBLE) / (243.12 + AVG(p."Air Temp 1 [C]"::DOUBLE))))
        * 1000.0,
        4
    ) AS q_parkplatz_50cm,
    ROUND(
        (0.622 * (AVG(p."Air Humidity 2 [%RH]"::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(p."Air Temp 2 [C]"::DOUBLE) / (243.12 + AVG(p."Air Temp 2 [C]"::DOUBLE))))
        / (1002.0 - 0.378 * (AVG(p."Air Humidity 2 [%RH]"::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(p."Air Temp 2 [C]"::DOUBLE) / (243.12 + AVG(p."Air Temp 2 [C]"::DOUBLE))))
        * 1000.0,
        4
    ) AS q_parkplatz_2m,
    -- Specific-humidity gradient delta_q = q_50cm - q_2m  (g/kg); positive = moister near surface
    -- Roof delta_q is only physically meaningful when has_dual_level_greenroof = TRUE (Jan 2024 onwards)
    ROUND(
        ((0.622 * (AVG(g.relative_humidity::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(g.air_temperature::DOUBLE) / (243.12 + AVG(g.air_temperature::DOUBLE))))
         / (1002.0 - 0.378 * (AVG(g.relative_humidity::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(g.air_temperature::DOUBLE) / (243.12 + AVG(g.air_temperature::DOUBLE))))
         * 1000.0)
        -
        ((0.622 * (AVG(g.air_humidity_2::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(g.air_temp_2::DOUBLE) / (243.12 + AVG(g.air_temp_2::DOUBLE))))
         / (1002.0 - 0.378 * (AVG(g.air_humidity_2::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(g.air_temp_2::DOUBLE) / (243.12 + AVG(g.air_temp_2::DOUBLE))))
         * 1000.0),
        4
    ) AS delta_q_roof,
    ROUND(
        ((0.622 * (AVG(p."Air Humidity 1 [%RH]"::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(p."Air Temp 1 [C]"::DOUBLE) / (243.12 + AVG(p."Air Temp 1 [C]"::DOUBLE))))
         / (1002.0 - 0.378 * (AVG(p."Air Humidity 1 [%RH]"::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(p."Air Temp 1 [C]"::DOUBLE) / (243.12 + AVG(p."Air Temp 1 [C]"::DOUBLE))))
         * 1000.0)
        -
        ((0.622 * (AVG(p."Air Humidity 2 [%RH]"::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(p."Air Temp 2 [C]"::DOUBLE) / (243.12 + AVG(p."Air Temp 2 [C]"::DOUBLE))))
         / (1002.0 - 0.378 * (AVG(p."Air Humidity 2 [%RH]"::DOUBLE)/100.0)
            * 6.112 * exp(17.62 * AVG(p."Air Temp 2 [C]"::DOUBLE) / (243.12 + AVG(p."Air Temp 2 [C]"::DOUBLE))))
         * 1000.0),
        4
    ) AS delta_q_parkplatz

FROM harm_greenroof g
INNER JOIN harm_parkplatz p
    ON date_trunc('minute', g.timestamp) = date_trunc('minute', p.timestamp)
LEFT JOIN harm_black_globes b
    ON date_trunc('minute', g.timestamp) = date_trunc('minute', b.timestamp)

GROUP BY date_trunc('minute', g.timestamp)

-- INTEGRATED FILTERING LOGIC from synchronize+filter_data_code.py
HAVING
    -- Temperature filters (custom ranges)
    (ROUND(AVG(g.air_temperature::DOUBLE), 3) IS NULL OR (ROUND(AVG(g.air_temperature::DOUBLE), 3) >= -25.0 AND ROUND(AVG(g.air_temperature::DOUBLE), 3) <= 45.0))
    AND (ROUND(AVG(p."Air Temp 1 [C]"::DOUBLE), 3) IS NULL OR (ROUND(AVG(p."Air Temp 1 [C]"::DOUBLE), 3) >= -25.0 AND ROUND(AVG(p."Air Temp 1 [C]"::DOUBLE), 3) <= 45.0))
    AND (ROUND(AVG(p."Air Temp 2 [C]"::DOUBLE), 3) IS NULL OR (ROUND(AVG(p."Air Temp 2 [C]"::DOUBLE), 3) >= -25.0 AND ROUND(AVG(p."Air Temp 2 [C]"::DOUBLE), 3) <= 45.0))
    AND (ROUND(AVG(p."Temp [C]"::DOUBLE), 3) IS NULL OR (ROUND(AVG(p."Temp [C]"::DOUBLE), 3) >= -25.0 AND ROUND(AVG(p."Temp [C]"::DOUBLE), 3) <= 45.0))
    AND (ROUND(AVG(g.soil_temperature::DOUBLE), 3) IS NULL OR (ROUND(AVG(g.soil_temperature::DOUBLE), 3) >= -20.0 AND ROUND(AVG(g.soil_temperature::DOUBLE), 3) <= 70.0))
    AND (ROUND(AVG(p."Soil Temp 1 [C]"::DOUBLE), 3) IS NULL OR (ROUND(AVG(p."Soil Temp 1 [C]"::DOUBLE), 3) >= -20.0 AND ROUND(AVG(p."Soil Temp 1 [C]"::DOUBLE), 3) <= 70.0))
    AND (ROUND(AVG(p."Soil Temp 2 [C]"::DOUBLE), 3) IS NULL OR (ROUND(AVG(p."Soil Temp 2 [C]"::DOUBLE), 3) >= -20.0 AND ROUND(AVG(p."Soil Temp 2 [C]"::DOUBLE), 3) <= 70.0))

    -- Humidity filters (custom ranges)
    AND (ROUND(AVG(g.relative_humidity::DOUBLE), 3) IS NULL OR (ROUND(AVG(g.relative_humidity::DOUBLE), 3) >= 0.0 AND ROUND(AVG(g.relative_humidity::DOUBLE), 3) <= 100.0))
    AND (ROUND(AVG(p."Air Humidity 1 [%RH]"::DOUBLE), 3) IS NULL OR (ROUND(AVG(p."Air Humidity 1 [%RH]"::DOUBLE), 3) >= 0.0 AND ROUND(AVG(p."Air Humidity 1 [%RH]"::DOUBLE), 3) <= 100.0))
    AND (ROUND(AVG(p."Air Humidity 2 [%RH]"::DOUBLE), 3) IS NULL OR (ROUND(AVG(p."Air Humidity 2 [%RH]"::DOUBLE), 3) >= 0.0 AND ROUND(AVG(p."Air Humidity 2 [%RH]"::DOUBLE), 3) <= 100.0))

    -- Wind speed filters (custom ranges)
    AND (ROUND(AVG(g.wind_speed_avg::DOUBLE), 3) IS NULL OR (ROUND(AVG(g.wind_speed_avg::DOUBLE), 3) >= 0.0 AND ROUND(AVG(g.wind_speed_avg::DOUBLE), 3) <= 50.0))
    AND (ROUND(AVG(p."Wind Speed [m/s]"::DOUBLE), 3) IS NULL OR (ROUND(AVG(p."Wind Speed [m/s]"::DOUBLE), 3) >= 0.0 AND ROUND(AVG(p."Wind Speed [m/s]"::DOUBLE), 3) <= 40.0))

    -- Soil moisture filters (custom ranges)
    AND (ROUND(AVG(g.soil_moisture::DOUBLE), 3) IS NULL OR (ROUND(AVG(g.soil_moisture::DOUBLE), 3) >= 0.0 AND ROUND(AVG(g.soil_moisture::DOUBLE), 3) <= 60.0))
    AND (ROUND(AVG(p."Soil Moisture [vol%]"::DOUBLE), 3) IS NULL OR (ROUND(AVG(p."Soil Moisture [vol%]"::DOUBLE), 3) >= 0.0 AND ROUND(AVG(p."Soil Moisture [vol%]"::DOUBLE), 3) <= 60.0))

    -- Pressure filter (custom range)
    AND (ROUND(AVG(p."Air Pressure [mbar]"::DOUBLE), 3) IS NULL OR (ROUND(AVG(p."Air Pressure [mbar]"::DOUBLE), 3) >= 700.0 AND ROUND(AVG(p."Air Pressure [mbar]"::DOUBLE), 3) <= 1500.0))

    -- Black-globe temperature filters (custom ranges; NULL always passes, so a
    -- missing reading never drops an otherwise-good greenroof/parkplatz minute)
    AND (ROUND(AVG(b.globe_temp_gruendach::DOUBLE), 3) IS NULL OR (ROUND(AVG(b.globe_temp_gruendach::DOUBLE), 3) BETWEEN -30.0 AND 90.0))
    AND (ROUND(AVG(b.globe_temp_parkplatz::DOUBLE), 3) IS NULL OR (ROUND(AVG(b.globe_temp_parkplatz::DOUBLE), 3) BETWEEN -30.0 AND 90.0))
    AND (ROUND(AVG(b.globe_temp_mobiga_nord::DOUBLE), 3) IS NULL OR (ROUND(AVG(b.globe_temp_mobiga_nord::DOUBLE), 3) BETWEEN -30.0 AND 90.0))
    AND (ROUND(AVG(b.globe_temp_mobiga_sued::DOUBLE), 3) IS NULL OR (ROUND(AVG(b.globe_temp_mobiga_sued::DOUBLE), 3) BETWEEN -30.0 AND 90.0))

ORDER BY timestamp ASC;
'''

INDEX_SYNC_SQL = '''
CREATE INDEX IF NOT EXISTS idx_sync_filtered_timestamp ON synchronized_data_filtered(timestamp);
'''


def sync_data():
    '''Synchronize greenroof and parkplatz data per minute with production filters.'''
    conn = get_connection()

    conn.execute('DROP TABLE IF EXISTS synchronized_data_filtered;')
    print('[sync_data] Joining greenroof + parkplatz (+ black-globe) per minute (this may take a while)...', flush=True)
    t0 = time.time()
    conn.execute(SYNC_SQL)
    conn.execute(INDEX_SYNC_SQL)

    count = conn.execute('SELECT COUNT(*) FROM synchronized_data_filtered;').fetchone()[0]
    print(f'[sync_data] Synchronized {count} minute-level records ({time.time() - t0:.1f}s)')

    # Show data quality summary
    summary = conn.execute('''
    SELECT
        MIN(timestamp) AS earliest,
        MAX(timestamp) AS latest,
        COUNT(*) AS total_minutes
    FROM synchronized_data_filtered;
    ''').fetchone()
    if summary:
        print(f'  Earliest: {summary[0]}')
        print(f'  Latest: {summary[1]}')
        print(f'  Total minutes: {summary[2]}')

    conn.close()
