"""
Bingen Green Roof Analysis Module
Adapted from enhanced_complexindex_temp_diff_2.py for the Bingen pipeline.
Connects to the pipeline's DuckDB file and uses the synchronized_data_filtered table.
"""

import os
import duckdb
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import warnings

try:
    import streamlit as st
except Exception:
    st = None
warnings.filterwarnings('ignore')


class DataLoadError(RuntimeError):
    """Raised when dashboard data cannot be loaded from the DuckDB database."""


# Every column load_data() selects except these is a plain numeric measurement.
# DuckDB (unlike the old Postgres/psycopg2 path) returns an all-NULL column as
# pandas dtype `object` instead of `float64` — this happens for real here (e.g.
# albedo_greenroof is entirely NULL for 2020-2023, before that sensor existed).
# Left uncoerced, a chart mixing such a column with a normal float64 column
# fails with "Plotly Express cannot process wide-form data with columns of
# different type." Coercing explicitly avoids that regardless of which slice
# of years/filters happens to be all-NULL.
_NON_NUMERIC_LOAD_COLUMNS = {'timestamp', 'has_dual_level_greenroof', 'measurement_period'}


def _get_secret(name):
    # st.secrets raises if .streamlit/secrets.toml doesn't exist at all (rather
    # than just reporting the key missing) — and that file is gitignored, so a
    # fresh checkout or a handed-over copy of this project won't have one.
    if st is not None and hasattr(st, "secrets"):
        try:
            if name in st.secrets:
                return st.secrets[name]
        except Exception:
            pass
    return os.getenv(name)


def _default_db_path():
    override = _get_secret("DUCKDB_PATH")
    if override:
        return override
    from config.settings import DUCKDB_PATH
    return DUCKDB_PATH


class BingenGreenRoofAnalyzer:
    """
    Green Roof Analysis for Bingen Pipeline.
    Uses temp_diff_1/temp_diff_2 and energy columns from synchronized_data_filtered.
    """

    def __init__(self, db_path=None):
        self.db_path = db_path or _default_db_path()
        # read_only=True: the dashboard never writes, and never contends with a
        # pipeline update — updates build into a separate staging file and are
        # only swapped into this path once fully verified (see run_update.py).
        # poolclass=NullPool: don't keep the file open between queries. Streamlit
        # caches this engine for the whole server session (st.cache_resource), so
        # a pooled connection would hold the file open continuously — blocking an
        # update's atomic swap on Windows for as long as the dashboard has ever
        # been used, not just while a query is actively running.
        self.engine = create_engine(
            f"duckdb:///{self.db_path}",
            connect_args={"read_only": True},
            poolclass=NullPool,
        )

        self.df = None
        self.analysis_results = {}
        self._computed_cache = {}

    def get_available_years(self):
        """Get available years for yearly analysis controls."""
        query = text(
            """
            SELECT DISTINCT EXTRACT(YEAR FROM timestamp)::INT AS year
            FROM synchronized_data_filtered
            WHERE timestamp IS NOT NULL
            ORDER BY year;
            """
        )
        try:
            years_df = pd.read_sql_query(query, self.engine)
            return years_df['year'].dropna().astype(int).tolist()
        except Exception:
            return []

    def get_timestamp_bounds(self):
        """Get min/max timestamp from synchronized table."""
        query = text(
            """
            SELECT MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts
            FROM synchronized_data_filtered;
            """
        )
        try:
            bounds = pd.read_sql_query(query, self.engine)
            min_ts = pd.to_datetime(bounds.loc[0, 'min_ts'])
            max_ts = pd.to_datetime(bounds.loc[0, 'max_ts'])
            return min_ts, max_ts
        except Exception:
            return None, None

    def get_data_fingerprint(self):
        """Return a lightweight fingerprint for cache invalidation after pipeline updates."""
        query = text(
            """
            SELECT
                COUNT(*)::BIGINT AS row_count,
                MAX(timestamp) AS max_ts
            FROM synchronized_data_filtered;
            """
        )
        try:
            fp = pd.read_sql_query(query, self.engine)
            row_count = int(fp.loc[0, 'row_count']) if not fp.empty else 0
            max_ts = fp.loc[0, 'max_ts'] if not fp.empty else None
            max_ts_str = pd.to_datetime(max_ts).isoformat() if pd.notna(max_ts) else 'none'
            return f"{row_count}:{max_ts_str}"
        except Exception:
            return 'unknown'

    def load_data(self, start_ts=None, end_ts=None, year=None, sample_every_n: int = 1):
        """Load filtered data from synchronized_data_filtered table.

        Parameters
        ----------
        sample_every_n:
            Keep only every N-th minute of data (1 = full resolution).
            This is applied in SQL so only the sampled rows are transferred.
        """
        # Invalidate derived cache when the underlying dataframe changes.
        self._computed_cache = {}
        conditions = [
            "temp_diff_1 IS NOT NULL",
            "temp_diff_2 IS NOT NULL",
        ]
        params = {}

        if year is not None:
            conditions.append("EXTRACT(YEAR FROM timestamp) = $year")
            params['year'] = int(year)

        if start_ts is not None:
            conditions.append("timestamp >= $start_ts")
            params['start_ts'] = pd.to_datetime(start_ts)

        if end_ts is not None:
            conditions.append("timestamp <= $end_ts")
            params['end_ts'] = pd.to_datetime(end_ts)

        n = max(1, int(sample_every_n))
        if n > 1:
            # Sample every N-th minute in SQL to limit transferred rows.
            conditions.append(
                f"FLOOR(EXTRACT(EPOCH FROM timestamp) / 60)::BIGINT % {n} = 0"
            )

        query = f"""
        SELECT
            timestamp,
            avg_air_temperature_greenroof,
            avg_air_temp_2_greenroof,
            avg_air_temp_1_parkplatz,
            avg_air_temp_2_parkplatz,
            avg_air_humidity_1_greenroof,
            avg_air_humidity_2_greenroof,
            avg_wind_speed_greenroof,
            avg_soil_temperature_greenroof,
            avg_soil_moisture_greenroof,
            avg_global_radiation_greenroof,
            avg_sr2_greenroof,
            avg_ir1_greenroof,
            avg_ir2_greenroof,
            avg_ir1_parkplatz,
            avg_ir2_parkplatz,
            avg_sr1_parkplatz,
            avg_sr2_parkplatz,
            avg_temp_parkplatz,
            has_dual_level_greenroof,
            measurement_period,
            temp_diff_1,
            temp_diff_2,
            delta_t_roof,
            delta_q_roof,
            delta_t_parkplatz,
            albedo_greenroof,
            albedo_parkplatz,
            radiation_balance_greenroof,
            energy_from_air_parkplatz,
            energy_from_surface_parkplatz,
            radiation_balance_parkplatz
        FROM synchronized_data_filtered
        WHERE {' AND '.join(conditions)}
        ORDER BY timestamp ASC;
        """
        # A direct duckdb connection + .df() is used here rather than the
        # SQLAlchemy engine (pd.read_sql_query) because that path's per-row
        # DBAPI cursor conversion becomes drastically slower as column count
        # grows — 3 columns ~12s, this query's 32 columns >180s on the full
        # dataset. Direct .df() materializes the whole result via Arrow in
        # ~14s regardless of column count. Opened/closed per call (not held
        # on self) so it never blocks a pipeline update's atomic swap.
        conn = None
        try:
            conn = duckdb.connect(database=self.db_path, read_only=True)
            self.df = conn.execute(query, params).df()
        except duckdb.Error as exc:
            error_text = str(exc)
            error_lower = error_text.lower()

            # Check is_locked first: DuckDB's real Windows file-in-use error is
            # 'Cannot open file "...": The process cannot access the file
            # because it is being used by another process' — that message
            # contains 'cannot open', so is_missing_file's check would
            # otherwise match first and misreport a busy file as a missing one.
            is_locked = any(token in error_lower for token in [
                'being used by another process',
                'file is already open',
                'conflicting lock',
            ])
            is_missing_file = not is_locked and any(token in error_lower for token in [
                'no such file',
                'cannot open',
                'does not exist',
            ])

            if is_locked:
                base_message = "The DuckDB database file is locked by another process."
                hint_message = (
                    "A pipeline update may be running, or another dashboard process has it open "
                    "for writing. Wait for it to finish and reload."
                )
            elif is_missing_file:
                base_message = "The DuckDB database file could not be found or opened."
                hint_message = (
                    f"Expected at: {self.db_path}\n"
                    "Run the pipeline update (Operations tab, or scripts/run_update.py) "
                    "to build it."
                )
            else:
                base_message = (
                    "Could not read data from table 'synchronized_data_filtered'. "
                    "This can indicate a corrupted or incompletely-built database file."
                )
                hint_message = (
                    "Re-run the pipeline update (Operations tab, or scripts/run_update.py) "
                    "to rebuild the database. Because updates build into a staging file first, "
                    "this file should never be left half-built — if it is, please report this."
                )

            raise DataLoadError(
                f"{base_message}\n{hint_message}\n\nTechnical error: {error_text}"
            ) from exc
        finally:
            if conn is not None:
                conn.close()

        numeric_cols = [c for c in self.df.columns if c not in _NON_NUMERIC_LOAD_COLUMNS]
        self.df[numeric_cols] = self.df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        if self.df.empty:
            self.analysis_results['dataset_info'] = {
                'total_measurements': 0,
                'date_range': 'No data in selected filter',
                'year_filter': year,
                'custom_range': {
                    'start': str(start_ts) if start_ts is not None else None,
                    'end': str(end_ts) if end_ts is not None else None,
                }
            }
            return self.df

        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        self.df.set_index('timestamp', inplace=True)
        self.df = self.df.sort_index(ascending=True)
        self.df = self.df[~self.df.index.duplicated(keep='first')]

        # Add time features
        self.df['hour'] = self.df.index.hour
        self.df['month'] = self.df.index.month
        self.df['year'] = self.df.index.year
        self.df['day_night'] = np.where(self.df['hour'].between(6, 20), 'day', 'night')
        self.df['season'] = self.df['month'].map({
            12: 'Winter', 1: 'Winter', 2: 'Winter',
            3: 'Spring', 4: 'Spring', 5: 'Spring',
            6: 'Summer', 7: 'Summer', 8: 'Summer',
            9: 'Autumn', 10: 'Autumn', 11: 'Autumn'
        })
        self.df['period'] = np.where(
            self.df['month'].between(4, 9), 'Growing (Apr–Sep)', 'Cold (Oct–Mar)'
        )

        self.analysis_results['dataset_info'] = {
            'total_measurements': len(self.df),
            'date_range': f"{self.df.index.min().strftime('%Y-%m-%d')} to {self.df.index.max().strftime('%Y-%m-%d')}",
            'year_filter': year,
            'custom_range': {
                'start': str(start_ts) if start_ts is not None else None,
                'end': str(end_ts) if end_ts is not None else None,
            }
        }

        return self.df

    def get_hourly_energy_analysis(self):
        """Get hourly averages for energy balance analysis."""
        hourly = self.df.groupby('hour').agg({
            'temp_diff_1': 'mean',
            'temp_diff_2': 'mean',
            'energy_from_air_parkplatz': 'mean',
            'energy_from_surface_parkplatz': 'mean',
            'radiation_balance_parkplatz': 'mean',
            'avg_global_radiation_greenroof': 'mean',
        }).reset_index()

        return hourly

    def get_seasonal_energy_analysis(self):
        """Get seasonal averages for energy balance."""
        seasonal = self.df.groupby('season').agg({
            'temp_diff_1': 'mean',
            'temp_diff_2': 'mean',
            'energy_from_air_parkplatz': 'mean',
            'energy_from_surface_parkplatz': 'mean',
            'radiation_balance_parkplatz': 'mean',
            'avg_global_radiation_greenroof': 'mean',
        }).reset_index()

        # Sort seasons properly
        season_order = ['Winter', 'Spring', 'Summer', 'Autumn']
        seasonal['season'] = pd.Categorical(seasonal['season'], categories=season_order, ordered=True)
        seasonal = seasonal.sort_values('season')

        return seasonal

    def get_radiation_vs_tempdiff_correlation(self):
        """Get data for radiation vs temperature difference correlation."""
        return self.df[['radiation_balance_parkplatz', 'temp_diff_1', 'temp_diff_2',
                        'energy_from_air_parkplatz', 'energy_from_surface_parkplatz']].dropna()

    def create_condition_categories(self):
        """Create enhanced condition categories."""
        if self.df is None or self.df.empty:
            return pd.Series(dtype='int64')

        def _categorize_series(series, low_max_exclusive, medium_max_exclusive, low_label, mid_label, high_label):
            return np.select(
                [
                    series.isna(),
                    series < low_max_exclusive,
                    series < medium_max_exclusive,
                ],
                [
                    'Unknown',
                    low_label,
                    mid_label,
                ],
                default=high_label,
            )

        temp_cat = _categorize_series(
            self.df['avg_air_temperature_greenroof'],
            5.0,
            30.0,
            'Dormant / Cold',
            'Growth / Comfort',
            'Heat Stress',
        )
        humidity_cat = _categorize_series(
            self.df.get('avg_air_humidity_1_greenroof', pd.Series(np.nan, index=self.df.index)),
            40.0,
            60.0,
            'Dry',
            'Optimal',
            'Humid',
        )
        wind_cat = _categorize_series(
            self.df.get('avg_wind_speed_greenroof', pd.Series(np.nan, index=self.df.index)),
            1.5,
            5.0,
            'Calm Wind',
            'Moderate Wind',
            'High Wind',
        )
        solar_cat = _categorize_series(
            self.df.get('avg_global_radiation_greenroof', pd.Series(np.nan, index=self.df.index)),
            200.0,
            600.0,
            'Overcast',
            'Partly Cloudy',
            'Clear Sky',
        )
        soil_cat = _categorize_series(
            self.df.get('avg_soil_moisture_greenroof', pd.Series(np.nan, index=self.df.index)),
            13.0,
            33.0,
            'Wilt Risk',
            'Optimal',
            'Saturation',
        )

        conditions = [
            (temp_cat == 'Heat Stress') & (solar_cat == 'Clear Sky') & (soil_cat == 'Wilt Risk'),
            (temp_cat == 'Heat Stress') & (solar_cat == 'Clear Sky') & (humidity_cat == 'Dry'),
            (temp_cat == 'Heat Stress') & (wind_cat == 'Calm Wind') & (humidity_cat == 'Dry'),
            (temp_cat == 'Heat Stress') & (solar_cat == 'Clear Sky') & (wind_cat == 'Calm Wind'),
            (temp_cat == 'Heat Stress') & (wind_cat == 'High Wind'),
            (temp_cat == 'Heat Stress') & (soil_cat == 'Wilt Risk'),
            (temp_cat == 'Growth / Comfort') & (soil_cat == 'Optimal') & (solar_cat == 'Partly Cloudy'),
            (temp_cat == 'Dormant / Cold') & (wind_cat == 'High Wind'),
            (solar_cat == 'Clear Sky') & (wind_cat == 'Calm Wind') & (temp_cat == 'Growth / Comfort'),
            (soil_cat == 'Saturation') & (temp_cat == 'Growth / Comfort'),
            (humidity_cat == 'Humid') & (temp_cat == 'Growth / Comfort'),
        ]
        labels = [
            'Compound Triple Stress',
            'High ET Stress',
            'Urban Heat Island',
            'Extreme Heat Stress',
            'Heat Stress + High Wind',
            'Drought Stress',
            'Optimal Growth',
            'Cold + Wind Stress',
            'High Solar + Calm',
            'Saturated Growth',
            'High Humidity Growth',
        ]

        self.df['enhanced_condition'] = np.select(conditions, labels, default=temp_cat)
        self._computed_cache.clear()

        return self.df['enhanced_condition'].value_counts()

    def get_cooling_by_condition(self, temp_diff_col='temp_diff_2'):
        """Get cooling statistics by enhanced condition."""
        cache_key = f'cooling_stats_{temp_diff_col}'
        if cache_key in self._computed_cache:
            return self._computed_cache[cache_key].copy()

        if 'enhanced_condition' not in self.df.columns:
            self.create_condition_categories()

        data = self.df[['enhanced_condition', temp_diff_col]].dropna().copy()
        if data.empty:
            result = pd.DataFrame(
                columns=[
                    'condition', 'mean_effect', 'std_effect', 'count',
                    'meaningful_cooling_pct', 'strong_cooling_pct',
                    'within_error_pct', 'meaningful_warming_pct', 'classification'
                ]
            )
            self._computed_cache[cache_key] = result
            return result.copy()

        data['meaningful_cooling'] = data[temp_diff_col] < -0.2
        data['strong_cooling'] = data[temp_diff_col] < -1.2
        data['within_error'] = data[temp_diff_col].between(-0.2, 0.2)
        data['meaningful_warming'] = data[temp_diff_col] > 0.2

        grouped = data.groupby('enhanced_condition').agg(
            mean_effect=(temp_diff_col, 'mean'),
            std_effect=(temp_diff_col, 'std'),
            count=(temp_diff_col, 'size'),
            meaningful_cooling_pct=('meaningful_cooling', 'mean'),
            strong_cooling_pct=('strong_cooling', 'mean'),
            within_error_pct=('within_error', 'mean'),
            meaningful_warming_pct=('meaningful_warming', 'mean'),
        )

        grouped = grouped[grouped['count'] >= 30].reset_index().rename(columns={'enhanced_condition': 'condition'})

        grouped['meaningful_cooling_pct'] = (grouped['meaningful_cooling_pct'] * 100).round(1)
        grouped['strong_cooling_pct'] = (grouped['strong_cooling_pct'] * 100).round(1)
        grouped['within_error_pct'] = (grouped['within_error_pct'] * 100).round(1)
        grouped['meaningful_warming_pct'] = (grouped['meaningful_warming_pct'] * 100).round(1)
        grouped['mean_effect'] = grouped['mean_effect'].round(3)
        grouped['std_effect'] = grouped['std_effect'].round(3)

        grouped['classification'] = np.select(
            [
                grouped['mean_effect'] < -1.2,
                grouped['mean_effect'] < -0.4,
                grouped['mean_effect'] < -0.2,
                grouped['mean_effect'].between(-0.2, 0.2),
                grouped['mean_effect'] <= 0.4,
            ],
            [
                'Very Strong Cooling',
                'Strong Cooling',
                'Moderate Cooling',
                'Within Instrument Error',
                'Weak Warming',
            ],
            default='Strong Warming',
        )

        result = grouped.sort_values('mean_effect')
        self._computed_cache[cache_key] = result
        return result.copy()

    def get_condition_distribution_data(self, temp_diff_col='temp_diff_2'):
        """Get data for box plot distribution by condition."""
        cache_key = f'distribution_{temp_diff_col}'
        if cache_key in self._computed_cache:
            return self._computed_cache[cache_key].copy()

        if 'enhanced_condition' not in self.df.columns:
            self.create_condition_categories()

        # Filter to conditions with enough data
        valid_conditions = self.df['enhanced_condition'].value_counts()
        valid_conditions = valid_conditions[valid_conditions >= 30].index.tolist()

        result = self.df[self.df['enhanced_condition'].isin(valid_conditions)][[
            'enhanced_condition', temp_diff_col, 'season', 'hour'
        ]].copy()
        self._computed_cache[cache_key] = result
        return result.copy()

    def get_seasonal_stress_data(self, temp_diff_col='temp_diff_2'):
        """Get seasonal variation data for stress conditions."""
        cache_key = f'seasonal_stress_{temp_diff_col}'
        if cache_key in self._computed_cache:
            return self._computed_cache[cache_key].copy()

        if 'enhanced_condition' not in self.df.columns:
            self.create_condition_categories()

        stress_keywords = ['Stress', 'Compound', 'High ET', 'Urban Heat', 'Drought', 'Extreme']
        stress_conditions = [c for c in self.df['enhanced_condition'].unique()
                           if any(kw in c for kw in stress_keywords)]

        seasonal_data = []
        for condition in stress_conditions:
            for season in ['Winter', 'Spring', 'Summer', 'Autumn']:
                subset = self.df[(self.df['enhanced_condition'] == condition) &
                               (self.df['season'] == season)]
                if len(subset) >= 5:
                    mean_effect = subset[temp_diff_col].mean()
                    meaningful_cooling = (subset[temp_diff_col] < -0.2).sum() / len(subset) * 100
                    seasonal_data.append({
                        'condition': condition,
                        'season': season,
                        'mean_effect': round(mean_effect, 3),
                        'meaningful_cooling_pct': round(meaningful_cooling, 1),
                        'count': len(subset)
                    })

        result = pd.DataFrame(seasonal_data)
        self._computed_cache[cache_key] = result
        return result.copy()

    def get_thesis_summary(self, scope='full_period', temp_diff_col='temp_diff_1'):
        """Return thesis summary grouped by year/season/day-night for selected scope."""
        cache_key = f'thesis_summary_{scope}_{temp_diff_col}'
        if cache_key in self._computed_cache:
            return self._computed_cache[cache_key].copy()

        data = self.df.copy()
        if scope == 'dual_level_only':
            data = data[data['has_dual_level_greenroof'] == True]

        if data.empty:
            result = pd.DataFrame()
            self._computed_cache[cache_key] = result
            return result

        grouped = data.groupby(['year', 'season', 'day_night']).agg(
            temp_signal_mean=(temp_diff_col, 'mean'),
            delta_t_roof_mean=('delta_t_roof', 'mean'),
            delta_q_roof_mean=('delta_q_roof', 'mean'),
            rnet_greenroof_mean=('radiation_balance_greenroof', 'mean'),
            albedo_greenroof_mean=('albedo_greenroof', 'mean'),
            records=(temp_diff_col, 'count'),
        ).reset_index()

        grouped['temp_signal_mean'] = grouped['temp_signal_mean'].round(3)
        grouped['delta_t_roof_mean'] = grouped['delta_t_roof_mean'].round(3)
        grouped['delta_q_roof_mean'] = grouped['delta_q_roof_mean'].round(5)
        grouped['rnet_greenroof_mean'] = grouped['rnet_greenroof_mean'].round(3)
        grouped['albedo_greenroof_mean'] = grouped['albedo_greenroof_mean'].round(4)

        result = grouped.sort_values(['year', 'season', 'day_night'])
        self._computed_cache[cache_key] = result
        return result.copy()

    def get_et_scatter_data(self, temp_diff_col='temp_diff_1', max_points=25000):
        """Return dual-level ET proxy (specific-humidity gradient) scatter data
        with optional downsampling."""
        data = self.df[self.df['has_dual_level_greenroof'] == True][
            ['delta_q_roof', temp_diff_col, 'season', 'year']
        ].dropna()

        if data.empty:
            return data

        if len(data) > max_points:
            data = data.sample(n=max_points, random_state=42)

        return data

    def get_monthly_albedo_rnet(self):
        """Return monthly albedo and radiation balance comparisons for roof and parkplatz."""
        data = self.df.copy()
        if data.empty:
            return pd.DataFrame()

        monthly = data.groupby(['year', 'month']).agg(
            albedo_greenroof=('albedo_greenroof', 'mean'),
            albedo_parkplatz=('albedo_parkplatz', 'mean'),
            rnet_greenroof=('radiation_balance_greenroof', 'mean'),
            rnet_parkplatz=('radiation_balance_parkplatz', 'mean'),
            dual_level_share=('has_dual_level_greenroof', 'mean'),
        ).reset_index()

        monthly['dual_level_share'] = (monthly['dual_level_share'] * 100).round(1)
        monthly['period'] = np.where(monthly['dual_level_share'] > 0, 'dual_level_present', 'single_level_only')
        monthly['month_label'] = monthly['year'].astype(str) + '-' + monthly['month'].astype(str).str.zfill(2)
        return monthly.sort_values(['year', 'month'])

    def get_period_summary(self, temp_diff_col='temp_diff_1'):
        """Growing (Apr–Sep) vs Cold (Oct–Mar) summary grouped by year and day/night."""
        cache_key = f'period_summary_{temp_diff_col}'
        if cache_key in self._computed_cache:
            return self._computed_cache[cache_key].copy()

        if 'period' not in self.df.columns:
            return pd.DataFrame()

        grouped = self.df.groupby(['year', 'period', 'day_night']).agg(
            temp_signal_mean=(temp_diff_col, 'mean'),
            delta_q_roof_mean=('delta_q_roof', 'mean'),
            delta_t_roof_mean=('delta_t_roof', 'mean'),
            rnet_greenroof_mean=('radiation_balance_greenroof', 'mean'),
            albedo_greenroof_mean=('albedo_greenroof', 'mean'),
            records=(temp_diff_col, 'count'),
        ).reset_index()

        for col in ['temp_signal_mean', 'delta_t_roof_mean', 'rnet_greenroof_mean']:
            grouped[col] = grouped[col].round(3)
        grouped['delta_q_roof_mean'] = grouped['delta_q_roof_mean'].round(5)
        grouped['albedo_greenroof_mean'] = grouped['albedo_greenroof_mean'].round(4)

        result = grouped.sort_values(['year', 'period', 'day_night'])
        self._computed_cache[cache_key] = result
        return result.copy()

    def get_overall_signal_stats(self, temp_diff_col='temp_diff_1'):
        """Return overall mean, CI, and sample size for selected temperature signal."""
        data = self.df[temp_diff_col].dropna()
        if data.empty:
            return {
                'mean': np.nan,
                'std': np.nan,
                'sem': np.nan,
                'ci_low': np.nan,
                'ci_high': np.nan,
                'count': 0,
            }

        mean_val = data.mean()
        std_val = data.std(ddof=1)
        sem_val = std_val / np.sqrt(len(data)) if len(data) > 1 else 0.0
        ci_margin = 1.96 * sem_val

        return {
            'mean': float(mean_val),
            'std': float(std_val),
            'sem': float(sem_val),
            'ci_low': float(mean_val - ci_margin),
            'ci_high': float(mean_val + ci_margin),
            'count': int(len(data)),
        }

    def get_year_significance_summary(self, temp_diff_col='temp_diff_1'):
        """Return yearly mean/CI and significance vs zero and vs full-period mean."""
        cache_key = f'year_significance_{temp_diff_col}'
        if cache_key in self._computed_cache:
            return self._computed_cache[cache_key].copy()

        data = self.df[[temp_diff_col, 'year']].dropna().copy()
        if data.empty:
            return pd.DataFrame()

        overall_stats = self.get_overall_signal_stats(temp_diff_col=temp_diff_col)
        overall_mean = overall_stats['mean']
        overall_sem = overall_stats['sem']

        grouped = data.groupby('year')[temp_diff_col].agg(['mean', 'std', 'count']).reset_index()
        grouped['sem'] = grouped['std'] / np.sqrt(grouped['count'].clip(lower=1))
        grouped['ci_margin'] = 1.96 * grouped['sem']
        grouped['ci_low'] = grouped['mean'] - grouped['ci_margin']
        grouped['ci_high'] = grouped['mean'] + grouped['ci_margin']
        grouped['significant_vs_zero'] = (grouped['ci_low'] > 0) | (grouped['ci_high'] < 0)

        grouped['overall_mean'] = overall_mean
        grouped['diff_vs_5yr_mean'] = grouped['mean'] - overall_mean
        grouped['diff_sem'] = np.sqrt((grouped['sem'] ** 2) + (overall_sem ** 2))
        grouped['diff_ci_margin'] = 1.96 * grouped['diff_sem']
        grouped['diff_ci_low'] = grouped['diff_vs_5yr_mean'] - grouped['diff_ci_margin']
        grouped['diff_ci_high'] = grouped['diff_vs_5yr_mean'] + grouped['diff_ci_margin']
        grouped['significant_vs_5yr_mean'] = (grouped['diff_ci_low'] > 0) | (grouped['diff_ci_high'] < 0)

        for col in ['mean', 'std', 'sem', 'ci_low', 'ci_high', 'diff_vs_5yr_mean', 'diff_ci_low', 'diff_ci_high']:
            grouped[col] = grouped[col].round(4)

        result = grouped.sort_values('year')
        self._computed_cache[cache_key] = result
        return result.copy()

    def get_monthly_progression(self, temp_diff_col='temp_diff_1'):
        """Return month-within-year progression table for selected temperature signal."""
        cache_key = f'monthly_progression_{temp_diff_col}'
        if cache_key in self._computed_cache:
            return self._computed_cache[cache_key].copy()

        data = self.df[[temp_diff_col, 'year', 'month']].dropna().copy()
        if data.empty:
            return pd.DataFrame()

        grouped = data.groupby(['year', 'month']).agg(
            mean_signal=(temp_diff_col, 'mean'),
            records=(temp_diff_col, 'count'),
            std_signal=(temp_diff_col, 'std'),
        ).reset_index()

        grouped['month_label'] = grouped['year'].astype(str) + '-' + grouped['month'].astype(str).str.zfill(2)
        grouped['mean_signal'] = grouped['mean_signal'].round(4)
        grouped['std_signal'] = grouped['std_signal'].round(4)

        result = grouped.sort_values(['year', 'month'])
        self._computed_cache[cache_key] = result
        return result.copy()

    def get_radiation_components_summary(self):
        """Return seasonal radiation component decomposition for both sites."""
        cache_key = 'radiation_components_summary'
        if cache_key in self._computed_cache:
            return self._computed_cache[cache_key].copy()

        cols = [
            'season',
            'avg_global_radiation_greenroof', 'avg_sr2_greenroof', 'avg_ir1_greenroof', 'avg_ir2_greenroof',
            'avg_sr1_parkplatz', 'avg_sr2_parkplatz', 'avg_ir1_parkplatz', 'avg_ir2_parkplatz',
        ]
        data = self.df[cols].dropna().copy()
        if data.empty:
            return pd.DataFrame()

        seasonal = data.groupby('season').agg(
            sw_in_greenroof=('avg_global_radiation_greenroof', 'mean'),
            sw_out_greenroof=('avg_sr2_greenroof', 'mean'),
            lw_in_greenroof=('avg_ir1_greenroof', 'mean'),
            lw_out_greenroof=('avg_ir2_greenroof', 'mean'),
            sw_in_parkplatz=('avg_sr1_parkplatz', 'mean'),
            sw_out_parkplatz=('avg_sr2_parkplatz', 'mean'),
            lw_in_parkplatz=('avg_ir1_parkplatz', 'mean'),
            lw_out_parkplatz=('avg_ir2_parkplatz', 'mean'),
        ).reset_index()

        season_order = ['Winter', 'Spring', 'Summer', 'Autumn']
        seasonal['season'] = pd.Categorical(seasonal['season'], categories=season_order, ordered=True)
        seasonal = seasonal.sort_values('season')

        records = []
        for _, row in seasonal.iterrows():
            for site in ['greenroof', 'parkplatz']:
                records.extend([
                    {'season': row['season'], 'site': site, 'component': 'SW_in', 'value': row[f'sw_in_{site}']},
                    {'season': row['season'], 'site': site, 'component': 'SW_out', 'value': row[f'sw_out_{site}']},
                    {'season': row['season'], 'site': site, 'component': 'LW_in', 'value': row[f'lw_in_{site}']},
                    {'season': row['season'], 'site': site, 'component': 'LW_out', 'value': row[f'lw_out_{site}']},
                ])

        result = pd.DataFrame(records)
        result['value'] = result['value'].round(3)
        self._computed_cache[cache_key] = result
        return result.copy()

    def get_case_study_day(self, date_str: str, temp_diff_col: str = 'temp_diff_1') -> pd.DataFrame:
        """Return minute-level data for a single calendar day."""
        try:
            day = pd.Timestamp(date_str)
        except Exception:
            return pd.DataFrame()

        day_data = self.df[self.df.index.date == day.date()].copy()
        if day_data.empty:
            return day_data

        day_data['hour_decimal'] = day_data.index.hour + day_data.index.minute / 60
        return day_data


# Color scheme for conditions
CONDITION_COLORS = {
    'Dormant / Cold': '#2196F3',
    'Growth / Comfort': '#4CAF50',
    'Heat Stress': '#F44336',
    'Compound Triple Stress': '#4A0E0E',
    'High ET Stress': '#7B1FA2',
    'Urban Heat Island': '#E65100',
    'Extreme Heat Stress': '#B71C1C',
    'Heat Stress + High Wind': '#FF5722',
    'Drought Stress': '#FF9800',
    'Cold + Wind Stress': '#1976D2',
    'Optimal Growth': '#66BB6A',
    'High Solar + Calm': '#FFC107',
    'Saturated Growth': '#00BCD4',
    'High Humidity Growth': '#9C27B0',
    'Unknown': '#9E9E9E',
}
