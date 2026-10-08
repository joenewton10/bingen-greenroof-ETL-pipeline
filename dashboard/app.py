"""
Bingen Green Roof Analysis Dashboard
Streamlit app for comprehensive green roof cooling analysis.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import inspect
from datetime import datetime, timedelta

from analysis import BingenGreenRoofAnalyzer, CONDITION_COLORS, DataLoadError
from operations import render_operations_page
from query_tool import render_query_page
from table_browser import render_browse_page


st.set_page_config(
    page_title="Bingen Green Roof Analysis",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .main-header { font-size: 2.5rem; font-weight: bold; color: #2E7D32; text-align: center; margin-bottom: 1rem; }
    .sub-header  { font-size: 1.2rem; color: #666; text-align: center; margin-bottom: 2rem; }

    /* Advanced mode has 11 tabs — wider than most screens. Scroll horizontally
       (mouse wheel / trackpad swipe / drag) instead of silently clipping the
       later tabs off the right edge. */
    .stTabs [data-baseweb="tab-list"] {
        overflow-x: auto;
        flex-wrap: nowrap;
        scrollbar-width: thin;
    }
    .stTabs [data-baseweb="tab"] {
        white-space: nowrap;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_metadata_analyzer():
    return BingenGreenRoofAnalyzer()


@st.cache_resource
def load_analyzer(selected_year, start_ts_iso, end_ts_iso, sample_n=1, data_fingerprint='unknown'):
    _ = data_fingerprint
    analyzer = BingenGreenRoofAnalyzer()
    start_ts = pd.to_datetime(start_ts_iso) if start_ts_iso else None
    end_ts = pd.to_datetime(end_ts_iso) if end_ts_iso else None
    # Compatibility fallback in case Streamlit still holds an older analyzer class in memory.
    if 'sample_every_n' in inspect.signature(analyzer.load_data).parameters:
        analyzer.load_data(
            start_ts=start_ts,
            end_ts=end_ts,
            year=selected_year,
            sample_every_n=sample_n,
        )
    else:
        analyzer.load_data(start_ts=start_ts, end_ts=end_ts, year=selected_year)
    return analyzer


def render_guided_dashboard(analyzer, temp_diff_choice):
    st.subheader("🧭 Analysis Mode")
    st.caption("Follow this sequence")

    overall = analyzer.get_overall_signal_stats(temp_diff_col=temp_diff_choice)
    year_sig = analyzer.get_year_significance_summary(temp_diff_col=temp_diff_choice)

    with st.expander("Summary", expanded=True):
        col1, col2, col3 = st.columns(3)
        col1.metric("Overall mean signal [°C]", f"{overall['mean']:.3f}")
        col2.metric("95% CI", f"[{overall['ci_low']:.3f}, {overall['ci_high']:.3f}]")
        direction = "Cooling" if overall['mean'] < 0 else "Warming"
        col3.metric("Interpretation", direction)

        if not year_sig.empty:
            warmest = year_sig.sort_values('mean', ascending=False).iloc[0]
            coolest = year_sig.sort_values('mean', ascending=True).iloc[0]
            st.write(
                f"Warmest year: **{int(warmest['year'])}** ({warmest['mean']:.3f}°C) | "
                f"Coolest year: **{int(coolest['year'])}** ({coolest['mean']:.3f}°C)"
            )

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "1) Overall Signal",
        "2) Day/Night + Seasons",
        "3) Year Significance",
        "4) Month within Year",
        "5) Energy Pathways",
        "6) Case Study Days",
    ])

    with tab1:
        st.header("Step 1: Overall Thermal Signal")
        col1, col2, col3 = st.columns(3)
        col1.metric("Mean", f"{overall['mean']:.3f} °C")
        col2.metric("Std", f"{overall['std']:.3f} °C")
        col3.metric("Records", f"{overall['count']:,}")

        signal_data = analyzer.df[temp_diff_choice].dropna()
        fig_hist = px.histogram(
            signal_data,
            x=temp_diff_choice,
            nbins=80,
            title=f"Distribution of {temp_diff_choice}",
        )
        fig_hist.add_vline(x=0, line_dash='dash', line_color='red')
        fig_hist.add_vline(x=-0.2, line_dash='dot', line_color='blue')
        fig_hist.add_vline(x=0.2, line_dash='dot', line_color='orange')
        fig_hist.update_layout(xaxis_title='Temperature signal [°C]', yaxis_title='Count', height=420)
        st.plotly_chart(fig_hist, use_container_width=True)

    with tab2:
        st.header("Step 2: Day/Night and Seasonal Breakdown")

        day_night = analyzer.df.groupby('day_night')[temp_diff_choice].agg(['mean', 'count']).reset_index()
        season = analyzer.df.groupby('season')[temp_diff_choice].agg(['mean', 'count']).reset_index()
        season['season'] = pd.Categorical(
            season['season'], categories=['Winter', 'Spring', 'Summer', 'Autumn'], ordered=True
        )
        season = season.sort_values('season')

        period_summary = analyzer.get_period_summary(temp_diff_col=temp_diff_choice)
        period_overall = pd.DataFrame()
        if not period_summary.empty:
            period_overall = period_summary.groupby('period', as_index=False).agg(
                temp_signal_mean=('temp_signal_mean', 'mean'),
                records=('records', 'sum'),
            )

        col1, col2, col3 = st.columns(3)
        with col1:
            fig_dn = px.bar(day_night, x='day_night', y='mean', title='Mean signal: Day vs Night')
            fig_dn.add_hline(y=0, line_dash='dash', line_color='red')
            fig_dn.update_layout(yaxis_title='Mean signal [°C]', xaxis_title='')
            st.plotly_chart(fig_dn, use_container_width=True)
        with col2:
            fig_season = px.bar(season, x='season', y='mean', title='Mean signal: 4 seasons')
            fig_season.add_hline(y=0, line_dash='dash', line_color='red')
            fig_season.update_layout(yaxis_title='Mean signal [°C]', xaxis_title='')
            st.plotly_chart(fig_season, use_container_width=True)
        with col3:
            if not period_overall.empty:
                fig_period = px.bar(period_overall, x='period', y='temp_signal_mean', title='Mean signal: Growing vs Cold')
                fig_period.add_hline(y=0, line_dash='dash', line_color='red')
                fig_period.update_layout(yaxis_title='Mean signal [°C]', xaxis_title='')
                st.plotly_chart(fig_period, use_container_width=True)
            else:
                st.info('Growing/Cold period summary is not available for this filter range.')

    with tab3:
        st.header("Step 3: Year-by-Year Significance")
        if year_sig.empty:
            st.warning('No yearly significance data available.')
        else:
            fig_year = go.Figure()
            fig_year.add_trace(go.Bar(
                x=year_sig['year'],
                y=year_sig['mean'],
                error_y=dict(type='data', array=1.96 * year_sig['sem'], visible=True),
                marker_color=np.where(year_sig['significant_vs_zero'], '#2E7D32', '#90A4AE'),
                name='Mean yearly signal',
            ))
            fig_year.add_hline(y=0, line_dash='dash', line_color='red')
            fig_year.add_hline(y=year_sig['overall_mean'].iloc[0], line_dash='dot', line_color='blue')
            fig_year.update_layout(
                title='Yearly mean with 95% CI',
                xaxis_title='Year',
                yaxis_title='Mean signal [°C]',
                height=420,
                showlegend=False,
            )
            st.plotly_chart(fig_year, use_container_width=True)

            display_cols = [
                'year', 'mean', 'ci_low', 'ci_high',
                'significant_vs_zero', 'diff_vs_5yr_mean', 'diff_ci_low', 'diff_ci_high',
                'significant_vs_5yr_mean',
            ]
            st.dataframe(year_sig[display_cols], use_container_width=True, hide_index=True)

    with tab4:
        st.header("Step 4: Month-Within-Year Progression")
        monthly = analyzer.get_monthly_progression(temp_diff_col=temp_diff_choice)
        if monthly.empty:
            st.warning('No monthly progression data available.')
        else:
            heatmap_df = monthly.pivot(index='month', columns='year', values='mean_signal')
            fig_heat = px.imshow(
                heatmap_df,
                labels=dict(x='Year', y='Month', color='Mean signal [°C]'),
                title='Monthly progression heatmap',
                aspect='auto',
                color_continuous_scale='RdBu_r',
            )
            st.plotly_chart(fig_heat, use_container_width=True)

            years = sorted(monthly['year'].unique().tolist())
            selected_year = st.selectbox('Year for monthly line view', options=years, index=len(years) - 1)
            year_view = monthly[monthly['year'] == selected_year]
            fig_line = px.line(year_view, x='month', y='mean_signal', markers=True, title=f'Monthly trend for {selected_year}')
            fig_line.add_hline(y=0, line_dash='dash', line_color='red')
            fig_line.update_layout(xaxis=dict(dtick=1), yaxis_title='Mean signal [°C]')
            st.plotly_chart(fig_line, use_container_width=True)

    with tab5:
        st.header("Step 5: Energy Pathways")

        rad_components = analyzer.get_radiation_components_summary()
        albedo_monthly = analyzer.get_monthly_albedo_rnet()

        sensible_data = analyzer.df[['delta_t_roof', temp_diff_choice, 'season']].dropna()
        if len(sensible_data) > 25000:
            sensible_data = sensible_data.sample(n=25000, random_state=42)

        # Latent proxy: specific-humidity gradient (delta_q_roof), computed in
        # the database with a fixed Bingen site pressure (pipeline/sync/sync_sites.py)
        # rather than the unreliable parkplatz pressure sensor. Always used here —
        # ΔRH is not a substitute, since relative humidity also moves with
        # temperature and doesn't isolate moisture content on its own.
        latent_data = analyzer.df[['delta_q_roof', temp_diff_choice, 'season']].dropna().copy()
        if not latent_data.empty:
            latent_data['latent_proxy'] = latent_data['delta_q_roof']

        if len(latent_data) > 25000:
            latent_data = latent_data.sample(n=25000, random_state=42)

        col1, col2 = st.columns(2)
        with col1:
            if not rad_components.empty:
                fig_rad = px.bar(
                    rad_components,
                    x='season',
                    y='value',
                    color='component',
                    facet_col='site',
                    barmode='group',
                    title='Radiation decomposition (4 components)',
                )
                fig_rad.update_layout(height=420)
                st.plotly_chart(fig_rad, use_container_width=True)
            else:
                st.info('Radiation component data is unavailable for current filters.')
        with col2:
            if not albedo_monthly.empty:
                fig_alb = px.line(
                    albedo_monthly,
                    x='month_label',
                    y=['albedo_greenroof', 'albedo_parkplatz'],
                    title='Albedo comparison',
                )
                fig_alb.update_layout(height=420, xaxis_title='Month', yaxis_title='Albedo')
                st.plotly_chart(fig_alb, use_container_width=True)
            else:
                st.info('Albedo data is unavailable for current filters.')

        col3, col4 = st.columns(2)
        with col3:
            if not sensible_data.empty:
                fig_sensible = px.scatter(
                    sensible_data,
                    x='delta_t_roof',
                    y=temp_diff_choice,
                    color='season',
                    opacity=0.3,
                    title='Sensible proxy: ΔT roof vs temperature signal',
                )
                fig_sensible.add_hline(y=0, line_dash='dash', line_color='red')
                st.plotly_chart(fig_sensible, use_container_width=True)
            else:
                st.info('Sensible proxy data is unavailable for current filters.')
        with col4:
            if not latent_data.empty:
                proxy_label = 'Δq roof (kg/kg)'
                fig_latent = px.scatter(
                    latent_data,
                    x='latent_proxy',
                    y=temp_diff_choice,
                    color='season',
                    opacity=0.3,
                    title=f'Latent proxy: {proxy_label} vs temperature signal',
                )
                fig_latent.add_hline(y=0, line_dash='dash', line_color='red')
                fig_latent.update_layout(xaxis_title=proxy_label)
                st.plotly_chart(fig_latent, use_container_width=True)
                st.caption(
                    "Δq roof = specific humidity at 50cm minus at 2m. Positive = moister air "
                    "near the canopy, consistent with evapotranspiration; negative = drier."
                )
            else:
                st.info('Latent proxy data is unavailable for current filters.')

    with tab6:
        st.header("Step 6: Case Study Days")
        day_series = analyzer.df[temp_diff_choice]
        day_means = day_series.resample('D').mean()
        day_counts = day_series.resample('D').count()

        # Exclude thin-coverage days (e.g. 2-4 records) — their daily mean is
        # noise, not a real warm/cool case, and would otherwise dominate the
        # nsmallest/nlargest picks below. Threshold scales with whatever data
        # resolution is currently selected in the sidebar, so it still works
        # at "every 15 min" as well as full-resolution loads.
        if day_counts.empty or day_counts.max() == 0:
            st.warning('No daily data available for case-study selection.')
            return
        min_records = max(20, int(day_counts.max() * 0.5))
        well_covered = day_counts >= min_records
        day_means = day_means[well_covered].dropna()
        if day_means.empty:
            st.warning('No days with sufficient record coverage for case-study selection.')
            return

        coolest_days = day_means.nsmallest(min(3, len(day_means)))
        warmest_days = day_means.nlargest(min(3, len(day_means)))
        curated = []
        for ts, val in coolest_days.items():
            curated.append((ts.date(), f"{ts.date()} | cool case ({val:.3f}°C)"))
        for ts, val in warmest_days.items():
            curated.append((ts.date(), f"{ts.date()} | warm case ({val:.3f}°C)"))

        unique_curated = {}
        for day, label in curated:
            unique_curated[day] = label

        day_options = sorted(unique_curated.keys())
        selected_day = st.selectbox(
            'Curated case days',
            options=day_options,
            format_func=lambda x: unique_curated[x],
            index=0,
        )

        use_custom = st.toggle('Use custom date instead', value=False)
        if use_custom:
            selected_day = st.date_input(
                'Custom case day',
                value=day_options[-1],
                min_value=analyzer.df.index.min().date(),
                max_value=analyzer.df.index.max().date(),
            )

        day_df = analyzer.get_case_study_day(str(selected_day), temp_diff_col=temp_diff_choice)
        if day_df.empty:
            st.warning(f'No data found for {selected_day}.')
        else:
            st.caption(f"{len(day_df)} minute-level records for {selected_day}")
            fig_cs = make_subplots(specs=[[{"secondary_y": True}]])
            fig_cs.add_trace(
                go.Scatter(x=day_df['hour_decimal'], y=day_df[temp_diff_choice], name=f"{temp_diff_choice} [°C]", line=dict(color='#2E7D32', width=2)),
                secondary_y=False,
            )
            fig_cs.add_trace(
                go.Scatter(x=day_df['hour_decimal'], y=day_df['avg_global_radiation_greenroof'], name='SW in roof [W/m²]', line=dict(color='#FFC107', width=1.5, dash='dot')),
                secondary_y=True,
            )
            fig_cs.add_trace(
                go.Scatter(x=day_df['hour_decimal'], y=day_df['radiation_balance_greenroof'], name='Rnet roof [W/m²]', line=dict(color='#E65100', width=1.5)),
                secondary_y=True,
            )
            fig_cs.add_hline(y=0, line_dash='dash', line_color='red', secondary_y=False)
            fig_cs.update_layout(title=f'Case-day energy and signal overview ({selected_day})', height=420)
            fig_cs.update_yaxes(title_text=f'{temp_diff_choice} [°C]', secondary_y=False)
            fig_cs.update_yaxes(title_text='Radiation [W/m²]', secondary_y=True)
            st.plotly_chart(fig_cs, use_container_width=True)


def main():
    st.markdown('<p class="main-header">🌿 Bingen Green Roof Cooling Analysis</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Comprehensive analysis of green roof thermal performance</p>', unsafe_allow_html=True)

    view = st.sidebar.radio(
        "View", ["📊 Dashboard", "⚙️ Operations", "🔍 Query Data", "📄 Browse Data"], index=0
    )
    if view == "⚙️ Operations":
        render_operations_page()
        return
    if view == "🔍 Query Data":
        render_query_page()
        return
    if view == "📄 Browse Data":
        render_browse_page()
        return

    st.sidebar.title("⚙️ Analysis Settings")

    metadata_analyzer = get_metadata_analyzer()
    available_years = metadata_analyzer.get_available_years()
    min_ts, max_ts = metadata_analyzer.get_timestamp_bounds()

    min_ts_valid = min_ts is not None and not pd.isna(min_ts)
    max_ts_valid = max_ts is not None and not pd.isna(max_ts)
    if not min_ts_valid or not max_ts_valid:
        if available_years:
            fallback_start = datetime(min(available_years), 1, 1)
            fallback_end = datetime(max(available_years), 12, 31, 23, 59, 59)
        else:
            fallback_end = datetime.now()
            fallback_start = fallback_end - timedelta(days=30)
        min_ts = pd.Timestamp(fallback_start)
        max_ts = pd.Timestamp(fallback_end)
        if available_years:
            st.sidebar.warning(
                "Could not read full timestamp bounds. Using available year range as fallback defaults."
            )
        else:
            st.sidebar.warning("Could not read full timestamp bounds. Using last 30 days as fallback defaults.")

    yearly_mode = st.sidebar.toggle(
        "Yearly analysis mode",
        value=True,
        help="When enabled, query only one year from the database.",
    )

    selected_year = None
    if yearly_mode and available_years:
        selected_year = st.sidebar.selectbox(
            "Select year",
            options=available_years,
            index=len(available_years) - 1,
        )

    custom_timestamp_mode = st.sidebar.toggle(
        "Custom timestamp filter",
        value=False,
        help="Restrict query to a custom start/end timestamp range.",
    )

    start_ts = None
    end_ts = None
    if custom_timestamp_mode:
        default_start = min_ts.to_pydatetime()
        default_end = max_ts.to_pydatetime()

        start_date = st.sidebar.date_input(
            "Start date",
            value=default_start.date(),
            min_value=min_ts.date(),
            max_value=max_ts.date(),
        )
        start_time = st.sidebar.time_input("Start time", value=default_start.time())

        end_date = st.sidebar.date_input(
            "End date",
            value=default_end.date(),
            min_value=min_ts.date(),
            max_value=max_ts.date(),
        )
        end_time = st.sidebar.time_input("End time", value=default_end.time())

        start_ts = datetime.combine(start_date, start_time)
        end_ts = datetime.combine(end_date, end_time)

        if start_ts > end_ts:
            st.sidebar.error("Start timestamp must be earlier than end timestamp.")
            st.stop()

    show_yearly_display = st.sidebar.toggle(
        "Show yearly analysis display",
        value=False,
        help="Display yearly trends (record count and mean temperature differences).",
    )

    enable_energy_analysis = st.sidebar.toggle(
        "Enable energy analysis",
        value=False,
        help="Enable the full energy balance tab (may be slower on large datasets).",
    )

    _resolution_options = {
        "Every minute (full resolution, slowest)": 1,
        "Every 5 min (recommended)": 5,
        "Every 15 min (fastest)": 15,
    }
    _res_label = st.sidebar.selectbox(
        "Data resolution",
        options=list(_resolution_options.keys()),
        index=1,
        help="Coarser resolution loads and renders faster. Use 'Every minute' only for Case Study Day deep-dives.",
    )
    sample_n = _resolution_options[_res_label]

    start_ts_iso = pd.Timestamp(start_ts).isoformat() if start_ts is not None else None
    end_ts_iso = pd.Timestamp(end_ts).isoformat() if end_ts is not None else None

    n_desc = "year" if selected_year else "all years"
    res_desc = "full" if sample_n == 1 else f"every {sample_n} min"
    data_fingerprint = metadata_analyzer.get_data_fingerprint()
    # Load data
    try:
        with st.spinner(f"Loading data ({n_desc}, {res_desc})…"):
            analyzer = load_analyzer(
                selected_year,
                start_ts_iso,
                end_ts_iso,
                sample_n,
                data_fingerprint,
            )
    except DataLoadError as exc:
        st.error("Database read failed while loading dashboard data.")
        st.code(str(exc))
        st.info(
            "Open the ⚙️ Operations view (left sidebar) and run a pipeline update to "
            "(re)build the database, then reload this page."
        )
        st.stop()

    if analyzer.df is None or analyzer.df.empty:
        st.warning("No records match the selected filters. Adjust year/timestamp filters and try again.")
        st.stop()

    temp_diff_choice = st.sidebar.radio(
        "Temperature Difference Metric",
        ["temp_diff_1", "temp_diff_2"],
        help="temp_diff_1: Greenroof - Parkplatz Sensor 1\ntemp_diff_2: Greenroof - Parkplatz Sensor 2",
    )

    dashboard_mode = st.sidebar.radio(
        "Dashboard Mode",
        ["Analysis Mode", "Advanced Diagnostics (Environmental Conditions)"],
        index=0,
        help="Analysis mode follows a structured flow. Advanced mode keeps the full exploratory tab set.",
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Dataset Info")
    info = analyzer.analysis_results['dataset_info']
    st.sidebar.write(f"**Records:** {info['total_measurements']:,}")
    st.sidebar.write(f"**Date Range:** {info['date_range']}")
    active_year_text = str(info.get('year_filter')) if info.get('year_filter') is not None else "All"
    custom_range = info.get('custom_range', {})
    custom_range_text = (
        f"{custom_range.get('start')} → {custom_range.get('end')}"
        if custom_range.get('start') and custom_range.get('end')
        else "All"
    )
    st.sidebar.write(f"**Year Filter:** {active_year_text}")
    st.sidebar.write(f"**Timestamp Filter:** {custom_range_text}")

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📐 Thresholds")
    st.sidebar.markdown("""
    - **Meaningful Cooling:** < -0.2°C
    - **Strong Cooling:** < -1.2°C
    - **Within Error:** ±0.2°C
    - **Meaningful Warming:** > +0.2°C
    """)

    if yearly_mode:
        st.info(f"Yearly mode enabled: analyzing year {selected_year}")

    if show_yearly_display:
        st.subheader("📅 Yearly Analysis Display")
        yearly_summary = analyzer.df.groupby('year').agg(
            records=('temp_diff_2', 'size'),
            mean_temp_diff_1=('temp_diff_1', 'mean'),
            mean_temp_diff_2=('temp_diff_2', 'mean')
        ).reset_index()

        fig_yearly = go.Figure()
        fig_yearly.add_trace(go.Bar(
            x=yearly_summary['year'],
            y=yearly_summary['records'],
            name='Records',
            yaxis='y2',
            marker_color='#90CAF9'
        ))
        fig_yearly.add_trace(go.Scatter(
            x=yearly_summary['year'],
            y=yearly_summary['mean_temp_diff_1'],
            mode='lines+markers',
            name='Mean temp_diff_1',
            line=dict(color='#2E7D32', width=2)
        ))
        fig_yearly.add_trace(go.Scatter(
            x=yearly_summary['year'],
            y=yearly_summary['mean_temp_diff_2'],
            mode='lines+markers',
            name='Mean temp_diff_2',
            line=dict(color='#1565C0', width=2)
        ))
        fig_yearly.add_hline(y=0, line_dash="dash", line_color="red", line_width=1)
        fig_yearly.update_layout(
            title='Yearly Temperature Difference Trends',
            xaxis_title='Year',
            yaxis_title='Mean Temperature Difference [°C]',
            yaxis2=dict(title='Record Count', overlaying='y', side='right', showgrid=False),
            height=420,
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5)
        )
        st.plotly_chart(fig_yearly, use_container_width=True)
        st.dataframe(yearly_summary, use_container_width=True, hide_index=True)

    if dashboard_mode == "Analysis Mode":
        render_guided_dashboard(analyzer, temp_diff_choice)
        return
    
    # Main content tabs
    # Cooling stats computed once — referenced by tabs 1, 3, and 4
    cooling_stats = analyzer.get_cooling_by_condition(temp_diff_choice)

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11 = st.tabs([
        "📊 Cooling Effectiveness",
        "📦 Distribution Analysis", 
        "📈 Cooling Frequency",
        "🎯 Performance Matrix",
        "🍂 Seasonal Analysis",
        "⚡ Energy Balance",
        "🧭 Year-Season-Day/Night Thermal Signal",
        "💧 ET Diagnostic",
        "🌞 Albedo & Rnet",
        "🌱 Growing vs Cold",
        "🔍 Case Study Day",
    ])
    
    # =========================================================================
    # TAB 1: Cooling Effectiveness Bar Chart
    # =========================================================================
    with tab1:
        st.header("Cooling Effectiveness by Environmental Condition")
        st.markdown(f"Using **{temp_diff_choice}** for temperature difference calculations")
        
        if not cooling_stats.empty:
            # Create bar chart
            fig = go.Figure()
            
            colors = [CONDITION_COLORS.get(c, '#808080') for c in cooling_stats['condition']]
            
            fig.add_trace(go.Bar(
                y=cooling_stats['condition'],
                x=cooling_stats['mean_effect'],
                orientation='h',
                marker_color=colors,
                error_x=dict(type='data', array=cooling_stats['std_effect'], visible=True),
                text=[f"n={n:,}" for n in cooling_stats['count']],
                textposition='outside',
                hovertemplate="<b>%{y}</b><br>Mean: %{x:.3f}°C<br>Std: %{error_x.array:.3f}°C<extra></extra>"
            ))
            
            fig.add_vline(x=0, line_dash="dash", line_color="red", line_width=2)
            fig.add_vline(x=-0.2, line_dash="dot", line_color="blue", line_width=1, 
                         annotation_text="Meaningful cooling", annotation_position="top")
            
            fig.update_layout(
                title=f"Green Roof Cooling Effectiveness ({temp_diff_choice})",
                xaxis_title="Temperature Difference [°C]",
                yaxis_title="Environmental Condition",
                height=max(500, len(cooling_stats) * 40),
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Summary table
            st.subheader("📋 Summary Statistics")
            display_df = cooling_stats[['condition', 'mean_effect', 'std_effect', 'count', 'classification']].copy()
            display_df.columns = ['Condition', 'Mean Effect (°C)', 'Std Dev (°C)', 'Sample Size', 'Classification']
            st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    # =========================================================================
    # TAB 2: Distribution Box Plots
    # =========================================================================
    with tab2:
        st.header("Distribution of Cooling Effects")
        
        dist_data = analyzer.get_condition_distribution_data(temp_diff_choice)
        
        if not dist_data.empty:
            # Order by mean
            order = dist_data.groupby('enhanced_condition')[temp_diff_choice].mean().sort_values().index.tolist()
            
            fig = px.box(
                dist_data,
                x=temp_diff_choice,
                y='enhanced_condition',
                category_orders={'enhanced_condition': order},
                color='enhanced_condition',
                color_discrete_map=CONDITION_COLORS,
                title=f"Distribution of Temperature Differences by Condition ({temp_diff_choice})"
            )
            
            fig.add_vline(x=0, line_dash="dash", line_color="red", line_width=2)
            fig.add_vline(x=-0.2, line_dash="dot", line_color="blue", line_width=1)
            fig.add_vline(x=0.2, line_dash="dot", line_color="orange", line_width=1)
            
            fig.update_layout(
                xaxis_title="Temperature Difference [°C]",
                yaxis_title="Environmental Condition",
                height=max(500, len(order) * 45),
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            st.info("**Interpretation:** Box plots show median (line), interquartile range (box), and outliers. Red dashed line = 0°C (no difference). Blue/orange dotted lines = ±0.2°C (instrument uncertainty).")
    
    # =========================================================================
    # TAB 3: Cooling Frequency Analysis
    # =========================================================================
    with tab3:
        st.header("Cooling Frequency Analysis")
        st.markdown("How often does meaningful cooling occur under each condition?")
        
        if not cooling_stats.empty:
            # Prepare data for grouped bar chart
            freq_data = []
            for _, row in cooling_stats.iterrows():
                freq_data.append({'Condition': row['condition'], 'Category': 'Meaningful Cooling (<-0.2°C)', 'Frequency': row['meaningful_cooling_pct']})
                freq_data.append({'Condition': row['condition'], 'Category': 'Strong Cooling (<-1.2°C)', 'Frequency': row['strong_cooling_pct']})
                freq_data.append({'Condition': row['condition'], 'Category': 'Within Error (±0.2°C)', 'Frequency': row['within_error_pct']})
                freq_data.append({'Condition': row['condition'], 'Category': 'Meaningful Warming (>0.2°C)', 'Frequency': row['meaningful_warming_pct']})
            
            freq_df = pd.DataFrame(freq_data)
            
            fig = px.bar(
                freq_df,
                y='Condition',
                x='Frequency',
                color='Category',
                orientation='h',
                barmode='group',
                color_discrete_map={
                    'Meaningful Cooling (<-0.2°C)': '#2196F3',
                    'Strong Cooling (<-1.2°C)': '#009688',
                    'Within Error (±0.2°C)': '#FFC107',
                    'Meaningful Warming (>0.2°C)': '#F44336'
                },
                title="Cooling Frequency by Environmental Condition"
            )
            
            fig.update_layout(
                xaxis_title="Frequency (%)",
                yaxis_title="Environmental Condition",
                height=max(500, len(cooling_stats) * 50),
                legend_title="Category"
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Frequency table
            st.subheader("📋 Frequency Table")
            freq_table = cooling_stats[['condition', 'meaningful_cooling_pct', 'strong_cooling_pct', 
                                        'within_error_pct', 'meaningful_warming_pct', 'count']].copy()
            freq_table.columns = ['Condition', 'Meaningful Cooling %', 'Strong Cooling %', 
                                  'Within Error %', 'Meaningful Warming %', 'Sample Size']
            st.dataframe(freq_table, use_container_width=True, hide_index=True)
    
    # =========================================================================
    # TAB 4: Performance Matrix
    # =========================================================================
    with tab4:
        st.header("Performance Matrix")
        st.markdown("Comparing mean cooling effect vs. cooling frequency")
        
        if not cooling_stats.empty:
            fig = go.Figure()
            
            colors = [CONDITION_COLORS.get(c, '#808080') for c in cooling_stats['condition']]
            
            fig.add_trace(go.Scatter(
                x=cooling_stats['mean_effect'],
                y=cooling_stats['meaningful_cooling_pct'],
                mode='markers+text',
                marker=dict(size=15, color=colors, line=dict(width=2, color='black')),
                text=cooling_stats['condition'],
                textposition='top center',
                textfont=dict(size=9),
                hovertemplate="<b>%{text}</b><br>Mean Effect: %{x:.3f}°C<br>Cooling Freq: %{y:.1f}%<extra></extra>"
            ))
            
            # Reference lines
            fig.add_vline(x=-0.2, line_dash="dash", line_color="red", line_width=2,
                         annotation_text="Meaningful cooling threshold")
            fig.add_hline(y=50, line_dash="dash", line_color="blue", line_width=2,
                         annotation_text="50% frequency")
            
            # Quadrant labels
            fig.add_annotation(x=-1.5, y=80, text="✅ Excellent<br>Performance", 
                             showarrow=False, font=dict(size=12, color="green"))
            fig.add_annotation(x=0.5, y=20, text="❌ Poor<br>Performance", 
                             showarrow=False, font=dict(size=12, color="red"))
            
            fig.update_layout(
                title=f"Performance Matrix: Cooling Effect vs. Frequency ({temp_diff_choice})",
                xaxis_title="Mean Cooling Effect [°C]",
                yaxis_title="Meaningful Cooling Frequency (%)",
                height=600
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            st.info("""
            **Interpretation:**
            - **Top-left quadrant:** Best performance (strong cooling, high frequency)
            - **Bottom-right quadrant:** Worst performance (warming, low cooling frequency)
            - Red dashed line: -0.2°C threshold for meaningful cooling
            - Blue dashed line: 50% frequency threshold
            """)
    
    # =========================================================================
    # TAB 5: Seasonal Analysis
    # =========================================================================
    with tab5:
        st.header("Seasonal Variation in Stress Conditions")
        
        seasonal_data = analyzer.get_seasonal_stress_data(temp_diff_choice)
        
        if not seasonal_data.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig1 = px.bar(
                    seasonal_data,
                    x='season',
                    y='mean_effect',
                    color='condition',
                    barmode='group',
                    color_discrete_map=CONDITION_COLORS,
                    title="Mean Cooling Effect by Season"
                )
                fig1.add_hline(y=0, line_dash="dash", line_color="red")
                fig1.add_hline(y=-0.2, line_dash="dot", line_color="orange")
                fig1.update_layout(
                    xaxis_title="Season",
                    yaxis_title="Mean Cooling Effect [°C]",
                    height=450
                )
                st.plotly_chart(fig1, use_container_width=True)
            
            with col2:
                fig2 = px.bar(
                    seasonal_data,
                    x='season',
                    y='meaningful_cooling_pct',
                    color='condition',
                    barmode='group',
                    color_discrete_map=CONDITION_COLORS,
                    title="Meaningful Cooling Frequency by Season"
                )
                fig2.update_layout(
                    xaxis_title="Season",
                    yaxis_title="Meaningful Cooling Frequency (%)",
                    height=450
                )
                st.plotly_chart(fig2, use_container_width=True)
            
            # Seasonal summary table
            st.subheader("📋 Seasonal Summary")
            st.dataframe(seasonal_data, use_container_width=True, hide_index=True)
        else:
            st.warning("Insufficient data for seasonal stress condition analysis.")
    
    # =========================================================================
    # TAB 6: Energy Balance Analysis (NEW)
    # =========================================================================
    with tab6:
        st.header("⚡ Energy Balance Analysis")
        st.markdown("""
        This analysis explores **why warming occurs instead of cooling** by examining the 
        energy fluxes at the parkplatz (reference) site throughout the day.
        """)

        if not enable_energy_analysis:
            st.info(
                "Energy analysis is disabled to keep the app responsive. "
                "Enable it in the sidebar when you need this section."
            )
            return
        
        # Get hourly data
        hourly = analyzer.get_hourly_energy_analysis()
        
        # Chart 1: Hourly Energy Pattern with Temperature Difference
        st.subheader("1️⃣ Hourly Energy Pattern vs Temperature Difference")
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Energy traces (right y-axis)
        fig.add_trace(
            go.Scatter(x=hourly['hour'], y=hourly['energy_from_air_parkplatz'],
                      name="Energy from Air (IR1 + σεT⁴)", line=dict(color='#FF6B6B', width=2)),
            secondary_y=True
        )
        fig.add_trace(
            go.Scatter(x=hourly['hour'], y=hourly['energy_from_surface_parkplatz'],
                      name="Energy from Surface (IR2 + σεT⁴)", line=dict(color='#4ECDC4', width=2)),
            secondary_y=True
        )
        fig.add_trace(
            go.Scatter(x=hourly['hour'], y=hourly['radiation_balance_parkplatz'],
                      name="Net Radiation Balance", line=dict(color='#45B7D1', width=2, dash='dash')),
            secondary_y=True
        )
        fig.add_trace(
            go.Scatter(x=hourly['hour'], y=hourly['avg_global_radiation_greenroof'],
                      name="Global Radiation (Greenroof)", line=dict(color='#FFE66D', width=2)),
            secondary_y=True
        )
        
        # Temperature difference traces (left y-axis)
        fig.add_trace(
            go.Scatter(x=hourly['hour'], y=hourly['temp_diff_1'],
                      name="temp_diff_1", line=dict(color='#2E7D32', width=3)),
            secondary_y=False
        )
        fig.add_trace(
            go.Scatter(x=hourly['hour'], y=hourly['temp_diff_2'],
                      name="temp_diff_2", line=dict(color='#1565C0', width=3)),
            secondary_y=False
        )
        
        # Add zero line for temp diff
        fig.add_hline(y=0, line_dash="dash", line_color="red", line_width=1, secondary_y=False)
        
        fig.update_layout(
            title="Hourly Energy Fluxes and Temperature Differences",
            height=500,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
        )
        fig.update_xaxes(title_text="Hour of Day", dtick=2)
        fig.update_yaxes(title_text="Temperature Difference [°C]", secondary_y=False)
        fig.update_yaxes(title_text="Energy Flux [W/m²]", secondary_y=True)
        
        st.plotly_chart(fig, use_container_width=True)
        
        st.info("""
        **Key Observations:**
        - When **radiation balance** is high (midday), there's more energy input → potential warming
        - When **radiation balance** is low/negative (night), energy is lost → potential cooling
        - **temp_diff > 0** means greenroof is warmer than parkplatz
        - **temp_diff < 0** means greenroof is cooler (desired cooling effect)
        """)
        
        # Chart 2: Correlation scatter
        st.subheader("2️⃣ Radiation Balance vs Temperature Difference")
        
        corr_data = analyzer.get_radiation_vs_tempdiff_correlation()
        
        # Sample for performance (max 10000 points)
        if len(corr_data) > 10000:
            sample_data = corr_data.sample(n=10000, random_state=42)
        else:
            sample_data = corr_data
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_scatter1 = px.scatter(
                sample_data,
                x='radiation_balance_parkplatz',
                y=temp_diff_choice,
                opacity=0.3,
                color_discrete_sequence=['#2196F3'],
                title=f"Radiation Balance vs {temp_diff_choice}"
            )
            fig_scatter1.add_hline(y=0, line_dash="dash", line_color="red")
            fig_scatter1.add_vline(x=0, line_dash="dash", line_color="gray")
            fig_scatter1.update_layout(
                xaxis_title="Radiation Balance [W/m²]",
                yaxis_title=f"{temp_diff_choice} [°C]",
                height=400
            )
            st.plotly_chart(fig_scatter1, use_container_width=True)
        
        with col2:
            # Calculate correlation
            corr = corr_data['radiation_balance_parkplatz'].corr(corr_data[temp_diff_choice])
            
            # Binned averages
            binned_input = corr_data.assign(
                rad_bin=pd.cut(corr_data['radiation_balance_parkplatz'], bins=20)
            )
            binned = binned_input.groupby('rad_bin')[temp_diff_choice].mean().reset_index()
            binned['rad_mid'] = binned['rad_bin'].apply(lambda x: x.mid if pd.notna(x) else np.nan)
            binned = binned.dropna()
            
            fig_trend = px.line(
                binned,
                x='rad_mid',
                y=temp_diff_choice,
                markers=True,
                title=f"Trend: Binned Radiation vs {temp_diff_choice}"
            )
            fig_trend.add_hline(y=0, line_dash="dash", line_color="red")
            fig_trend.update_layout(
                xaxis_title="Radiation Balance [W/m²]",
                yaxis_title=f"Mean {temp_diff_choice} [°C]",
                height=400
            )
            st.plotly_chart(fig_trend, use_container_width=True)
        
        st.metric("Correlation Coefficient", f"{corr:.3f}")
        
        # Chart 3: Seasonal energy patterns
        st.subheader("3️⃣ Seasonal Energy Patterns")
        
        seasonal = analyzer.get_seasonal_energy_analysis()
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_seasonal_energy = px.bar(
                seasonal,
                x='season',
                y=['energy_from_air_parkplatz', 'energy_from_surface_parkplatz', 'radiation_balance_parkplatz'],
                barmode='group',
                title="Seasonal Energy Fluxes",
                labels={'value': 'Energy [W/m²]', 'variable': 'Flux Type'}
            )
            fig_seasonal_energy.update_layout(height=400)
            st.plotly_chart(fig_seasonal_energy, use_container_width=True)
        
        with col2:
            fig_seasonal_temp = px.bar(
                seasonal,
                x='season',
                y=['temp_diff_1', 'temp_diff_2'],
                barmode='group',
                title="Seasonal Temperature Differences",
                color_discrete_map={'temp_diff_1': '#2E7D32', 'temp_diff_2': '#1565C0'}
            )
            fig_seasonal_temp.add_hline(y=0, line_dash="dash", line_color="red")
            fig_seasonal_temp.update_layout(
                yaxis_title="Temperature Difference [°C]",
                height=400
            )
            st.plotly_chart(fig_seasonal_temp, use_container_width=True)
        
        # Explanation box
        st.markdown("---")
        st.subheader("🔍 Why is Warming Occurring?")
        
        avg_td1 = analyzer.df['temp_diff_1'].mean()
        avg_td2 = analyzer.df['temp_diff_2'].mean()
        avg_rad = analyzer.df['radiation_balance_parkplatz'].mean()
        
        st.markdown(f"""
        **Overall Averages:**
        - Mean temp_diff_1: **{avg_td1:.3f}°C** (Greenroof {'warmer' if avg_td1 > 0 else 'cooler'} than Parkplatz Sensor 1)
        - Mean temp_diff_2: **{avg_td2:.3f}°C** (Greenroof {'warmer' if avg_td2 > 0 else 'cooler'} than Parkplatz Sensor 2)
        - Mean Radiation Balance: **{avg_rad:.1f} W/m²**
        
        **Possible Explanations for Warming:**
        1. **Daytime solar gain dominates:** High radiation balance during day causes warming that outweighs nighttime cooling
        2. **Thermal mass effect:** Green roof substrate retains heat longer
        3. **Reduced convective cooling:** Vegetation may trap air, reducing wind-driven heat loss
        4. **Sensor placement differences:** Location differences between greenroof and parkplatz sensors
        5. **Evapotranspiration limited:** Low soil moisture reduces evaporative cooling effect
        
        **Next steps:** Filter by time of day, season, or soil moisture to identify when cooling actually occurs.
        """)

    # =========================================================================
    # TAB 7: Year-Season-Day/Night Thermal Signal (full period vs dual-level only)
    # =========================================================================
    with tab7:
        st.header("Year-Season-Day/Night Thermal Signal")
        scope_choice = st.radio(
            "Summary scope",
            options=["full_period", "dual_level_only"],
            horizontal=True,
            format_func=lambda x: "Full period" if x == "full_period" else "Dual-level only",
            key="thesis_scope",
        )

        thesis_summary = analyzer.get_thesis_summary(scope=scope_choice, temp_diff_col=temp_diff_choice)

        if thesis_summary.empty:
            st.warning("No records available for the selected thesis summary scope.")
        else:
            fig_summary = px.bar(
                thesis_summary,
                x='season',
                y='temp_signal_mean',
                color='day_night',
                facet_col='year',
                barmode='group',
                title=f"Mean {temp_diff_choice} by Year, Season, and Day/Night",
            )
            fig_summary.add_hline(y=0, line_dash="dash", line_color="red")
            fig_summary.update_layout(height=500)
            st.plotly_chart(fig_summary, use_container_width=True)

            st.caption(
                "delta_q_roof_mean = specific humidity at 50cm minus at 2m (kg/kg). "
                "Positive = moister air near the canopy (evapotranspiration signal); negative = drier."
            )
            st.dataframe(thesis_summary, use_container_width=True, hide_index=True)

    # =========================================================================
    # TAB 8: ET Diagnostic (dual-level only)
    # =========================================================================
    with tab8:
        st.header("ET Diagnostic: ET Proxy vs Temperature Signal")
        st.caption("This diagnostic is restricted to rows with dual-level greenroof measurements.")
        st.caption(
            "Δq roof = specific humidity at 50cm minus at 2m. Positive = moister air near the "
            "canopy than higher up, consistent with evapotranspiration; negative = drier near "
            "the canopy. (ΔRH was dropped as a proxy here — it moves with temperature as well as "
            "moisture, so it doesn't reliably track the same thing Δq does.)"
        )

        scatter_data = analyzer.get_et_scatter_data(temp_diff_col=temp_diff_choice)
        if scatter_data.empty:
            st.warning("No dual-level rows are available in the selected date range.")
        else:
            fig_et = px.scatter(
                scatter_data,
                x='delta_q_roof',
                y=temp_diff_choice,
                color='season',
                opacity=0.35,
                title=f"ET Proxy vs {temp_diff_choice}",
                labels={
                    'delta_q_roof': 'Δq roof (50cm - 2m) [kg/kg]',
                    temp_diff_choice: f'{temp_diff_choice} [°C]'
                }
            )
            fig_et.add_hline(y=0, line_dash="dash", line_color="red")
            fig_et.update_layout(height=520)
            st.plotly_chart(fig_et, use_container_width=True)

    # =========================================================================
    # TAB 9: Albedo and Radiation Balance
    # =========================================================================
    with tab9:
        st.header("Albedo and Net Radiation Comparison")
        monthly_data = analyzer.get_monthly_albedo_rnet()

        if monthly_data.empty:
            st.warning("No monthly data available for albedo and radiation diagnostics.")
        else:
            col1, col2 = st.columns(2)

            with col1:
                fig_albedo = px.line(
                    monthly_data,
                    x='month_label',
                    y=['albedo_greenroof', 'albedo_parkplatz'],
                    color_discrete_map={
                        'albedo_greenroof': '#2E7D32',
                        'albedo_parkplatz': '#1565C0',
                    },
                    title='Monthly Albedo (Roof vs Parkplatz)',
                )
                fig_albedo.update_layout(
                    xaxis_title='Month',
                    yaxis_title='Albedo',
                    height=420,
                )
                st.plotly_chart(fig_albedo, use_container_width=True)

            with col2:
                fig_rnet = px.line(
                    monthly_data,
                    x='month_label',
                    y=['rnet_greenroof', 'rnet_parkplatz'],
                    color_discrete_map={
                        'rnet_greenroof': '#E65100',
                        'rnet_parkplatz': '#00838F',
                    },
                    title='Monthly Net Radiation (Roof vs Parkplatz)',
                )
                fig_rnet.add_hline(y=0, line_dash='dash', line_color='red')
                fig_rnet.update_layout(
                    xaxis_title='Month',
                    yaxis_title='Net radiation [W/m²]',
                    height=420,
                )
                st.plotly_chart(fig_rnet, use_container_width=True)

            st.dataframe(
                monthly_data[['month_label', 'period', 'dual_level_share', 'albedo_greenroof', 'albedo_parkplatz', 'rnet_greenroof', 'rnet_parkplatz']],
                use_container_width=True,
                hide_index=True,
            )


        # =========================================================================
        # TAB 10: Growing vs Cold Period Analysis (thesis Level 2)
        # =========================================================================
        with tab10:
            st.header("Growing vs Cold Period Analysis")
            st.markdown(
                "**Growing season** (Apr–Sep) vs **Cold period** (Oct–Mar) — "
                "thesis Level 2 breakdown across years and day/night."
            )

            period_summary = analyzer.get_period_summary(temp_diff_col=temp_diff_choice)

            if period_summary.empty:
                st.warning("No period data available for the selected date range.")
            else:
                col1, col2 = st.columns(2)

                with col1:
                    fig_period = px.bar(
                        period_summary,
                        x='period',
                        y='temp_signal_mean',
                        color='day_night',
                        facet_col='year',
                        barmode='group',
                        color_discrete_map={'day': '#F4A261', 'night': '#264653'},
                        title=f"Mean {temp_diff_choice} by Period and Day/Night",
                    )
                    fig_period.add_hline(y=0, line_dash="dash", line_color="red")
                    fig_period.update_layout(
                        yaxis_title="Temperature Signal [°C]",
                        height=450,
                    )
                    st.plotly_chart(fig_period, use_container_width=True)

                with col2:
                    fig_q_period = px.bar(
                        period_summary,
                        x='period',
                        y='delta_q_roof_mean',
                        color='day_night',
                        facet_col='year',
                        barmode='group',
                        color_discrete_map={'day': '#F4A261', 'night': '#264653'},
                        title="Mean Δq Roof (ET proxy) by Period and Day/Night",
                    )
                    fig_q_period.add_hline(y=0, line_dash="dash", line_color="gray")
                    fig_q_period.update_layout(
                        yaxis_title="Δq Roof [kg/kg]",
                        height=450,
                    )
                    st.plotly_chart(fig_q_period, use_container_width=True)
                    st.caption(
                        "Positive Δq = moister air near the canopy (50cm) than at 2m — "
                        "consistent with evapotranspiration; negative = drier near the canopy."
                    )

                st.subheader("Overall Period Comparison (all years combined)")
                overall = period_summary.groupby('period').agg(
                    temp_signal_mean=('temp_signal_mean', 'mean'),
                    delta_q_roof_mean=('delta_q_roof_mean', 'mean'),
                    rnet_greenroof_mean=('rnet_greenroof_mean', 'mean'),
                    albedo_greenroof_mean=('albedo_greenroof_mean', 'mean'),
                    total_records=('records', 'sum'),
                ).reset_index()
                overall['temp_signal_mean'] = overall['temp_signal_mean'].round(3)
                overall['delta_q_roof_mean'] = overall['delta_q_roof_mean'].round(5)
                st.dataframe(overall, use_container_width=True, hide_index=True)

                st.subheader("Full Year × Period × Day/Night Breakdown")
                st.dataframe(period_summary, use_container_width=True, hide_index=True)

        # =========================================================================
        # TAB 11: Case Study Day Picker (thesis Level 5)
        # =========================================================================
        with tab11:
            st.header("Case Study Day Analysis")
            st.markdown(
                "Examine a single day's minute-level energy flux, ET proxy and temperature signal. "
                "Target warm summer days or cool post-rain days for thesis Level 5."
            )

            data_min_date = analyzer.df.index.min().date()
            data_max_date = analyzer.df.index.max().date()

            case_date = st.date_input(
                "Select a day",
                value=data_max_date,
                min_value=data_min_date,
                max_value=data_max_date,
                key="case_study_date",
            )

            day_df = analyzer.get_case_study_day(str(case_date), temp_diff_col=temp_diff_choice)

            if day_df.empty:
                st.warning(f"No data found for {case_date}. Try a different date.")
            else:
                st.caption(f"{len(day_df)} minute-level records for {case_date}")

                # Chart 1: temperature signal vs radiation balance (dual-axis)
                fig_cs1 = make_subplots(specs=[[{"secondary_y": True}]])
                fig_cs1.add_trace(
                    go.Scatter(
                        x=day_df['hour_decimal'], y=day_df[temp_diff_choice],
                        name=f"{temp_diff_choice} [°C]", line=dict(color='#2E7D32', width=2),
                    ),
                    secondary_y=False,
                )
                fig_cs1.add_trace(
                    go.Scatter(
                        x=day_df['hour_decimal'], y=day_df['avg_global_radiation_greenroof'],
                        name="Global Radiation [W/m²]", line=dict(color='#FFC107', width=1.5, dash='dot'),
                    ),
                    secondary_y=True,
                )
                fig_cs1.add_trace(
                    go.Scatter(
                        x=day_df['hour_decimal'], y=day_df['radiation_balance_greenroof'],
                        name="R_net Greenroof [W/m²]", line=dict(color='#E65100', width=1.5),
                    ),
                    secondary_y=True,
                )
                fig_cs1.add_hline(y=0, line_dash="dash", line_color="red", secondary_y=False)
                fig_cs1.update_layout(
                    title=f"Temperature Signal and Radiation — {case_date}",
                    height=420,
                    xaxis_title="Hour of Day",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
                )
                fig_cs1.update_yaxes(title_text=f"{temp_diff_choice} [°C]", secondary_y=False)
                fig_cs1.update_yaxes(title_text="Radiation [W/m²]", secondary_y=True)
                st.plotly_chart(fig_cs1, use_container_width=True)

                # Chart 2: ET proxy and sensible heat proxy
                if 'delta_q_roof' in day_df.columns and 'delta_t_roof' in day_df.columns:
                    day_reset = day_df.reset_index()
                    col1, col2 = st.columns(2)

                    with col1:
                        fig_et_day = px.line(
                            day_reset,
                            x='hour_decimal',
                            y='delta_q_roof',
                            title="Δq Roof (ET proxy)",
                            labels={'hour_decimal': 'Hour of Day', 'delta_q_roof': 'Δq [kg/kg]'},
                        )
                        fig_et_day.add_hline(y=0, line_dash="dash", line_color="gray")
                        fig_et_day.update_layout(height=320)
                        st.plotly_chart(fig_et_day, use_container_width=True)

                    with col2:
                        fig_dt_day = px.line(
                            day_reset,
                            x='hour_decimal',
                            y='delta_t_roof',
                            title="ΔT Roof (sensible heat proxy)",
                            labels={'hour_decimal': 'Hour of Day', 'delta_t_roof': 'ΔT [°C]'},
                            color_discrete_sequence=['#F44336'],
                        )
                        fig_dt_day.add_hline(y=0, line_dash="dash", line_color="gray")
                        fig_dt_day.update_layout(height=320)
                        st.plotly_chart(fig_dt_day, use_container_width=True)

                # Key daily metrics
                mean_signal = day_df[temp_diff_choice].mean()
                mean_soil_series = day_df.get('avg_soil_moisture_greenroof', pd.Series(dtype=float))
                mean_soil = mean_soil_series.mean() if not mean_soil_series.empty else float('nan')
                col1, col2, col3 = st.columns(3)
                col1.metric(f"Mean {temp_diff_choice} [°C]", f"{mean_signal:.3f}")
                if not pd.isna(mean_soil):
                    col2.metric("Mean soil moisture [vol%]", f"{mean_soil:.1f}")
                col3.metric("Minutes with data", len(day_df))

if __name__ == "__main__":
    main()

