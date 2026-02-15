# Optimized Dashboard for faster page load times
import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pvlib
from pvlib.location import Location
import logging

# Setup logging - minimized
log_dir = "logs/dashboard"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(os.path.join(log_dir, f"dashboard_{datetime.now():%Y%m%d}.log"))]
)
for logger in ('urllib3', 'requests', 'streamlit', 'matplotlib', 'PIL', 'watchdog'):
    logging.getLogger(logger).setLevel(logging.ERROR)

# Page config
st.set_page_config(page_title="Solar Data Monitor", page_icon="☀️", layout="wide", initial_sidebar_state="collapsed")

# CSS
st.markdown("""
<style>
.main-header{font-size:2.5rem;font-weight:bold;color:#1f77b4;text-align:center;margin-bottom:1rem}
.station-header{font-size:1.8rem;font-weight:bold;color:#2c3e50;text-align:center;margin:1rem 0}
.subsection-header{font-size:1.2rem;font-weight:bold;color:#2c3e50;margin:1rem 0 .5rem;padding-bottom:.3rem;border-bottom:2px solid #e9ecef}
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="main-header">Solar Data Monitor</h1>', unsafe_allow_html=True)
st.markdown('<p style="text-align:center;font-size:1.2rem;color:#666">Solar Radiation & Meteorological Data (Last 72 Hours)</p>', unsafe_allow_html=True)

# Caching
@st.cache_data(ttl=30)
def get_stations():
    """Get available stations - CACHED"""
    interim_dir = Path("data/interim")
    return sorted([d.name.lower() for d in interim_dir.iterdir() if d.is_dir()]) if interim_dir.exists() else []

@st.cache_data(ttl=600)
def get_metadata(station_dirs):
    """Get station metadata - CACHED"""
    location_csv = Path("data/interim/INPESONDA_Stations.csv")
    if not location_csv.exists():
        return pd.DataFrame(columns=['station', 'latitude', 'longitude'])
    df = pd.read_csv(location_csv, dtype={'station': str})
    df['station_normalized'] = df['station'].str.strip().str.lower()
    return df[df['station_normalized'].isin(station_dirs)].drop_duplicates('station_normalized').sort_values('station').reset_index(drop=True)

@st.cache_data(ttl=15)
def get_latest_files(station):
    """Get latest parquet files - CACHED"""
    station_path = Path(f"data/interim/{station}")
    if not station_path.exists():
        return {}
    latest = {}
    for f in station_path.glob("*.parquet"):
        dtype = f.name.split('_')[-3] if '_' in f.name else None
        if dtype and (dtype not in latest or f.stat().st_mtime > Path(latest[dtype]).stat().st_mtime):
            latest[dtype] = str(f)
    return latest

def filter_72h(df, ts_col='TIMESTAMP'):
    """Filter to last 72h"""
    if ts_col not in df.columns:
        return df
    if not pd.api.types.is_datetime64_any_dtype(df[ts_col]):
        df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')
    return df[df[ts_col] >= datetime.now() - timedelta(hours=72)].copy()

@st.cache_data(ttl=15)
def get_data_status(station):
    """Get data freshness - CACHED"""
    files = get_latest_files(station)
    if not files:
        return ('red', None, None)
    latest_ts = None
    for fpath in files.values():
        try:
            df = pd.read_parquet(fpath, columns=['TIMESTAMP'])
            if not df.empty:
                df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
                file_max = df['TIMESTAMP'].max()
                if pd.notna(file_max) and (latest_ts is None or file_max > latest_ts):
                    latest_ts = file_max
        except:
            continue
    if latest_ts is None:
        return ('red', None, None)
    hours = (datetime.now() - latest_ts).total_seconds() / 3600
    return (('green' if hours < 1 else 'yellow' if hours <= 72 else 'red'), hours, latest_ts)

@st.cache_data(ttl=15)
def load_station_data(station):
    """Load station data - CACHED"""
    files = get_latest_files(station)
    if not files:
        return None
    data = {}
    for dtype, fpath in files.items():
        try:
            df = pd.read_parquet(fpath)
            if df.empty:
                continue
            # Fast timestamp conversion
            if 'TIMESTAMP' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['TIMESTAMP']):
                df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce', format='ISO8601')
            # Numeric conversion - vectorized
            for col in df.select_dtypes(include=['object']).columns:
                if col not in {'TIMESTAMP', 'source_file', 'station', 'data_type', 'file_path'}:
                    df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float')
            # Filter to 72h
            df_recent = filter_72h(df.copy())
            if df_recent.empty and 'TIMESTAMP' in df.columns:
                latest = df['TIMESTAMP'].max()
                if pd.notna(latest):
                    st.warning(f"⚠️ {dtype} data is {(datetime.now() - latest).total_seconds() / 3600:.0f}h old")
                df_recent = df
            # Add metadata
            df_recent = df_recent.assign(source_file=Path(fpath).name, station=station, data_type=dtype, file_path=fpath)
            data[dtype] = df_recent
        except Exception as e:
            st.info(f"ℹ️ Could not load {dtype}: {str(e)[:50]}")
    return data if data else None

@st.cache_data(ttl=600)
def calc_clearsky(df, lat, lon, alt=0, tz='America/Sao_Paulo'):
    """Calculate clear sky - CACHED"""
    if any(col in df.columns for col in ['clearsky_GHI', 'clearsky_DNI', 'clearsky_DHI']):
        return df
    df = df.copy()
    if 'TIMESTAMP' not in df.columns:
        return df
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce').dropna()
    if df.empty:
        return df
    if df['TIMESTAMP'].dt.tz is None:
        df['TIMESTAMP'] = df['TIMESTAMP'].dt.tz_localize('UTC').dt.tz_convert(tz)
    try:
        clearsky = Location(lat, lon, tz=tz, altitude=alt).get_clearsky(df.set_index('TIMESTAMP').index)
        df['clearsky_GHI'], df['clearsky_DNI'], df['clearsky_DHI'] = clearsky['ghi'].values, clearsky['dni'].values, clearsky['dhi'].values
    except:
        pass
    return df

def get_vars(df):
    """Get numeric variables"""
    exclude = {'TIMESTAMP', 'source_file', 'station', 'data_type', 'file_path', 'Id', 'Min', 'RECORD', 'Year', 'Jday'}
    return [c for c in df.select_dtypes(include=[np.number]).columns if c not in exclude]

def plot_vars(df, vars, title, height=300):
    """Plot variables"""
    if not vars or 'TIMESTAMP' not in df.columns:
        st.warning(f"⚠️ Cannot plot {title}")
        return False
    vars = [v for v in vars if v in df.columns]
    if not vars:
        return False
    plot_df = df[['TIMESTAMP'] + vars].dropna(subset=['TIMESTAMP']).set_index('TIMESTAMP')
    if len(plot_df) > 1000:
        plot_df = plot_df.iloc[::len(plot_df)//1000]
    if plot_df.empty:
        return False
    st.line_chart(plot_df, height=height)
    st.caption(f"📊 {title}: {', '.join(vars)} ({len(plot_df)} pts)")
    return True

# Main logic
stations = get_stations()
if not stations:
    st.error("❌ No stations in data/interim")
    st.markdown("""
    ## 🚀 Fresh Installation
    Trigger data pipeline via Airflow:
    ```bash
    docker-compose exec airflow-apiserver airflow dags trigger refresh_data_pipeline
    ```
    """)
    st.stop()

metadata = get_metadata(stations)
st.markdown("---")
st.markdown('<h3 style="text-align:center;color:#2c3e50;margin-bottom:1rem">📡 Select Station</h3>', unsafe_allow_html=True)
st.markdown('<div style="text-align:center;margin-bottom:.8rem;font-size:.9rem;color:#495057;background:#f8f9fa;padding:.5rem;border-radius:.3rem"><span style="margin:0 15px"><strong>🟢 LIVE</strong> (&lt;1h)</span><span style="margin:0 15px"><strong>🟡 RECENT</strong> (1-72h)</span><span style="margin:0 15px"><strong>🔴 OLD</strong> (&gt;72h)</span></div>', unsafe_allow_html=True)

status_map = {s: get_data_status(s) for s in stations}

def fmt_station(s):
    status, hrs, _ = status_map[s]
    ind = {'green': '🟢', 'yellow': '🟡', 'red': '🔴'}.get(status, '⚪')
    time_info = f"({int(hrs * 60)}min)" if hrs and hrs < 1 else f"({hrs:.1f}h)" if hrs and hrs <= 24 else f"({hrs/24:.1f}d)" if hrs else "(no data)"
    return f"{ind} {s.upper()} {time_info}"

selected = st.radio("Select:", stations, format_func=fmt_station, horizontal=True, label_visibility="collapsed")

counts = {'green': sum(1 for s in status_map.values() if s[0] == 'green'),
          'yellow': sum(1 for s in status_map.values() if s[0] == 'yellow'),
          'red': sum(1 for s in status_map.values() if s[0] == 'red')}
st.markdown(f'<div style="text-align:center;margin-top:.5rem;font-size:.85rem;color:#666">Summary: <strong style="color:#28a745">{counts["green"]} Live</strong> • <strong style="color:#ffc107">{counts["yellow"]} Recent</strong> • <strong style="color:#dc3545">{counts["red"]} Old</strong></div>', unsafe_allow_html=True)
st.markdown("---")

status, hrs, ts = status_map[selected]
status_emoji = {'green': '🟢', 'yellow': '🟡', 'red': '🔴'}.get(status, '⚪')
st.markdown(f'<h2 class="station-header">{status_emoji} Station: {selected.upper()}</h2>', unsafe_allow_html=True)

with st.spinner(f"Loading {selected.upper()}..."):
    data_dict = load_station_data(selected)

if data_dict:
    st.header("📊 Data Overview")

    # Find latest timestamp
    latest_overall = max((df['TIMESTAMP'].max() for df in data_dict.values() if 'TIMESTAMP' in df.columns and pd.api.types.is_datetime64_any_dtype(df['TIMESTAMP']) and pd.notna(df['TIMESTAMP'].max())), default=None)

    if latest_overall:
        hrs_old = (datetime.now() - latest_overall).total_seconds() / 3600
        bg, border, icon, lbl = (("#d4edda", "#28a745", "🟢", "LIVE") if hrs_old < 1 else
                                   ("#fff3cd", "#ffc107", "🟡", "RECENT") if hrs_old <= 72 else
                                   ("#f8d7da", "#dc3545", "🔴", "OLD"))
        st.markdown(f'<div style="background:{bg};padding:1rem;border-radius:.5rem;border-left:5px solid {border};margin-bottom:1rem;text-align:center"><div style="font-size:.9rem;font-weight:bold;color:#495057;margin-bottom:.3rem">{icon} STATUS: {lbl}</div><div style="font-size:1.5rem;font-weight:bold;color:#212529">📅 {latest_overall:%Y-%m-%d} ⏰ {latest_overall:%H:%M:%S}</div><div style="font-size:.85rem;color:#6c757d;margin-top:.3rem">Last update: {hrs_old:.1f}h ago ({hrs_old/24:.1f} days)</div></div>', unsafe_allow_html=True)

    # Metrics
    cols = st.columns(len(data_dict))
    for i, (dtype, df) in enumerate(data_dict.items()):
        with cols[i]:
            time_range = "N/A"
            if 'TIMESTAMP' in df.columns and pd.api.types.is_datetime64_any_dtype(df['TIMESTAMP']):
                tmin, tmax = df['TIMESTAMP'].min(), df['TIMESTAMP'].max()
                if pd.notna(tmin) and pd.notna(tmax):
                    time_range = f"{tmin:%m-%d %H:%M} to {tmax:%m-%d %H:%M}"
            st.metric(f"{dtype} Data", f"{len(df):,}", f"{len(df.columns)} cols")
            st.caption(f"Range: {time_range}")

    # Plots
    st.header("📈 Real-time Visualization")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("☀️ Solar (SD)")
        if 'SD' in data_dict:
            df = data_dict['SD'].copy()
            row = metadata[metadata['station'].str.lower() == selected.lower()]
            if not row.empty:
                try:
                    df = calc_clearsky(df, row['latitude'].values[0], row['longitude'].values[0])
                except:
                    pass
            vars = get_vars(df)
            if vars and 'TIMESTAMP' in df.columns:
                st.markdown('<p class="subsection-header">Solar Radiation</p>', unsafe_allow_html=True)
                solar = [v for v in vars if any(x in v.lower() for x in ['glo', 'dir', 'dif']) and any(x in v.lower() for x in ['avg', 'std'])]
                cs = [v for v in ['clearsky_GHI', 'clearsky_DNI', 'clearsky_DHI'] if v in df.columns]
                all_solar = solar + cs
                if all_solar:
                    defaults = [v for v in all_solar if ('avg' in v.lower() and any(x in v.lower() for x in ['glo', 'dir', 'dif'])) or v == 'clearsky_GHI']
                    sel = st.multiselect("Select:", all_solar, defaults if defaults else all_solar[:2], key="solar")
                    plot_vars(df, sel, "Solar Radiation")
                else:
                    st.info("No solar variables")
                st.markdown('<p class="subsection-header">Longwave</p>', unsafe_allow_html=True)
                lw = [v for v in vars if 'lw' in v.lower() and any(x in v.lower() for x in ['avg', 'std'])]
                if lw:
                    plot_vars(df, st.multiselect("Select:", lw, lw, key="lw"), "Longwave")
                else:
                    st.info("No longwave")

                # SW Residual plot
                st.markdown('<p class="subsection-header">SW Residual</p>', unsafe_allow_html=True)
                glo_col = next((c for c in df.columns if 'glo' in c.lower() and 'avg' in c.lower()), None)
                dif_col = next((c for c in df.columns if 'dif' in c.lower() and 'avg' in c.lower()), None)
                dir_col = next((c for c in df.columns if 'dir' in c.lower() and 'avg' in c.lower()), None)
                if glo_col and dif_col and dir_col:
                    df['sw_res'] = df[glo_col] - (df[dif_col] + df[dir_col])
                    res_df = df[['TIMESTAMP', 'sw_res']].dropna(subset=['TIMESTAMP', 'sw_res']).copy()
                    if not res_df.empty:
                        res_df['Threshold 0.95'] = 0.95
                        res_df['Threshold 1.05'] = 1.05
                        res_df = res_df.set_index('TIMESTAMP')
                        if len(res_df) > 1000:
                            res_df = res_df.iloc[::len(res_df)//1000]
                        st.line_chart(res_df, height=300)
                        st.caption(f"SW Residual: {glo_col} - ({dif_col} + {dir_col}) | Thresholds: 0.95 / 1.05 ({len(res_df)} pts)")
                    else:
                        st.info("No SW Residual data available")
                else:
                    missing = [n for n, c in [('Global', glo_col), ('Diffuse', dif_col), ('Direct', dir_col)] if not c]
                    st.info(f"Missing columns for SW Residual: {', '.join(missing)}")
            else:
                st.warning("No SD data")
        else:
            st.info("No SD data")

    with col2:
        st.subheader("🌤️ Environmental")
        st.markdown('<p class="subsection-header">Meteorological</p>', unsafe_allow_html=True)
        if 'MD' in data_dict:
            df_md = data_dict['MD'].copy()
            vars_md = get_vars(df_md)
            if vars_md and 'TIMESTAMP' in df_md.columns:
                meteo = [v for v in vars_md if any(x in v.lower() for x in ['tp_sfc', 'humid', 'press', 'rain'])]
                if meteo:
                    plot_vars(df_md, st.multiselect("Select:", meteo, meteo, key="meteo"), "Meteorological")
                else:
                    st.info("No meteo vars")
            else:
                st.warning("No MD data")
        else:
            st.info("No MD data")

        st.markdown('<p class="subsection-header">Wind</p>', unsafe_allow_html=True)
        for h in ['10m', '25m', '50m']:
            for dt in ['MD', 'WD']:
                if dt in data_dict:
                    df_w = data_dict[dt].copy()
                    wvars = [v for v in get_vars(df_w) if h in v]
                    if wvars:
                        st.write(f"**Wind {h}**")
                        plot_vars(df_w, st.multiselect("Select:", wvars, wvars, key=f"wind_{h}"), f"Wind {h}", 200)
                        break
            else:
                st.info(f"No wind {h}")

    with col3:
        st.subheader("🔍 Detailed")
        detail_vars = []
        if 'SD' in data_dict:
            df_sd = data_dict['SD'].copy()
            row = metadata[metadata['station'].str.lower() == selected.lower()]
            if not row.empty:
                try:
                    df_sd = calc_clearsky(df_sd, row['latitude'].values[0], row['longitude'].values[0])
                except:
                    pass
            vars_sd = get_vars(df_sd)
            solar = [v for v in vars_sd if any(x in v.lower() for x in ['glo', 'dir', 'dif']) and any(x in v.lower() for x in ['avg', 'std'])]
            detail_vars.extend([f"SD: {v}" for v in solar])
            cs = [v for v in ['clearsky_GHI', 'clearsky_DNI', 'clearsky_DHI'] if v in df_sd.columns]
            detail_vars.extend([f"SD: {v}" for v in cs])

        if detail_vars:
            sel_detail = st.selectbox("Choose:", detail_vars, key="detail")
            if sel_detail:
                dt, vn = sel_detail.split(": ", 1)
                df_det = data_dict[dt].copy()
                if 'TIMESTAMP' in df_det.columns and vn in df_det.columns:
                    st.write(f"**{vn} ({dt})**")
                    st.line_chart(df_det[['TIMESTAMP', vn]].set_index('TIMESTAMP'), height=400)
                    c1, c2 = st.columns(2)
                    with c1:
                        st.metric("Mean", f"{df_det[vn].mean():.2f}")
                        st.metric("Min", f"{df_det[vn].min():.2f}")
                    with c2:
                        st.metric("Max", f"{df_det[vn].max():.2f}")
                        st.metric("Std", f"{df_det[vn].std():.2f}")
        else:
            st.info("No detail vars")

    with st.expander("📋 Raw Data"):
        for dt, df in data_dict.items():
            st.write(f"**{selected.upper()} - {dt}** ({df.shape[0]}×{df.shape[1]})")
            st.dataframe(df.head(20))
else:
    st.error(f"❌ No data for {selected.upper()}")

st.markdown("---")
st.markdown('<p style="text-align:center;color:#666;font-size:.9rem">Solar Data Monitoring System</p>', unsafe_allow_html=True)
