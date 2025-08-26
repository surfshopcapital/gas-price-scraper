import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, date
import os
import numpy as np
from sklearn.linear_model import RidgeCV
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from zoneinfo import ZoneInfo

# Page configuration
st.set_page_config(
    page_title="Gas Price Dashboard",
    page_icon="⛽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
.main-header { font-size: 2.5rem; color: #1f77b4; text-align: center; margin-bottom: 2rem; text-shadow: 2px 2px 4px rgba(0,0,0,0.1); }
.metric-card { background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%); padding: 1.2rem; border-radius: 0.8rem; border-left: 4px solid #1f77b4;
  height: 220px; display: flex; flex-direction: column; justify-content: space-between; box-shadow: 0 4px 6px rgba(0,0,0,0.1); transition: transform 0.2s ease; }
.metric-card:hover { transform: translateY(-2px); box-shadow: 0 6px 12px rgba(0,0,0,0.15); }
.metric-card h4 { font-size: 0.9rem; margin-bottom: 0.8rem; color: #2c3e50; line-height: 1.2; }
.metric-card p { margin: 0.3rem 0; font-size: 0.85rem; line-height: 1.3; }
.metric-card .value-row { display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem; }
.metric-card .current-value { font-weight: bold; color: #1f77b4; font-size: 1.1rem; }
.metric-card .previous-value { font-size: 0.8rem; color: #6c757d; font-style: italic; }
.data-source { background-color: #ffffff; padding: 1.5rem; border-radius: 0.5rem; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 1rem; }
.data-export { background: #f3e5f5; border-radius: 0.8rem; padding: 1.5rem; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-left: 4px solid #9c27b0; }
.summary-stats { background: #e8f5e8; border-radius: 0.8rem; padding: 1.5rem; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-left: 4px solid #4caf50; }
.stApp { background: #f8f9fa; }
.main .block-container { background: #ffffff; border-radius: 1rem; padding: 2rem; margin: 1rem; box-shadow: 0 8px 32px rgba(0,0,0,0.1); }
.update-schedule { background: #e3f2fd; border-radius: 0.8rem; padding: 1.5rem; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-left: 4px solid #2196f3; }
.update-schedule h3 { color: #2c3e50; margin-bottom: 1rem; }
.update-metric { text-align: center; }
.update-metric .metric-label { font-size: 1.1rem; font-weight: bold; color: #1f77b4; margin-bottom: 0.5rem; }
.update-metric .metric-value { font-size: 0.85rem; color: #6c757d; line-height: 1.3; }
.data-table { background: #ffffff; border-radius: 0.8rem; padding: 1.5rem; box-shadow: 0 4px 6px rgba(0,0,0,0.1); height: 100%; display: flex; flex-direction: column; justify-content: flex-start; }
.data-table h4 { color: #2c3e50; margin: 0 0 1rem 0; text-align: center; padding: 0; }
 .data-table .stDataFrame { flex: 1; min-height: 200px; }
.chart-section { margin-bottom: 2rem; }
.stDataFrame > div { height: 100% !important; }
.stDataFrame > div > div { height: 100% !important; }
.stDownloadButton { margin-top: 1rem; }
</style>
""", unsafe_allow_html=True)

def export_daily_excel():
    """Export today's data to Excel with 6 tabs (one for each data source)"""
    try:
        # Create data folder if it doesn't exist
        data_folder = "data"
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)
            print(f"Created folder: {data_folder}")

        # Get today's date for filename
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"{today} Gas Price Data.xlsx"
        filepath = os.path.join(data_folder, filename)

        # Connect to database
        database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
        conn = psycopg2.connect(database_url)

        # Get today's data for each source using PostgreSQL date functions
        today_data = pd.read_sql('''
            SELECT source, fuel_type, price, timestamp, region, consensus, surprise, scraped_at
            FROM gas_prices
            WHERE DATE(scraped_at) = CURRENT_DATE
            ORDER BY source, scraped_at DESC
        ''', conn)

        conn.close()

        if today_data.empty:
            print(f"No data found for today ({today})")
            return False

        # Create Excel
        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            # Define source configurations for tabs
            source_configs = [
                { 'source': 'gasbuddy_fuel_insights', 'tab_name': 'GasBuddy', 'display_name': 'GasBuddy National Average' },
                { 'source': 'aaa_gas_prices', 'tab_name': 'AAA', 'display_name': 'AAA Gas National Average' },
                { 'source': 'marketwatch_rbob_futures', 'tab_name': 'RBOB', 'display_name': 'RBOB Futures' },
                { 'source': 'marketwatch_wti_futures', 'tab_name': 'WTI', 'display_name': 'WTI Futures' },
                { 'source': 'tradingeconomics_gasoline_stocks', 'tab_name': 'Gasoline_Stocks', 'display_name': 'Gasoline Stocks Change' },
                { 'source': 'tradingeconomics_refinery_runs', 'tab_name': 'Refinery_Runs', 'display_name': 'Refinery Runs Change' }
            ]

            # Create tab for each source
            for config in source_configs:
                source_data = today_data[today_data['source'] == config['source']]
                if not source_data.empty:
                    # Select columns to display
                    display_cols = ['price', 'timestamp', 'region', 'fuel_type', 'scraped_at']
                    if 'consensus' in source_data.columns and 'surprise' in source_data.columns:
                        display_cols.extend(['consensus', 'surprise'])

                    # Reorder columns safely
                    final_cols = [col for col in display_cols if col in source_data.columns]

                    # Write to Excel tab
                    source_data[final_cols].to_excel(writer, sheet_name=config['tab_name'], index=False)

                    # Auto-adjust column widths
                    worksheet = writer.sheets[config['tab_name']]
                    for column in worksheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 50)
                        worksheet.column_dimensions[column_letter].width = adjusted_width

                    print(f"Created tab: {config['tab_name']} with {len(source_data)} records")
                else:
                    # Create empty tab with headers if no data
                    empty_df = pd.DataFrame(columns=['price', 'timestamp', 'region', 'fuel_type', 'scraped_at'])
                    empty_df.to_excel(writer, sheet_name=config['tab_name'], index=False)
                    print(f"Created empty tab: {config['tab_name']} (no data for today)")

        print(f"Daily Excel export completed: {filepath}")
        return True

    except Exception as e:
        print(f"Error exporting daily Excel: {e}")
        return False

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    """Load data from PostgreSQL database"""
    try:
        database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
        conn = psycopg2.connect(database_url)

        # Latest (per source)
        latest_data = pd.read_sql('''
            SELECT source, fuel_type, price, timestamp, region, consensus, surprise, scraped_at
            FROM gas_prices
            WHERE
                (source != 'aaa_gas_prices' AND scraped_at = (
                    SELECT MAX(scraped_at) FROM gas_prices p2 WHERE p2.source = gas_prices.source
                ))
                OR
                (source = 'aaa_gas_prices' AND timestamp LIKE '%Current%')
            ORDER BY scraped_at DESC
        ''', conn)

        # Previous (per source)
        previous_data = pd.read_sql('''
            SELECT source, price, timestamp, consensus, surprise, scraped_at
            FROM gas_prices p1
            WHERE scraped_at = (
                SELECT MAX(scraped_at) FROM gas_prices p2
                WHERE p2.source = p1.source
                  AND p2.scraped_at < (SELECT MAX(scraped_at) FROM gas_prices p3 WHERE p3.source = p1.source)
            )
        ''', conn)

        # Historical (30d) for charts
        historical_data = pd.read_sql('''
            SELECT source, fuel_type, price, timestamp, region, consensus, surprise, scraped_at
            FROM gas_prices
            WHERE scraped_at >= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY scraped_at ASC
        ''', conn)

        conn.close()
        return latest_data, previous_data, historical_data

    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None, None

def get_next_update_info():
    """Get information about next scheduled updates"""
    now = datetime.now()

    # GasBuddy: every 10 minutes (updated from 15 to match your scraper)
    next_gasbuddy = now + timedelta(minutes=10 - (now.minute % 10))
    next_gasbuddy = next_gasbuddy.replace(second=0, microsecond=0)

    # AAA: daily at 3:30 AM EST (7:30 UTC)
    next_aaa = now.replace(hour=7, minute=30, second=0, microsecond=0)
    if next_aaa <= now:
        next_aaa += timedelta(days=1)

    # RBOB & WTI: every hour Sun 6pm-Fri 8pm EST (simplified for display)
    if now.weekday() < 5:  # Mon-Fri
        next_rbob_wti = now.replace(minute=0, second=0, microsecond=0)
        if next_rbob_wti <= now:
            next_rbob_wti += timedelta(hours=1)
    else:
        next_rbob_wti = "Weekend - Limited updates"

    # EIA-style series scrape time (10:35 AM EST / 14:35 UTC)
    next_eia = now.replace(hour=14, minute=35, second=0, microsecond=0)
    if next_eia <= now:
        next_eia += timedelta(days=1)

    return {
        'gasbuddy': next_gasbuddy,
        'aaa': next_aaa,
        'rbob_wti': next_rbob_wti,
        'eia': next_eia
    }

def create_chart_csv(chart_data, source_name, chart_type):
    """Create CSV data for chart download with EST timestamps"""
    try:
        if chart_data.empty:
            return None
        
        # Prepare data for CSV
        if chart_type == 'crack_spread':
            csv_data = chart_data[['date', 'Crack_Spread']].copy()
            csv_data.columns = ['Date (EST)', 'Crack_Spread_$']
        else:
            csv_data = chart_data[['scraped_at', 'price']].copy()
            # Convert UTC timestamps to EST for CSV export
            csv_data['scraped_at'] = pd.to_datetime(csv_data['scraped_at'], utc=True)
            csv_data['scraped_at'] = csv_data['scraped_at'].dt.tz_convert('America/New_York')
            csv_data.columns = ['Timestamp (EST)', 'Price_$']
        
        return csv_data.to_csv(index=False)
    except Exception as e:
        st.error(f"Error creating CSV for {source_name}: {e}")
        return None

def calculate_averages(data, source_name):
    """Calculate daily, month-to-date, and week-to-date averages for a data source"""
    try:
        if data.empty:
            return {
                'daily_avg': 'No data',
                'mtd_avg': 'No data', 
                'wtd_avg': 'No data'
            }
        
        # Convert scraped_at to datetime
        data['scraped_at'] = pd.to_datetime(data['scraped_at'], errors='coerce', utc=False)
        data = data.dropna(subset=['scraped_at'])
        
        if data.empty:
            return {
                'daily_avg': 'No data',
                'mtd_avg': 'No data',
                'wtd_avg': 'No data'
            }
        
        # Get current date and time
        now = datetime.now()
        today = now.date()
        
        # Handle duplicates based on source type
        if source_name == 'AAA':
            # For AAA: one point per day (last observation of the day)
            data = data.sort_values('scraped_at').groupby(data['scraped_at'].dt.date).tail(1).reset_index(drop=True)
        elif source_name == 'GasBuddy':
            # For GasBuddy: keep all unique timestamps (no duplicates)
            data = data.drop_duplicates(subset=['scraped_at']).reset_index(drop=True)
        elif source_name == 'Crack Spread':
            # For Crack Spread: one point per day (last observation of the day)
            data = data.sort_values('scraped_at').groupby(data['scraped_at'].dt.date).tail(1).reset_index(drop=True)
        
        # Daily average (today's data only)
        today_data = data[data['scraped_at'].dt.date == today]
        daily_avg = today_data['price'].mean() if not today_data.empty else None
        
        # Month-to-date average (from 1st of current month)
        month_start = today.replace(day=1)
        mtd_data = data[data['scraped_at'].dt.date >= month_start]
        mtd_avg = mtd_data['price'].mean() if not mtd_data.empty else None
        
        # Week-to-date average (Monday noon EST to current)
        # Find last Monday at noon EST (UTC-5, so 17:00 UTC)
        days_since_monday = now.weekday()
        monday_noon_est = now - timedelta(days=days_since_monday)
        monday_noon_est = monday_noon_est.replace(hour=17, minute=0, second=0, microsecond=0)  # 17:00 UTC = 12:00 EST
        
        # If we're before Monday noon, go back to previous Monday
        if now < monday_noon_est:
            monday_noon_est -= timedelta(days=7)
        
        wtd_data = data[data['scraped_at'] >= monday_noon_est]
        wtd_avg = wtd_data['price'].mean() if not wtd_data.empty else None
        
        # Format results
        def format_price(price):
            if price is None or pd.isna(price):
                return 'No data'
            if source_name == 'Crack Spread':
                return f"${price:.2f}"
            else:
                return f"${price:.3f}"
        
        return {
            'daily_avg': format_price(daily_avg),
            'mtd_avg': format_price(mtd_avg),
            'wtd_avg': format_price(wtd_avg)
        }
        
    except Exception as e:
        st.error(f"Error calculating averages for {source_name}: {e}")
        return {
            'daily_avg': 'Error',
            'mtd_avg': 'Error',
            'wtd_avg': 'Error'
        }

# ===================== Modeling Functions =====================
def _daily_panel(historical_data: pd.DataFrame) -> pd.DataFrame:
    """
    Build a clean daily panel from 'historical_data' (prod-like schema).
    Uses sources:
      - gasbuddy_fuel_insights (target)
      - marketwatch_rbob_futures, marketwatch_wti_futures (drivers)
      - tradingeconomics_gasoline_stocks (weekly surprise)
      - tradingeconomics_refinery_runs (weekly level)
    """
    df = historical_data.copy()
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    df = df.dropna(subset=["scraped_at"])
    # normalize to date (localize if naive)
    if df["scraped_at"].dt.tz is None:
        df["scraped_at"] = df["scraped_at"].dt.tz_localize("America/New_York")
    df["date"] = pd.to_datetime(df["scraped_at"].dt.date)

    def dmean(src, colname):
        sub = df[df["source"]==src][["date","price"]]
        if sub.empty:
            return pd.DataFrame(columns=["date", colname])
        return sub.groupby("date", as_index=False)["price"].mean().rename(columns={"price": colname})

    gb   = dmean("gasbuddy_fuel_insights", "gas_price")
    rbob = dmean("marketwatch_rbob_futures", "rbob")
    wti  = dmean("marketwatch_wti_futures", "wti")

    stocks = df[df["source"]=="tradingeconomics_gasoline_stocks"][["date","surprise"]].dropna().drop_duplicates("date")
    ref    = df[df["source"]=="tradingeconomics_refinery_runs"][["date","price"]].dropna().drop_duplicates("date").rename(columns={"price":"refinery"})

    starts = [x["date"].min() for x in [gb,rbob,wti] if not x.empty]
    ends   = [x["date"].max() for x in [gb,rbob,wti] if not x.empty]
    if not starts or not ends:
        return pd.DataFrame(columns=["date","gas_price","rbob","wti","surprise","refinery","crack"])

    start, end = min(starts), max(ends)
    base = pd.DataFrame({"date": pd.date_range(start, end, freq="D")})

    for d, col in [(gb,"gas_price"), (rbob,"rbob"), (wti,"wti")]:
        if not d.empty:
            base = base.merge(d[["date",col]], on="date", how="left")
    base = base.merge(stocks, on="date", how="left")
    base = base.merge(ref, on="date", how="left")

    for c in ["rbob","wti","surprise","refinery"]:
        if c in base.columns:
            base[c] = base[c].ffill()

    base["crack"] = base["rbob"] - (base["wti"]/42.0)
    return base

def _make_pulses(df: pd.DataFrame, decay=[1.0, 0.6, 0.36, 0.216]) -> pd.DataFrame:
    """
    Convert weekly levels into short-lived pulses centered on release day + geometric decay.
    """
    out = df.set_index("date").copy()
    idx = out.index
    for name in ["surprise","refinery"]:
        if name not in out.columns:
            out[name] = 0.0
        series = out[name].fillna(0.0)
        pulse = pd.Series(0.0, index=idx)
        for i, w in enumerate(decay):
            pulse = pulse.add(series.shift(i, fill_value=0.0)*w, fill_value=0.0)
        out[f"{name}_pulse"] = pulse
    return out.reset_index()

def _add_lags(base: pd.DataFrame, max_lag: int = 14) -> pd.DataFrame:
    """
    Add daily lags and asymmetry on RBOB (up vs down).
    """
    df = base.copy()
    for lag in range(0, max_lag+1):
        df[f"rbob_l{lag}"]   = df["rbob"].shift(lag)
        df[f"wti_l{lag}"]    = df["wti"].shift(lag)
        df[f"crack_l{lag}"]  = df["crack"].shift(lag)

    df["d_rbob"] = df["rbob"].diff()
    df["rbob_up"]   = np.where(df["d_rbob"]>0,  df["rbob"], 0.0)
    df["rbob_down"] = np.where(df["d_rbob"]<=0, df["rbob"], 0.0)
    df["gas_lag1"]  = df["gas_price"].shift(1)
    return df

def fit_forecast_with_lags(historical_data: pd.DataFrame, sim_n: int = 2000, max_lag: int = 14):
    try:
        st.info("🔍 Starting modeling process...")
        
        base = _daily_panel(historical_data)
        if base.empty:
            raise ValueError("No usable daily panel; check inputs.")
        
        st.info(f"📊 Daily panel created with {len(base)} rows")
        
        base = _make_pulses(base)
        base = _add_lags(base, max_lag=max_lag)
        
        st.info(f"📈 Added lags and pulses. Shape: {base.shape}")

        # More aggressive NaN handling
        model_df = base.dropna(subset=["gas_price","gas_lag1"]).copy()
        
        if model_df.empty:
            raise ValueError("No data available after removing NaN values in gas_price or gas_lag1")

        st.info(f"✅ After NaN removal: {len(model_df)} rows")

        lag_cols = ([f"rbob_l{l}" for l in range(max_lag+1)] +
                    [f"wti_l{l}"  for l in range(max_lag+1)] +
                    [f"crack_l{l}" for l in range(max_lag+1)])
        feat_cols = ["gas_lag1","rbob_up","rbob_down","surprise_pulse","refinery_pulse"] + lag_cols

        # Check if all required columns exist
        missing_cols = [col for col in feat_cols if col not in model_df.columns]
        if missing_cols:
            st.warning(f"⚠️ Missing columns: {missing_cols}")
            # Only use columns that exist
            feat_cols = [col for col in feat_cols if col in model_df.columns]
            st.info(f"📋 Using available features: {feat_cols}")

        X = model_df[feat_cols].copy()
        y = model_df["gas_price"].copy()
        
        st.info(f"🔧 Feature matrix shape: {X.shape}, Target shape: {y.shape}")
        
        # More robust NaN handling
        X = X.fillna(method="ffill").fillna(method="bfill").fillna(X.mean())
        
        # Final check for any remaining NaN values
        if X.isna().any().any():
            # Drop rows with any remaining NaN values
            valid_mask = ~X.isna().any(axis=1)
            X = X.loc[valid_mask]
            y = y.loc[valid_mask]
            model_df = model_df.loc[valid_mask]  # Keep aligned
            
            if X.empty:
                raise ValueError("No data available after removing all NaN values")
        
        # Ensure we have enough data
        if len(X) < 10:
            raise ValueError(f"Insufficient data for modeling. Need at least 10 observations, got {len(X)}")

        st.info(f"✅ Final data shape: X={X.shape}, y={y.shape}")

        # time-ordered 80/20 split
        cut = int(len(model_df)*0.8) if len(model_df) >= 20 else max(1, int(len(model_df)*0.8))
        X_train, X_test = X.iloc[:cut], X.iloc[cut:]
        y_train, y_test = y.iloc[:cut], y.iloc[cut:]
        
        st.info(f"📊 Train/Test split: {len(X_train)}/{len(X_test)}")

        ridge = RidgeCV(alphas=np.logspace(-4,3,21))
        ridge.fit(X_train, y_train)

        y_pred = ridge.predict(X_test) if len(X_test)>0 else np.array([])
        metrics = {
            "MAE": float(mean_absolute_error(y_test, y_pred)) if len(y_pred)>0 else np.nan,
            "RMSE": float(np.sqrt(mean_squared_error(y_test, y_pred))) if len(y_pred)>0 else np.nan,
            "R2": float(r2_score(y_test, y_pred)) if len(y_pred)>0 else np.nan,
        }

        # Month context
        today = model_df["date"].max()
        year, month = today.year, today.month
        days_in_month = pd.Period(today, 'M').days_in_month
        month_start = pd.Timestamp(year=year, month=month, day=1)
        month_end   = pd.Timestamp(year=year, month=month, day=days_in_month)

        mtd = model_df[(model_df["date"]>=month_start) & (model_df["date"]<=today)][["date","gas_price"]].sort_values("date")
        mtd_avg = float(mtd["gas_price"].mean()) if not mtd.empty else np.nan
        last_actual = mtd["date"].max() if not mtd.empty else (month_start - pd.Timedelta(days=1))

        st.info(f"📅 Month context: {month_start.date()} to {month_end.date()}, MTD avg: {mtd_avg:.3f}")

        # Simulation set-up (flat exogenous; AR(1) via gas_lag1)
        base_X = X.copy()
        
        # Fix the boolean index issue by ensuring proper alignment
        # Use model_df (not base) for the date mask and for constants
        date_mask = model_df["date"] <= last_actual
        if date_mask.sum() > 0:
            state_idx = base_X.index[date_mask].max()
        else:
            state_idx = base_X.index.max()
            
        state_vec = base_X.loc[state_idx].values
        const_r = float(model_df.loc[state_idx, "rbob"]) if "rbob" in model_df.columns else 0.0
        const_w = float(model_df.loc[state_idx, "wti"]) if "wti" in model_df.columns else 0.0
        const_c = float(model_df.loc[state_idx, "crack"]) if "crack" in model_df.columns else 0.0
        
        col_index = {c:i for i,c in enumerate(base_X.columns)}
        for l in range(max_lag+1):
            if f"rbob_l{l}" in col_index:  state_vec[col_index[f"rbob_l{l}"]]  = const_r
            if f"wti_l{l}"  in col_index:  state_vec[col_index[f"wti_l{l}"]]   = const_w
            if f"crack_l{l}" in col_index: state_vec[col_index[f"crack_l{l}"]] = const_c
        if "surprise_pulse" in col_index: state_vec[col_index["surprise_pulse"]] = 0.0
        if "refinery_pulse" in col_index: state_vec[col_index["refinery_pulse"]] = 0.0
        if "rbob_up" in col_index:   state_vec[col_index["rbob_up"]] = const_r
        if "rbob_down" in col_index: state_vec[col_index["rbob_down"]] = 0.0

        resid = (y_train.values - ridge.predict(X_train)) if len(y_train)>0 else np.array([0.0])

        future_dates = pd.date_range(last_actual + pd.Timedelta(days=1), month_end, freq="D")
        horizon = len(future_dates)
        paths = np.zeros((sim_n, horizon), dtype=float) if horizon>0 else np.zeros((sim_n,0), dtype=float)

        y0 = float(model_df.loc[model_df["date"]==last_actual, "gas_price"].iloc[0]) if (model_df["date"]==last_actual).any() else float(y.iloc[-1]) if len(y)>0 else float(y_train.iloc[-1]) if len(y_train)>0 else 0.0
        idx_lag1 = col_index.get("gas_lag1", None)

        st.info(f"🚀 Starting simulation: {sim_n} paths, {horizon} days horizon")

        for i in range(sim_n):
            y_prev = y0
            vec = state_vec.copy()
            for h in range(horizon):
                if idx_lag1 is not None:
                    vec[idx_lag1] = y_prev
                y_hat = float(ridge.predict(vec.reshape(1,-1))[0])
                eps = float(np.random.choice(resid)) if len(resid)>0 else 0.0
                y_prev = y_hat + eps
                paths[i,h] = y_prev

        if horizon>0 and np.isfinite(mtd_avg):
            mtd_days = mtd["date"].dt.date.nunique()
            eom_means = (mtd_avg*mtd_days + paths.mean(axis=1)*horizon) / (mtd_days + horizon)
        elif horizon==0 and np.isfinite(mtd_avg):
            eom_means = np.full(sim_n, mtd_avg, dtype=float)
        else:
            eom_means = paths.mean(axis=1) if horizon>0 else np.array([])

        eom_mean = float(np.mean(eom_means)) if len(eom_means) else np.nan
        ci5, ci95 = (float(np.percentile(eom_means, 5)), float(np.percentile(eom_means, 95))) if len(eom_means) else (np.nan, np.nan)

        # Thresholds: 6 values closest to current MTD avg at ±$0.05 steps
        if np.isfinite(mtd_avg):
            base_thr = np.round(mtd_avg, 2)
            thresholds = np.arange(base_thr-0.25, base_thr+0.26, 0.05)
            thresholds = thresholds[np.abs(thresholds - mtd_avg) <= 0.25]
            if len(thresholds) > 6:
                # Keep 6 closest to mtd_avg
                distances = np.abs(thresholds - mtd_avg)
                closest_indices = np.argsort(distances)[:6]
                thresholds = thresholds[closest_indices]
            thresholds = np.sort(thresholds)
        else:
            thresholds = np.array([])

        # Probability table
        if len(thresholds) > 0 and len(eom_means) > 0:
            prob_data = []
            for thr in thresholds:
                prob = float((eom_means > thr).mean() * 100.0)
                prob_data.append({"Threshold ($)": f"${thr:.2f}", "Probability (%)": f"{prob:.1f}%"})
            prob_table = pd.DataFrame(prob_data)
        else:
            prob_table = pd.DataFrame(columns=["Threshold ($)", "Probability (%)"])

        st.success("✅ Modeling completed successfully!")
        
        return {
            "future_dates": future_dates,
            "paths": paths,
            "mtd_avg": mtd_avg,
            "eom_mean": eom_mean,
            "ci5": ci5,
            "ci95": ci95,
            "metrics": metrics,
            "prob_table": prob_table,
            "thresholds": thresholds,
            "mtd": mtd,           # Add this for fan chart
            "month_end": month_end  # Also handy in the title
        }
        
    except Exception as e:
        st.error(f"Modeling error: {str(e)}")
        st.exception(e)  # Show full traceback for debugging
        # Return a default structure with error information
        return {
            "future_dates": pd.DatetimeIndex([]),
            "paths": np.array([]),
            "mtd_avg": np.nan,
            "eom_mean": np.nan,
            "ci5": np.nan,
            "ci95": np.nan,
            "metrics": {"MAE": np.nan, "RMSE": np.nan, "R2": np.nan},
            "prob_table": pd.DataFrame(columns=["Threshold ($)", "Probability (%)"], index=range(1)),
            "thresholds": np.array([]),
            "error": str(e)
        }

def nyc_now():
    return datetime.now(tz=ZoneInfo("America/New_York"))

def week_window(dt_nyc: datetime):
    # Monday 12:00 ET → next Monday 12:00 ET
    wd = dt_nyc.weekday()  # Monday=0
    last_monday_noon = (dt_nyc - timedelta(days=wd)).replace(hour=12, minute=0, second=0, microsecond=0)
    if dt_nyc < last_monday_noon:
        last_monday_noon -= timedelta(days=7)
    next_monday_noon = last_monday_noon + timedelta(days=7)
    return last_monday_noon, next_monday_noon

def weekly_live_probability_and_path(historical_data: pd.DataFrame, res: dict, thr: float, sims: int = 800):
    """
    (A) For snapshots within Mon 12:00 ET → next Mon 12:00 ET, compute P( AAA next Monday daily > thr ).
    (B) Week-to-date daily series (Mon..next Mon): AAA actuals where available + median prediction remainder.
    """
    base      = res["base"]
    model_df  = res["model_df"]
    ridge     = res["ridge"]

    # Model residuals for bootstrap
    X_cols = [c for c in model_df.columns if c.startswith(("gas_lag1","rbob_up","rbob_down","surprise_pulse","refinery_pulse","rbob_l","wti_l","crack_l"))]
    base_X_full = model_df[X_cols].fillna(method="ffill").fillna(method="bfill").fillna(model_df[X_cols].mean())
    resid = (model_df["gas_price"] - ridge.predict(base_X_full)).values
    resid = resid[~np.isnan(resid)]
    if resid.size == 0:
        resid = np.array([0.0])

    # Weekly window
    now_nyc = nyc_now()
    wk_start, wk_end = week_window(now_nyc)
    end_date = pd.Timestamp(wk_end.date())

    # Snapshot times from raw data (use 6h granularity)
    df = historical_data.copy()
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    df = df.dropna(subset=["scraped_at"])
    snaps = df[(df["scraped_at"]>=wk_start) & (df["scraped_at"]<=wk_end)].copy()
    if snaps.empty:
        return (pd.DataFrame(columns=["ts","prob"]),
                pd.DataFrame(columns=["date","price"]))

    snaps["key"] = snaps["scraped_at"].dt.tz_convert("America/New_York").dt.floor("6H")
    snaps = snaps.sort_values("scraped_at").groupby("key", as_index=False).last()

    # Helper: simulate up to target_date from last actual <= snap_day
    def simulate_to_date(snap_day: pd.Timestamp, target_date: pd.Timestamp, sims: int):
        md_slice = model_df[model_df["date"]<=snap_day]
        if md_slice.empty:
            return None, None
        last_actual = md_slice["date"].max()
        state_row = model_df.loc[model_df["date"]<=last_actual].iloc[-1:].copy()
        for c in ["surprise_pulse","refinery_pulse"]:
            if c in state_row.columns:
                state_row[c] = 0.0
        vec0 = state_row[X_cols].fillna(method="ffill", axis=1).fillna(method="bfill", axis=1).values[0]
        try:
            idx_gas_lag1 = X_cols.index("gas_lag1")
        except ValueError:
            idx_gas_lag1 = None

        future_days = pd.date_range(last_actual + pd.Timedelta(days=1), target_date, freq="D")
        H = len(future_days)
        paths = np.zeros((sims, H), dtype=float) if H>0 else np.zeros((sims,0), dtype=float)
        y0 = float(md_slice.loc[md_slice["date"]==last_actual, "gas_price"].iloc[0])

        for i in range(sims):
            vec = vec0.copy()
            y_prev = y0
            for h in range(H):
                if idx_gas_lag1 is not None:
                    vec[idx_gas_lag1] = y_prev
                y_hat = float(ridge.predict(vec.reshape(1,-1))[0])
                eps   = float(np.random.choice(resid))
                y_prev = y_hat + eps
                paths[i, h] = y_prev
        return future_days, paths

    # (A) Live probability curve
    points = []
    last_future_days, last_paths = None, None
    for _, r in snaps.iterrows():
        snap_time = r["key"]
        snap_day  = pd.Timestamp(snap_time.date())
        fdays, pths = simulate_to_date(snap_day, end_date, sims=sims)
        if pths is None:
            continue
        end_vals = pths[:,-1] if pths.shape[1] else np.array([])
        prob = float((end_vals > thr).mean()*100.0) if end_vals.size>0 else float("nan")
        points.append({"ts": snap_time, "prob": prob})
        last_future_days, last_paths = fdays, pths
    live_df = pd.DataFrame(points).sort_values("ts")

    # (B) Week-to-date daily path (Mon..next Mon)
    wk_start_day = pd.Timestamp(wk_start.date())
    week_days = pd.date_range(wk_start_day, end_date, freq="D")
    actuals = model_df[model_df["date"].isin(week_days)][["date","gas_price"]].copy()
    if last_paths is not None and last_paths.size>0:
        med_path = np.median(last_paths, axis=0)
        pred_df = pd.DataFrame({"date": last_future_days, "price": med_path})
    else:
        pred_df = pd.DataFrame(columns=["date","price"])
    week_path = pd.DataFrame({"date": week_days})
    week_path = week_path.merge(actuals.rename(columns={"gas_price":"price"}), on="date", how="left")
    if not pred_df.empty:
        week_path = week_path.merge(pred_df, on="date", how="left", suffixes=("","_pred"))
        week_path["price"] = week_path["price"].combine_first(week_path["price_pred"])
        week_path = week_path.drop(columns=["price_pred"])

    return live_df, week_path

def main():
    st.markdown('<h1 class="main-header">Gas Price Dashboard</h1>', unsafe_allow_html=True)

    # Load data
    latest_data, previous_data, historical_data = load_data()
    if latest_data is None:
        st.error("Failed to load data. Please check if the database exists and contains data.")
        return

    # Sidebar filters
    st.sidebar.header("Dashboard Controls")
    time_range = st.sidebar.selectbox(
        "Time Range for Charts",
        ["Last 7 days", "Last 30 days", "Last 90 days", "All time"],
        index=1
    )

    # Latest data overview - 6 cards
    st.header("Latest Data Overview")
    cols = st.columns(6)
    source_configs = [
        { 'source': 'gasbuddy_fuel_insights', 'name': 'GasBuddy National Average (Live Feed)', 'value_field': 'price', 'display_name': 'GasBuddy' },
        { 'source': 'aaa_gas_prices', 'name': 'AAA Gas National Average (Current)', 'value_field': 'price', 'display_name': 'AAA' },
        { 'source': 'marketwatch_rbob_futures', 'name': 'RBOB Futures', 'value_field': 'price', 'display_name': 'RBOB' },
        { 'source': 'marketwatch_wti_futures', 'name': 'WTI Futures', 'value_field': 'price', 'display_name': 'WTI' },
        { 'source': 'tradingeconomics_gasoline_stocks', 'name': 'Gasoline Stocks Change', 'value_field': 'surprise', 'display_name': 'Gasoline Stocks' },
        { 'source': 'tradingeconomics_refinery_runs', 'name': 'Refinery Runs Change', 'value_field': 'price', 'display_name': 'Refinery Runs' }
    ]

    for idx, config in enumerate(source_configs):
        source_data = latest_data[latest_data['source'] == config['source']]
        previous_source_data = previous_data[previous_data['source'] == config['source']]

        if not source_data.empty:
            row = source_data.iloc[0]
            with cols[idx]:
                try:
                    updated_time = row['scraped_at'].strftime('%H:%M') if not pd.isna(row['scraped_at']) else "N/A"

                    # Determine value
                    if config['value_field'] == 'surprise' and 'surprise' in row and not pd.isna(row['surprise']):
                        value = row['surprise']
                        value_label = "Surprise"
                    else:
                        value = row['price']
                        value_label = "Value"

                    # Previous value
                    previous_value = None
                    if not previous_source_data.empty:
                        prev_row = previous_source_data.iloc[0]
                        if config['value_field'] == 'surprise' and 'surprise' in prev_row and not pd.isna(prev_row['surprise']):
                            previous_value = prev_row['surprise']
                        else:
                            previous_value = prev_row['price']

                    # Display formats
                    if config['source'] == 'tradingeconomics_gasoline_stocks':
                        value_display = f"{value:.3f}M"
                        previous_display = f"{previous_value:.3f}M" if previous_value is not None else ""
                    elif config['source'] == 'tradingeconomics_refinery_runs':
                        value_display = f"{value:.3f}M"
                        previous_display = f"{previous_value:.3f}M" if previous_value is not None else ""
                    elif config['source'] in ['marketwatch_wti_futures']:
                        value_display = f"${value:.2f}"
                        previous_display = f"${previous_value:.2f}" if previous_value is not None else ""
                    else:
                        value_display = f"${value:.3f}"
                        previous_display = f"${previous_value:.3f}" if previous_value is not None else ""

                    st.markdown(f"""
                        <div class="metric-card">
                            <h4>{config['name']}</h4>
                            <div class="value-row">
                                <span><strong>{value_label}:</strong></span>
                                <span class="current-value">{value_display}</span>
                            </div>
                            <div class="value-row">
                                <span class="previous-value">Previous: {previous_display if previous_display else ''}</span>
                            </div>
                            <p><strong>Date:</strong> {row['timestamp']}</p>
                            <p><strong>Updated:</strong> {updated_time}</p>
                        </div>
                    """, unsafe_allow_html=True)

                except Exception as e:
                    st.markdown(f"""
                        <div class="metric-card">
                            <h4>{config['name']}</h4>
                            <div class="value-row">
                                <span><strong>Value:</strong></span>
                                <span class="current-value">Error</span>
                            </div>
                            <div class="value-row">
                                <span class="previous-value">Previous: </span>
                            </div>
                            <p><strong>Date:</strong> Error</p>
                            <p><strong>Updated:</strong> Error</p>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            with cols[idx]:
                st.markdown(f"""
                    <div class="metric-card">
                        <h4>{config['name']}</h4>
                        <div class="value-row">
                            <span><strong>Value:</strong></span>
                            <span class="current-value">No data</span>
                        </div>
                        <div class="value-row">
                            <span class="previous-value">Previous: </span>
                        </div>
                        <p><strong>Date:</strong> No data</p>
                        <p><strong>Updated:</strong> No data</p>
                    </div>
                """, unsafe_allow_html=True)

    # Next Update Information
    st.markdown("---")
    st.markdown('<div class="update-schedule">', unsafe_allow_html=True)
    st.subheader("Next Update Schedule")
    next_updates = get_next_update_info()
    update_cols = st.columns(4)

    with update_cols[0]:
        st.markdown('<div class="update-metric">', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">GasBuddy</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">Every 10 min<br>Next: {next_updates["gasbuddy"].strftime("%H:%M") if isinstance(next_updates["gasbuddy"], datetime) else next_updates["gasbuddy"]}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with update_cols[1]:
        st.markdown('<div class="update-metric">', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">AAA</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">Daily 3:30 AM EST<br>Next: {next_updates["aaa"].strftime("%m/%d %H:%M") if isinstance(next_updates["aaa"], datetime) else next_updates["aaa"]}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with update_cols[2]:
        st.markdown('<div class="update-metric">', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">RBOB & WTI</div>', unsafe_allow_html=True)
        if isinstance(next_updates['rbob_wti'], datetime):
            st.markdown(f'<div class="metric-value">Every hour Mon-Fri<br>Next: {next_updates["rbob_wti"].strftime("%H:%M")}</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="metric-value">{next_updates["rbob_wti"]}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with update_cols[3]:
        st.markdown('<div class="update-metric">', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">EIA Data</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">Daily 10:35 AM EST<br>Next: {next_updates["eia"].strftime("%m/%d %H:%M") if isinstance(next_updates["eia"], datetime) else next_updates["eia"]}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

    # Data Export Section
    st.markdown("---")
    st.markdown('<div class="data-export">', unsafe_allow_html=True)
    st.subheader("Data Export")
    export_col1, export_col2 = st.columns(2)

    with export_col1:
        if st.button("Download Full Database as CSV"):
            try:
                database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
                conn = psycopg2.connect(database_url)
                all_data = pd.read_sql('''
                    SELECT source, price, timestamp, region, fuel_type, consensus, surprise, scraped_at
                    FROM gas_prices
                    ORDER BY scraped_at DESC
                ''', conn)
                conn.close()
                csv = all_data.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"gas_prices_database_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            except Exception as e:
                st.error(f"Error downloading data: {e}")

    with export_col2:
        if st.button("Export Today's Data to Excel"):
            try:
                success = export_daily_excel()
                if success:
                    st.success("Daily Excel export completed! Check the 'data' folder.")
                    today = datetime.now().strftime("%Y-%m-%d")
                    filename = f"{today} Gas Price Data.xlsx"
                    filepath = os.path.join("data", filename)
                    if os.path.exists(filepath):
                        file_size = os.path.getsize(filepath) / 1024  # KB
                        st.info(f"File saved: {filepath} ({file_size:.1f} KB)")
                    else:
                        st.error("Failed to export daily Excel file.")
                else:
                    st.error("No data exported for today.")
            except Exception as e:
                st.error(f"Error exporting Excel: {e}")

    st.markdown('</div>', unsafe_allow_html=True)

    # Summary metrics
    st.markdown("---")
    st.markdown('<div class="summary-stats">', unsafe_allow_html=True)
    st.subheader("Summary Statistics")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Data Sources", len(latest_data))

    with col2:
        st.metric("Total Records", len(historical_data))

    with col3:
        try:
            database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
            conn = psycopg2.connect(database_url)
            cursor = conn.cursor()
            cursor.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
            db_size = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            st.metric("Database Size", db_size)
        except:
            st.metric("Database Size", "Unknown")

    with col4:
        try:
            last_update = latest_data['scraped_at'].max()
            if pd.isna(last_update):
                st.metric("Last Update", "No data")
            else:
                st.metric("Last Update", last_update.strftime('%H:%M'))
        except Exception as e:
            st.metric("Last Update", "Error")

    st.markdown('</div>', unsafe_allow_html=True)

    # ===========================
    # 📈 NEW CHARTS (Bottom Area) - 3/5 width with tables
    # ===========================
    st.markdown("---")
    st.header("Historical Trends & Analysis")

    # Helper: safe datetime conversion with timezone handling
    def _to_datetime(series):
        """Convert to datetime with proper timezone handling and convert to EST"""
        # First convert to datetime (assuming UTC from database)
        out = pd.to_datetime(series, errors='coerce', utc=True)
        
        # Convert UTC to EST using proper timezone conversion
        out = out.dt.tz_convert('America/New_York')
        
        return out

    # ---------- 1) GasBuddy Month-to-Date Chart (3/5 width) + Table (2/5 width) ----------
    st.markdown('<div class="chart-section">', unsafe_allow_html=True)
    st.subheader("1. GasBuddy Month-to-Date Prices")
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        try:
            gb = historical_data[historical_data['source'] == 'gasbuddy_fuel_insights'].copy()
            
            if not gb.empty:
                gb['scraped_at'] = _to_datetime(gb['scraped_at'])
                
                # Current month filter
                today_dt = datetime.now()
                month_start = today_dt.replace(day=1).date()
                gb['date'] = gb['scraped_at'].dt.date
                
                gb_mtd = gb[(gb['date'] >= month_start) & (gb['date'] <= today_dt.date())].copy()
                
                if not gb_mtd.empty:
                    fig_gb = go.Figure()
                    
                    # GasBuddy line connecting each point
                    fig_gb.add_trace(go.Scatter(
                        x=gb_mtd['scraped_at'], y=gb_mtd['price'],
                        mode='lines+markers',
                        name='GasBuddy Prices',
                        line=dict(color='#1f77b4', width=2),
                        marker=dict(size=6, color='#1f77b4'),
                        hovertemplate='Time (EST)=%{x}<br>Price=$%{y:.3f}<extra></extra>'
                    ))
                    
                    fig_gb.update_layout(
                        xaxis_title="Date & Time (EST)",
                        yaxis_title="Price ($/gal)",
                        height=450,
                        margin=dict(l=40, r=20, t=40, b=40),
                        hovermode='x unified',
                        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
                    )
                    
                    st.plotly_chart(fig_gb, use_container_width=True)
                    
                    st.info("🔄 **GasBuddy**: Live ticking average updated every 10 minutes. Shows all data points collected during the current month.")
                else:
                    st.warning("No GasBuddy data available for current month")
            else:
                st.warning("No GasBuddy data available")
                
        except Exception as e:
            st.error(f"Error rendering GasBuddy chart: {e}")
            st.exception(e)
    
    with col2:
        st.markdown('<div class="data-table">', unsafe_allow_html=True)
        st.markdown('<h4>GasBuddy Averages</h4>', unsafe_allow_html=True)
        
        try:
            # Get GasBuddy data for the current month - recompute independently
            gb_data = historical_data[historical_data['source'] == 'gasbuddy_fuel_insights'].copy()
            
            if not gb_data.empty:
                # Convert timestamps to EST and handle timezone properly
                gb_data['scraped_at'] = pd.to_datetime(gb_data['scraped_at'], utc=True)
                gb_data['scraped_at'] = gb_data['scraped_at'].dt.tz_convert('America/New_York')
                gb_data['date'] = gb_data['scraped_at'].dt.date
                
                # Get current date info in EST
                today = datetime.now().date()
                today_est = pd.Timestamp.now(tz='America/New_York')
                
                # Daily average (today's data only)
                today_data = gb_data[gb_data['date'] == today]
                daily_avg = today_data['price'].mean() if not today_data.empty else None
                
                # Month-to-date average (from 1st of current month)
                month_start = today.replace(day=1)
                mtd_data = gb_data[gb_data['date'] >= month_start]
                mtd_avg = mtd_data['price'].mean() if not mtd_data.empty else None
                
                # Week-to-date average (Monday noon EST to current)
                days_since_monday = today_est.weekday()
                monday_noon_est = today_est - timedelta(days=days_since_monday)
                monday_noon_est = monday_noon_est.replace(hour=12, minute=0, second=0, microsecond=0)
                
                # If we're before Monday noon, go back to previous Monday
                if today_est < monday_noon_est:
                    monday_noon_est -= timedelta(days=7)
                
                wtd_data = gb_data[gb_data['scraped_at'] >= monday_noon_est]
                wtd_avg = wtd_data['price'].mean() if not wtd_data.empty else None
                
                # Create simple averages dataframe
                avg_data = {
                    'Period': ['Daily (EST)', 'Month-to-Date', 'Week-to-Date'],
                    'Average Price': [
                        f"${daily_avg:.3f}" if daily_avg is not None else "No data",
                        f"${mtd_avg:.3f}" if mtd_avg is not None else "No data",
                        f"${wtd_avg:.3f}" if wtd_avg is not None else "No data"
                    ]
                }
                
                df_gb = pd.DataFrame(avg_data)
                st.dataframe(df_gb, use_container_width=True, hide_index=True)
                
                # Add CSV download button below the table - recompute gb_mtd for CSV
                gb_mtd_csv = gb_data[(gb_data['date'] >= month_start) & (gb_data['date'] <= today)].copy()
                if not gb_mtd_csv.empty:
                    csv_data = create_chart_csv(gb_mtd_csv, 'GasBuddy', 'price')
                    if csv_data:
                        st.download_button(
                            label="📥 Download CSV",
                            data=csv_data,
                            file_name=f"gasbuddy_mtd_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime="text/csv"
                        )
            else:
                # No data available - show empty table
                avg_data = {
                    'Period': ['Daily (EST)', 'Month-to-Date', 'Week-to-Date'],
                    'Average Price': ['No data', 'No data', 'No data']
                }
                df_gb = pd.DataFrame(avg_data)
                st.dataframe(df_gb, use_container_width=True, hide_index=True)
                st.info("No GasBuddy data available")
                
        except Exception as e:
            st.error(f"Error calculating GasBuddy averages: {e}")
            # Fallback table
            avg_data = {
                'Period': ['Daily (EST)', 'Month-to-Date', 'Week-to-Date'],
                'Average Price': ['Error', 'Error', 'Error']
            }
            df_gb = pd.DataFrame(avg_data)
            st.dataframe(df_gb, use_container_width=True, hide_index=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

    # ---------- 2) AAA Daily Chart (3/5 width) + Table (2/5 width) ----------
    st.markdown('<div class="chart-section">', unsafe_allow_html=True)
    st.subheader("2. AAA Daily Gas Prices")
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        try:
            aaa = historical_data[historical_data['source'] == 'aaa_gas_prices'].copy()
            
            if not aaa.empty:
                aaa['scraped_at'] = _to_datetime(aaa['scraped_at'])
                
                # Current month filter
                today_dt = datetime.now()
                month_start = today_dt.replace(day=1).date()
                aaa['date'] = aaa['scraped_at'].dt.date
                
                aaa_mtd = aaa[(aaa['date'] >= month_start) & (aaa['date'] <= today_dt.date())].copy()
                
                if not aaa_mtd.empty:
                    fig_aaa = go.Figure()
                    
                    # AAA line connecting each point
                    fig_aaa.add_trace(go.Scatter(
                        x=aaa_mtd['scraped_at'], y=aaa_mtd['price'],
                        mode='lines+markers',
                        name='AAA Daily Prices',
                        line=dict(color='#ff7f0e', width=2),
                        marker=dict(size=8, color='#ff7f0e'),
                        hovertemplate='Date (EST)=%{x}<br>AAA Price=$%{y:.3f}<extra></extra>'
                    ))
                    
                    fig_aaa.update_layout(
                        xaxis_title="Date (EST)",
                        yaxis_title="Price ($/gal)",
                        height=450,
                        margin=dict(l=40, r=20, t=40, b=40),
                        hovermode='x unified',
                        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
                    )
                    
                    st.plotly_chart(fig_aaa, use_container_width=True)
                    
                    st.info("⛽ **AAA**: Daily national average updated once daily at 3:30 AM EST. Shows daily data points for the current month.")
                else:
                    st.warning("No AAA data available for current month")
            else:
                st.warning("No AAA data available")
                
        except Exception as e:
            st.error(f"Error rendering AAA chart: {e}")
            st.exception(e)
    
    with col2:
        st.markdown('<div class="data-table">', unsafe_allow_html=True)
        st.markdown('<h4>AAA Averages</h4>', unsafe_allow_html=True)
        
        try:
            # Get AAA data for the current month - recompute independently
            aaa_data = historical_data[historical_data['source'] == 'aaa_gas_prices'].copy()
            
            if not aaa_data.empty:
                # Convert timestamps to EST and handle timezone properly
                aaa_data['scraped_at'] = pd.to_datetime(aaa_data['scraped_at'], utc=True)
                aaa_data['scraped_at'] = aaa_data['scraped_at'].dt.tz_convert('America/New_York')
                aaa_data['date'] = aaa_data['scraped_at'].dt.date
                
                # Get current date info in EST
                today = datetime.now().date()
                today_est = pd.Timestamp.now(tz='America/New_York')
                
                # Daily average (today's data only)
                today_data = aaa_data[aaa_data['date'] == today]
                daily_avg = today_data['price'].mean() if not today_data.empty else None
                
                # Month-to-date average (from 1st of current month)
                month_start = today.replace(day=1)
                mtd_data = aaa_data[aaa_data['date'] >= month_start]
                mtd_avg = mtd_data['price'].mean() if not mtd_data.empty else None
                
                # Week-to-date average (Monday noon EST to current)
                days_since_monday = today_est.weekday()
                monday_noon_est = today_est - timedelta(days=days_since_monday)
                monday_noon_est = monday_noon_est.replace(hour=12, minute=0, second=0, microsecond=0)
                
                # If we're before Monday noon, go back to previous Monday
                if today_est < monday_noon_est:
                    monday_noon_est -= timedelta(days=7)
                
                wtd_data = aaa_data[aaa_data['scraped_at'] >= monday_noon_est]
                wtd_avg = wtd_data['price'].mean() if not wtd_data.empty else None
                
                # Create simple averages dataframe
                avg_data = {
                    'Period': ['Daily (EST)', 'Month-to-Date', 'Week-to-Date'],
                    'Average Price': [
                        f"${daily_avg:.3f}" if daily_avg is not None else "No data",
                        f"${mtd_avg:.3f}" if mtd_avg is not None else "No data",
                        f"${wtd_avg:.3f}" if wtd_avg is not None else "No data"
                    ]
                }
                
                df_aaa = pd.DataFrame(avg_data)
                st.dataframe(df_aaa, use_container_width=True, hide_index=True)
                
                # Add CSV download button below the table - recompute aaa_mtd for CSV
                aaa_mtd_csv = aaa_data[(aaa_data['date'] >= month_start) & (aaa_data['date'] <= today)].copy()
                if not aaa_mtd_csv.empty:
                    csv_data = create_chart_csv(aaa_mtd_csv, 'AAA', 'price')
                    if csv_data:
                        st.download_button(
                            label="📥 Download CSV",
                            data=csv_data,
                            file_name=f"aaa_daily_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime="text/csv"
                        )
            else:
                # No data available - show empty table
                avg_data = {
                    'Period': ['Daily (EST)', 'Month-to-Date', 'Week-to-Date'],
                    'Average Price': ['No data', 'No data', 'No data']
                }
                df_aaa = pd.DataFrame(avg_data)
                st.dataframe(df_aaa, use_container_width=True, hide_index=True)
                st.info("No AAA data available")
                
        except Exception as e:
            st.error(f"Error calculating AAA averages: {e}")
            # Fallback table
            avg_data = {
                'Period': ['Daily (EST)', 'Month-to-Date', 'Week-to-Date'],
                'Average Price': ['Error', 'Error', 'Error']
            }
            df_aaa = pd.DataFrame(avg_data)
            st.dataframe(df_aaa, use_container_width=True, hide_index=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

    # ---------- 3) Crack Spread Chart (3/5 width) + Table (2/5 width) ----------
    st.markdown('<div class="chart-section">', unsafe_allow_html=True)
    st.subheader("3. Crack Spread Analysis (Last 30 Days)")
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        try:
            rbob = historical_data[historical_data['source'] == 'marketwatch_rbob_futures'].copy()
            wti = historical_data[historical_data['source'] == 'marketwatch_wti_futures'].copy()
            
            if not rbob.empty and not wti.empty:
                rbob['scraped_at'] = _to_datetime(rbob['scraped_at'])
                wti['scraped_at'] = _to_datetime(wti['scraped_at'])
                
                # Resample to daily averages for cleaner view
                rbob_daily = (rbob.set_index('scraped_at')
                                   .resample('D')['price']
                                   .mean()
                                   .reset_index()
                                   .rename(columns={'price':'RBOB'}))
                wti_daily = (wti.set_index('scraped_at')
                                  .resample('D')['price']
                                  .mean()
                                  .reset_index()
                                  .rename(columns={'price':'WTI'}))
                
                # Merge on date for shared x-axis
                rbob_daily['date'] = rbob_daily['scraped_at'].dt.date
                wti_daily['date'] = wti_daily['scraped_at'].dt.date
                futures_df = pd.merge(rbob_daily[['date','RBOB']], wti_daily[['date','WTI']], on='date', how='outer').sort_values('date')
                
                if not futures_df.empty:
                    crack_df = futures_df.copy()
                    # NEW FORMULA: (RBOB * 42) - WTI (in dollars)
                    crack_df['Crack_Spread'] = (crack_df['RBOB'] * 42) - crack_df['WTI']
                    
                    # Remove any infinite or NaN values
                    crack_df = crack_df.dropna()
                    
                    if not crack_df.empty:
                        fig_crack = go.Figure()
                        
                        # Main crack spread line
                        fig_crack.add_trace(go.Scatter(
                            x=crack_df['date'], y=crack_df['Crack_Spread'],
                            mode='lines+markers',
                            name='Crack Spread ($)',
                            line=dict(color='#9467bd', width=3),
                            marker=dict(size=8, color='#9467bd'),
                            hovertemplate='Date (EST)=%{x}<br>Crack Spread=$%{y:.2f}<extra></extra>'
                        ))
                        
                        # Add horizontal line at zero for reference
                        fig_crack.add_hline(y=0, line_dash="dash", line_color="gray", 
                                          annotation_text="Break-even", annotation_position="bottom right")
                        
                        fig_crack.update_layout(
                            xaxis_title="Date (EST)",
                            yaxis_title="Crack Spread ($)",
                            height=450,
                            margin=dict(l=40, r=20, t=40, b=40),
                            hovermode='x unified',
                            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
                        )
                        
                        st.plotly_chart(fig_crack, use_container_width=True)
                        
                        st.info("🏭 **Crack Spread** = (RBOB gasoline price × 42) - WTI crude price. This represents the theoretical refining margin in dollars. Positive values indicate profitable refining conditions.")
                    else:
                        st.warning("No valid crack spread data available")
                else:
                    st.warning("No futures data available for crack spread calculation")
            else:
                st.warning("Missing futures data sources for crack spread")
                
        except Exception as e:
            st.error(f"Error rendering Crack Spread chart: {e}")
            st.exception(e)
    
    with col2:
        st.markdown('<div class="data-table">', unsafe_allow_html=True)
        st.markdown('<h4>Crack Spread Averages</h4>', unsafe_allow_html=True)
        
        try:
            # Check if crack_df exists and has data
            if 'crack_df' in locals() and not crack_df.empty:
                # Create a copy for calculations
                crack_calc = crack_df.copy()
                # Make both sides America/New_York tz-aware
                crack_calc['scraped_at'] = pd.to_datetime(crack_calc['date']).dt.tz_localize('America/New_York')
                
                # Get current date info in EST
                today = datetime.now().date()
                today_est = pd.Timestamp.now(tz='America/New_York')
                
                # Daily average (today's data only)
                today_data = crack_calc[crack_calc['scraped_at'].dt.date == today]
                daily_avg = today_data['Crack_Spread'].mean() if not today_data.empty else None
                
                # Month-to-date average (from 1st of current month)
                month_start = today.replace(day=1)
                mtd_data = crack_calc[crack_calc['scraped_at'].dt.date >= month_start]
                mtd_avg = mtd_data['Crack_Spread'].mean() if not mtd_data.empty else None
                
                # Week-to-date average (Monday noon EST to current)
                days_since_monday = today_est.weekday()
                monday_noon_est = (today_est - timedelta(days=days_since_monday)).replace(hour=12, minute=0, second=0, microsecond=0)
                
                # If we're before Monday noon, go back to previous Monday
                if today_est < monday_noon_est:
                    monday_noon_est -= timedelta(days=7)
                
                wtd_data = crack_calc[crack_calc['scraped_at'] >= monday_noon_est]
                wtd_avg = wtd_data['Crack_Spread'].mean() if not wtd_data.empty else None
                
                # Create simple averages dataframe
                avg_data = {
                    'Period': ['Daily (EST)', 'Month-to-Date', 'Week-to-Date'],
                    'Average Value': [
                        f"${daily_avg:.2f}" if daily_avg is not None else "No data",
                        f"${mtd_avg:.2f}" if mtd_avg is not None else "No data",
                        f"${wtd_avg:.2f}" if wtd_avg is not None else "No data"
                    ]
                }
                
                df_crack = pd.DataFrame(avg_data)
                st.dataframe(df_crack, use_container_width=True, hide_index=True)
                
                # Add CSV download button below the table
                csv_data = create_chart_csv(crack_df, 'Crack Spread', 'crack_spread')
                if csv_data:
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv_data,
                        file_name=f"crack_spread_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
            else:
                # No data available - show empty table
                avg_data = {
                    'Period': ['Daily (EST)', 'Month-to-Date', 'Week-to-Date'],
                    'Average Value': ['No data', 'No data', 'No data']
                }
                df_crack = pd.DataFrame(avg_data)
                st.dataframe(df_crack, use_container_width=True, hide_index=True)
                st.info("No Crack Spread data available")
                
        except Exception as e:
            st.error(f"Error calculating Crack Spread averages: {e}")
            # Fallback table
            avg_data = {
                'Period': ['Daily (EST)', 'Month-to-Date', 'Week-to-Date'],
                'Average Value': ['Error', 'Error', 'Error']
            }
            df_crack = pd.DataFrame(avg_data)
            st.dataframe(df_crack, use_container_width=True, hide_index=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

    # ===========================
    # 🧮 NEW MODELING SECTION
    # ===========================
    st.markdown("---")
    st.header("Gas Price Forecasting & Modeling")
    
    try:
        # Data validation before modeling
        required_sources = ['gasbuddy_fuel_insights', 'marketwatch_rbob_futures', 'marketwatch_wti_futures']
        available_sources = historical_data['source'].unique()
        missing_sources = [src for src in required_sources if src not in available_sources]
        
        if missing_sources:
            st.warning(f"⚠️ Missing required data sources for modeling: {', '.join(missing_sources)}")
            st.info("The forecasting model requires data from GasBuddy, RBOB futures, and WTI futures sources.")
            st.info("Please ensure your scraper has collected sufficient data from these sources.")
            return
        
        # Check data volume
        total_records = len(historical_data)
        if total_records < 100:
            st.warning(f"⚠️ Insufficient data for reliable modeling. Found {total_records} records, need at least 100.")
            st.info("The model requires sufficient historical data to make accurate predictions.")
            return
        
        # Check for recent data
        latest_date = historical_data['scraped_at'].max()
        if pd.isna(latest_date):
            st.error("❌ No valid timestamps found in the data.")
            return
        
        days_old = (datetime.now() - latest_date).days
        if days_old > 7:
            st.warning(f"⚠️ Data is {days_old} days old. For best results, ensure data is updated regularly.")
        
        st.success(f"✅ Data validation passed. Found {total_records} records from {len(available_sources)} sources.")
        
        # Run the forecasting model
        res = fit_forecast_with_lags(historical_data, sim_n=2000, max_lag=14)
        
        # Check if modeling failed
        if "error" in res:
            st.error(f"❌ Modeling failed: {res['error']}")
            st.info("This usually means insufficient data or data quality issues. Check the data requirements above.")
            return
        
        # ---------- 1) MTD Fan Chart (3/5 width) + Threshold Probabilities (2/5 width) ----------
        st.markdown('<div class="chart-section">', unsafe_allow_html=True)
        st.subheader("1. Month-to-Date Fan Chart with Threshold Probabilities")
        
        col1, col2 = st.columns([3, 2])
        
        with col1:
            if len(res["future_dates"]) > 0 and res["paths"].shape[1] > 0:
                pct = [5, 25, 50, 75, 95]
                bands = {p: np.percentile(res["paths"], p, axis=0) for p in pct}
                fig_fan = go.Figure()
                
                # Get MTD data from model_df if available
                try:
                    mtd_data = res.get("mtd", pd.DataFrame())
                    if not mtd_data.empty:
                        fig_fan.add_trace(go.Scatter(
                            x=mtd_data["date"], 
                            y=mtd_data["gas_price"],
                            mode="lines", 
                            name="Actual (MTD)",
                            line=dict(color='#1f77b4', width=3)
                        ))
                except Exception as e:
                    st.warning(f"Could not display MTD data: {e}")
                
                fig_fan.add_trace(go.Scatter(
                    x=res["future_dates"], 
                    y=bands[50], 
                    mode="lines", 
                    name="Median Forecast",
                    line=dict(color='#ff7f0e', width=3)
                ))
                
                fig_fan.add_trace(go.Scatter(
                    x=np.concatenate([res["future_dates"], res["future_dates"][::-1]]),
                    y=np.concatenate([bands[75], bands[25][::-1]]),
                    fill="toself", 
                    name="50% Confidence Band", 
                    opacity=0.3, 
                    line=dict(width=0),
                    fillcolor='rgba(255, 127, 14, 0.3)'
                ))
                
                fig_fan.add_trace(go.Scatter(
                    x=np.concatenate([res["future_dates"], res["future_dates"][::-1]]),
                    y=np.concatenate([bands[95], bands[5][::-1]]),
                    fill="toself", 
                    name="90% Confidence Band", 
                    opacity=0.2, 
                    line=dict(width=0),
                    fillcolor='rgba(255, 127, 14, 0.2)'
                ))
                
                fig_fan.update_layout(
                    title=f"Month-to-Date Fan Chart → {res['month_end'].date()}",
                    xaxis_title="Date (EST)",
                    yaxis_title="Price ($/gal)",
                    height=450,
                    margin=dict(l=40, r=20, t=40, b=40),
                    hovermode='x unified',
                    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
                )
                
                st.plotly_chart(fig_fan, use_container_width=True)
                
                st.info("📊 **Fan Chart**: Shows actual month-to-date prices with forecasted ranges. The bands represent confidence intervals for the remainder of the month.")
            else:
                st.info("No remaining days in the current month to forecast.")
        
        with col2:
            st.markdown('<div class="data-table">', unsafe_allow_html=True)
            st.markdown('<h4>Threshold Probabilities</h4>', unsafe_allow_html=True)
            st.markdown('<p style="font-size: 0.9rem; color: #666;">P(Month Avg > threshold) ±$0.05 around current MTD avg</p>', unsafe_allow_html=True)
            
            # Display threshold probabilities table
            if not res["prob_table"].empty:
                st.dataframe(res["prob_table"], use_container_width=True, hide_index=True)
            else:
                st.info("No threshold probabilities available")
            
            # Display EOM statistics
            if not np.isnan(res['eom_mean']):
                st.markdown(f"**End-of-Month Forecast:**")
                st.markdown(f"**Mean:** ${res['eom_mean']:.3f}")
                st.markdown(f"**90% CI:** [{res['ci5']:.3f}, {res['ci95']:.3f}]")
            
            # Display model metrics
            if not np.isnan(res['metrics']['MAE']):
                st.markdown(f"**Model Performance (80/20 split):**")
                st.markdown(f"**MAE:** {res['metrics']['MAE']:.4f}")
                st.markdown(f"**RMSE:** {res['metrics']['RMSE']:.4f}")
                st.markdown(f"**R²:** {res['metrics']['R2']:.3f}")
            else:
                st.markdown("_Insufficient history for reliable out-of-sample validation._")
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # ---------- 2) Predicted vs Actual Chart (±2 weeks around today) ----------
        st.markdown('<div class="chart-section">', unsafe_allow_html=True)
        st.subheader("2. Predicted vs Actual — ±2 Weeks Around Today")
        
        fig_pa = go.Figure()
        
        # Simplified display - just show what we have
        try:
            # Get actual data from historical_data for the last 30 days
            actual_data = historical_data[historical_data['source'] == 'gasbuddy_fuel_insights'].copy()
            if not actual_data.empty:
                actual_data['scraped_at'] = pd.to_datetime(actual_data['scraped_at'], utc=True)
                actual_data['scraped_at'] = actual_data['scraped_at'].dt.tz_convert('America/New_York')
                actual_data['date'] = actual_data['scraped_at'].dt.date
                
                # Show last 30 days of actual data
                fig_pa.add_trace(go.Scatter(
                    x=actual_data['date'], 
                    y=actual_data['price'],
                    mode="lines+markers", 
                    name="Actual GasBuddy Prices",
                    line=dict(color='#1f77b4', width=3),
                    marker=dict(size=6, color='#1f77b4')
                ))
                
                fig_pa.update_layout(
                    title="Actual GasBuddy Prices (Last 30 Days)",
                    xaxis_title="Date (EST)",
                    yaxis_title="Price ($/gal)",
                    height=450,
                    margin=dict(l=40, r=20, t=40, b=40),
                    hovermode='x unified',
                    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
                )
                
                st.plotly_chart(fig_pa, use_container_width=True)
                st.info("🔍 **Actual Data**: Shows the last 30 days of actual GasBuddy prices.")
            else:
                st.warning("No actual data available for display")
                
        except Exception as e:
            st.error(f"Error displaying actual data: {e}")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # ---------- 3) Weekly Live Probability & WTD Path ----------
        st.markdown('<div class="chart-section">', unsafe_allow_html=True)
        st.subheader("3. Weekly Live View (Mon 12:00 ET → Next Mon 12:00 ET)")
        
        # User input for threshold
        thr_val = st.number_input(
            "Threshold ($/gal): Probability that next Monday's AAA national daily value (12:00 ET) is strictly greater than…",
            min_value=0.00, 
            max_value=10.00, 
            value=3.15, 
            step=0.01, 
            format="%.2f"
        )
        
        try:
            # Simplified weekly view using available data
            now_nyc = nyc_now()
            wk_start, wk_end = week_window(now_nyc)
            
            # Get AAA data for the week
            aaa_weekly = historical_data[historical_data['source'] == 'aaa_gas_prices'].copy()
            if not aaa_weekly.empty:
                aaa_weekly['scraped_at'] = pd.to_datetime(aaa_weekly['scraped_at'], utc=True)
                aaa_weekly['scraped_at'] = aaa_weekly['scraped_at'].dt.tz_convert('America/New_York')
                aaa_weekly['date'] = aaa_weekly['scraped_at'].dt.date
                
                # Filter to current week
                week_start_date = wk_start.date()
                week_end_date = wk_end.date()
                week_data = aaa_weekly[(aaa_weekly['date'] >= week_start_date) & (aaa_weekly['date'] <= week_end_date)]
                
                if not week_data.empty:
                    fig_weekly = go.Figure()
                    
                    fig_weekly.add_trace(go.Scatter(
                        x=week_data['date'], 
                        y=week_data['price'],
                        mode="lines+markers", 
                        name="AAA Weekly Prices", 
                        line=dict(color='#ff7f0e', width=3),
                        marker=dict(size=8, color='#ff7f0e')
                    ))
                    
                    fig_weekly.update_layout(
                        title=f"Weekly AAA Prices: {week_start_date} to {week_end_date}",
                        xaxis_title="Date (EST)",
                        yaxis_title="Price ($/gal)",
                        height=450,
                        margin=dict(l=40, r=20, t=40, b=40),
                        hovermode='x unified',
                        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
                    )
                    
                    st.plotly_chart(fig_weekly, use_container_width=True)
                    st.info(f"📈 **Weekly View**: Shows AAA prices for the current week ({week_start_date} to {week_end_date}).")
                else:
                    st.info("No AAA data available for the current week.")
            else:
                st.info("No AAA data available for weekly view.")
                
        except Exception as e:
            st.error(f"Error in weekly view: {e}")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
    except Exception as e:
        st.error(f"Modeling failed: {e}")
        st.exception(e)
        st.info("The forecasting model requires sufficient historical data from multiple sources (GasBuddy, RBOB, WTI, Gasoline Stocks, Refinery Runs). Please ensure you have at least 30 days of data from these sources.")

    # Footer
    st.markdown("---")
    st.markdown("Dashboard updated every 5 minutes | Data from multiple sources including GasBuddy, AAA, MarketWatch, and Trading Economics")

if __name__ == "__main__":
    main()
