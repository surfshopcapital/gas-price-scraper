import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import os
import numpy as np
from sklearn.linear_model import RidgeCV
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from zoneinfo import ZoneInfo

# ============== PAGE & CSS ==============
st.set_page_config(page_title="Gas Price Dashboard", page_icon="⛽", layout="wide", initial_sidebar_state="expanded")

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
.data-source { background: #ffffff; padding: 1.5rem; border-radius: 0.5rem; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 1rem; }
.data-export { background: #f3e5f5; border-radius: 0.8rem; padding: 1.5rem; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-left: 4px solid #9c27b0; }
.summary-stats { background: #e8f5e8; border-radius: 0.8rem; padding: 1.5rem; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-left: 4px solid #4caf50; }
.stApp { background: #f8f9fa; }
.main .block-container { background: #ffffff; border-radius: 1rem; padding: 2rem; margin: 1rem; box-shadow: 0 8px 32px rgba(0,0,0,0.1); }
.update-schedule { background: #e3f2fd; border-radius: 0.8rem; padding: 1.5rem; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-left: 4px solid #2196f3; }
.update-schedule h3 { color: #2c3e50; margin-bottom: 1rem; }
.update-metric { text-align: center; }
.update-metric .metric-label { font-size: 1.1rem; font-weight: bold; color: #1f77b4; margin-bottom: 0.5rem; }
.update-metric .metric-value { font-size: 0.85rem; color: #6c757d; line-height: 1.3; }
/* kill the white card look around side tables */
.data-table { background: transparent; box-shadow: none; padding: 0; }
/* center the table alongside the chart */
.right-table-wrap { 
  display: flex; 
  height: 450px;               /* same as charts */
  align-items: center; 
  justify-content: center; 
}
.right-table-wrap .stDataFrame { width: 100%; max-width: 520px; }
/* optional: tone down headings */
.data-table h4 { display:none; }   /* remove those titles entirely */
</style>
""", unsafe_allow_html=True)

# ============== UTILS ==============
@st.cache_data(ttl=300)
def load_data():
    try:
        database_url = os.getenv('DATABASE_URL',
            'postgresql://postgres:QKmnqfjnamSWVXIgeEZZctczGSYOZiKw@switchback.proxy.rlwy.net:51447/railway')
        conn = psycopg2.connect(database_url)

        latest_data = pd.read_sql("""
            SELECT source, fuel_type, price, timestamp, region, consensus, surprise, scraped_at
            FROM gas_prices
            WHERE
              (source != 'aaa_gas_prices' AND scraped_at = (
                 SELECT MAX(scraped_at) FROM gas_prices p2 WHERE p2.source = gas_prices.source
              ))
              OR
              (source = 'aaa_gas_prices' AND timestamp LIKE '%Current%')
            ORDER BY scraped_at DESC
        """, conn)

        previous_data = pd.read_sql("""
            SELECT source, price, timestamp, consensus, surprise, scraped_at
            FROM gas_prices p1
            WHERE scraped_at = (
                SELECT MAX(scraped_at) FROM gas_prices p2
                WHERE p2.source = p1.source
                  AND p2.scraped_at < (SELECT MAX(scraped_at) FROM gas_prices p3 WHERE p3.source = p1.source)
            )
        """, conn)

        historical_data = pd.read_sql("""
            SELECT source, fuel_type, price, timestamp, region, consensus, surprise, scraped_at
            FROM gas_prices
            WHERE scraped_at >= CURRENT_DATE - INTERVAL '90 days'
            ORDER BY scraped_at ASC
        """, conn)
        
        # Check if as_of_date column exists, if not, add it as None
        if 'as_of_date' not in historical_data.columns:
            print("Debug: as_of_date column not found in database, adding as None")
            historical_data['as_of_date'] = None
        conn.close()
        return latest_data, previous_data, historical_data
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None, None

def to_est(s):
    out = pd.to_datetime(s, errors='coerce', utc=True)
    return out.dt.tz_convert('America/New_York')

def create_chart_csv(chart_data, chart_type):
    if chart_data.empty:
        return None
    if chart_type == 'crack_spread':
        c = chart_data[['date','Crack_Spread']].rename(columns={'date':'Date (EST)', 'Crack_Spread':'Crack_Spread_$'})
    else:
        c = chart_data[['scraped_at','price']].copy()
        c['scraped_at'] = to_est(c['scraped_at'])
        c.columns = ['Timestamp (EST)', 'Price_$']
    return c.to_csv(index=False)

def round_to_05(x):
    return np.round(x*20)/20.0  # steps of 0.05 → ends with .x0 or .x5

def make_threshold_table(eom_samples: np.ndarray, center: float) -> pd.DataFrame:
    if eom_samples.size == 0 or not np.isfinite(center):
        return pd.DataFrame(columns=["Threshold ($)", "Probability (%)"])
    c = round_to_05(center)  # This ensures multiples of 0.05
    # 3 below, 3 above, spaced by 0.05
    thresholds = np.array([c-0.10, c-0.05, c, c+0.05, c+0.10, c+0.15])
    rows = [{"Threshold ($)": f"${t:.2f}",
             "Probability (%)": f"{(eom_samples > t).mean()*100:.1f}%"} for t in thresholds]
    return pd.DataFrame(rows)

# >>> REPLACE the two helpers with this improved pair

def _simple_beta_fit_and_predict(h: pd.DataFrame, k: int = 7, lookback_days: int = 120):
    """
    Fits Retail_t = alpha + b1*(RB_{t-k}*42) + b2*WTI_{t-k} on a recent window.
    Uses AAA 'as-of-yesterday' first, then GasBuddy daily mean as fallback.

    Returns:
      last7_df:  last 7 days of actuals (AAA→GB)
      future_dates: next 7 daily dates
      preds: next 7-day point forecasts
      betas: (alpha, b1, b2)
      fit_used: DataFrame actually used in the OLS fit (for residuals)
      base: daily panel used to source lags & actuals
    """
    base = _daily_panel(h)  # AAA-first fallback, ET daily calendar
    if base.empty or base[["rbob", "wti", "gas_price"]].dropna().empty:
        return None, None, None, None, None, None

    base = base.sort_values("date").copy()
    base["rb42"] = base["rbob"] * 42.0
    base["rb42_lag"] = base["rb42"].shift(k)
    base["wti_lag"]  = base["wti"].shift(k)

    fit = base.dropna(subset=["gas_price", "rb42_lag", "wti_lag"]).copy()
    if fit.empty:
        return None, None, None, None, None, None

    # regime-aware lookback; fall back to all if too short
    cutoff = base["date"].max() - pd.Timedelta(days=lookback_days)
    fit_recent = fit[fit["date"] >= cutoff]
    use = fit_recent if len(fit_recent) >= 25 else fit

    X = np.column_stack([np.ones(len(use)), use["rb42_lag"].values, use["wti_lag"].values])
    y = use["gas_price"].values
    alpha, b1, b2 = np.linalg.lstsq(X, y, rcond=None)[0]

    # Previous 7 days of actuals
    last7_df = base.tail(7)[["date", "gas_price"]].copy()

    # Next 7 days predictions (use RB/WTI from t-k; fall back to latest if gap)
    start_date   = base["date"].max() + pd.Timedelta(days=1)
    future_dates = pd.date_range(start_date, periods=7, freq="D")
    preds = []
    for d in future_dates:
        ref = d - pd.Timedelta(days=k)
        row = base.loc[base["date"] == ref, ["rbob", "wti"]]
        if row.empty:
            rb = float(base["rbob"].dropna().iloc[-1])
            wt = float(base["wti"].dropna().iloc[-1])
        else:
            rb = float(row["rbob"].iloc[0])
            wt = float(row["wti"].iloc[0])
        preds.append(alpha + b1*(rb*42.0) + b2*wt)

    return last7_df, future_dates, np.array(preds), (float(alpha), float(b1), float(b2)), use[["rb42_lag","wti_lag","gas_price"]].copy(), base


def _render_simple_beta_block(h: pd.DataFrame, k: int = 7, lookback_days: int = 120, sim_n: int = 5000):
    last7, fdates, preds, betas, fit_used, base = _simple_beta_fit_and_predict(h, k=k, lookback_days=lookback_days)
    if last7 is None:
        st.info("Simple beta model: not enough data yet to fit.")
        return

    alpha, b1, b2 = betas

    # ===== Chart: last 7 actuals + next 7 forecasts (values labeled) =====
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=last7["date"], y=last7["gas_price"],
        mode="lines+markers", name="Actual (AAA→GasBuddy fallback)",
        hovertemplate="Date=%{x|%Y-%m-%d}<br>Retail=$%{y:.3f}<extra></extra>"
    ))
    fig.add_trace(go.Scatter(
        x=fdates, y=preds, mode="lines+markers+text", name=f"Forecast (k={k})",
        text=[f"<b>${v:.3f}</b>" for v in preds],
        textposition="top center",
        hovertemplate="Date=%{x|%Y-%m-%d}<br>Forecast=$%{y:.3f}<extra></extra>"
    ))
    fig.add_vline(x=last7["date"].max(), line_dash="dot", line_color="gray", opacity=0.5)
    fig.update_layout(
        title="Simple Beta Model: last 7 actuals and next 7-day forecast",
        xaxis_title="Date (ET)", yaxis_title="Retail price ($/gal)",
        height=380, margin=dict(l=40, r=20, t=40, b=40),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig, use_container_width=True)

    # ===== 7-row forecast table =====
    pred_df = pd.DataFrame({
        "Date": [d.date() for d in fdates],
        "Predicted Value": [f"${v:.3f}" for v in preds]
    })
    st.dataframe(pred_df, use_container_width=True, hide_index=True)

    # ===== Month-end thresholds (simple-beta) =====
    # Residual bootstrap on the fit_used window
    # build residuals
    yhat_fit = alpha + b1*fit_used["rb42_lag"].values + b2*fit_used["wti_lag"].values
    resid    = fit_used["gas_price"].values - yhat_fit
    if resid.size == 0:
        resid = np.array([0.0])

    # Determine the current month-end date
    today = base["date"].max()
    month_end = pd.Timestamp(year=today.year, month=today.month, day=pd.Period(today, 'M').days_in_month)

    # Forecast the (lagged) drivers for the month-end day using levels at (month_end - k)
    ref_eom = month_end - pd.Timedelta(days=k)
    row_eom = base.loc[base["date"] == ref_eom, ["rbob", "wti"]]
    if row_eom.empty:
        rb_eom = float(base["rbob"].dropna().iloc[-1])
        wt_eom = float(base["wti"].dropna().iloc[-1])
    else:
        rb_eom = float(row_eom["rbob"].iloc[0])
        wt_eom = float(row_eom["wti"].iloc[0])

    eom_point = alpha + b1*(rb_eom*42.0) + b2*wt_eom
    eom_samples = eom_point + np.random.choice(resid, size=sim_n, replace=True)

    # Make probability table (same format as the advanced model)
    prob_df = make_threshold_table(eom_samples, float(np.mean(eom_samples)))
    st.markdown("##### Month-End Thresholds (Simple Beta)")
    st.caption(f"Mean EOM daily price: ${np.mean(eom_samples):.3f} • 90% CI [{np.percentile(eom_samples,5):.3f}, {np.percentile(eom_samples,95):.3f}]")

    # simple color styling (local to avoid coupling to the advanced section)
    prob_df['Probability (%)'] = prob_df['Probability (%)'].str.rstrip('%').astype(float)
    def _color_prob(val):
        if pd.isna(val): return 'background-color: white'
        if val >= 80:    return 'background-color: #d4edda; color: #155724'
        if val >= 60:    return 'background-color: #c3e6cb; color: #155724'
        if val >= 40:    return 'background-color: #fff3cd; color: #856404'
        if val >= 20:    return 'background-color: #f8d7da; color: #721c24'
        return 'background-color: #f5c6cb; color: #721c24'
    st.dataframe(prob_df.style.applymap(_color_prob, subset=['Probability (%)']),
                 use_container_width=True, hide_index=True)

    # tiny caption with fitted betas
    st.caption(f"Fitted: Retail = {alpha:.3f} + {b1:.3f}·(RB*42 lag {k}) + {b2:.3f}·WTI lag {k}")

# >>> FIX for blank tables:
# The original code opened/closed custom HTML containers and then, inside multiple try/excepts,
# sometimes *returned early* or never reached st.dataframe when filters produced empty frames.
# I centralize the average computation and ALWAYS render a small 3-row table.
def averages_table(data: pd.DataFrame, label: str):
    if data.empty:
        df = pd.DataFrame({'Period':['Daily (EST)','Month-to-Date','Week-to-Date'],
                           f'{label}':[ 'No data','No data','No data']})
        st.dataframe(df, use_container_width=True, hide_index=True)
        return

    data = data.copy()
    data['scraped_at'] = to_est(data['scraped_at'])
    data['date'] = data['scraped_at'].dt.date

    today_est = datetime.now(tz=ZoneInfo("America/New_York"))
    today = today_est.date()
    month_start = today.replace(day=1)

    daily = data.loc[data['date']==today, 'price'].mean()
    mtd   = data.loc[data['date']>=month_start, 'price'].mean()

    # Week window = Mon 12:00 ET → now (EST). We use EST tz-aware timestamps.
    wd = today_est.weekday()
    monday_noon_est = (today_est - timedelta(days=wd)).replace(hour=12, minute=0, second=0, microsecond=0)
    if today_est < monday_noon_est:
        monday_noon_est -= timedelta(days=7)
    wtd = data.loc[data['scraped_at']>=monday_noon_est, 'price'].mean()

    fmt = lambda x, n=3: (f"${x:.{n}f}" if pd.notna(x) else "No data")
    df = pd.DataFrame({
        'Period':['Daily (EST)','Month-to-Date','Week-to-Date'],
        f'{label}':[fmt(daily), fmt(mtd), fmt(wtd)]
    })
    st.dataframe(df, use_container_width=True, hide_index=True)

# ============== MODEL ==============
def _daily_panel(h: pd.DataFrame) -> pd.DataFrame:
    df = h.copy()
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce", utc=True).dt.tz_convert("America/New_York")
    df = df.dropna(subset=["scraped_at"])
    df["date"] = pd.to_datetime(df["scraped_at"].dt.date)

    # --- GasBuddy daily mean (date = same calendar day, ET) ---
    gb = (df[df["source"]=="gasbuddy_fuel_insights"][["date","price"]]
            .groupby("date", as_index=False)["price"].mean()
            .rename(columns={"price":"gb"}))

    # --- AAA daily mean (value is "as-of previous day") ---
    aaa = df[df["source"]=="aaa_gas_prices"][["scraped_at","price"]].copy()
    if not aaa.empty:
        # Since as_of_date column doesn't exist yet, calculate from scraped_at
        aaa["date"] = (aaa["scraped_at"] - pd.Timedelta(days=1)).dt.normalize()  # yesterday's date
        aaa = (aaa.groupby("date", as_index=False)["price"].mean()
                 .rename(columns={"price":"aaa"}))
    else:
        aaa = pd.DataFrame(columns=["date","aaa"])

    # RBOB / WTI daily
    rbob = (df[df["source"]=="marketwatch_rbob_futures"][["date","price"]]
              .groupby("date", as_index=False)["price"].mean()
              .rename(columns={"price":"rbob"}))
    wti  = (df[df["source"]=="marketwatch_wti_futures"][["date","price"]]
              .groupby("date", as_index=False)["price"].mean()
              .rename(columns={"price":"wti"}))

    # TE extras
    stocks = (df[df["source"]=="tradingeconomics_gasoline_stocks"][["date","surprise"]]
                .dropna().drop_duplicates("date"))
    ref    = (df[df["source"]=="tradingeconomics_refinery_runs"][["date","price"]]
                .dropna().drop_duplicates("date").rename(columns={"price":"refinery"}))

    # Debug info
    print(f"Debug: GB records: {len(gb)}, AAA records: {len(aaa)}, RBOB records: {len(rbob)}, WTI records: {len(wti)}")
    
    # calendar base
    frames = [g for g in [gb, aaa, rbob, wti] if not g.empty]
    if not frames:
        print("Debug: No usable frames found")
        return pd.DataFrame(columns=["date","gas_price","rbob","wti","surprise","refinery","crack"])
    
    # Ensure all dates are timezone-naive for comparison
    for frame in frames:
        if not frame.empty:
            frame.loc[:, "date"] = frame["date"].dt.tz_localize(None)
    
    start, end = min(x["date"].min() for x in frames), max(x["date"].max() for x in frames)
    base = pd.DataFrame({"date": pd.date_range(start, end, freq="D")})
    print(f"Debug: Date range: {start} to {end}, {len(base)} days")

    for d in (aaa, gb, rbob, wti):
        base = base.merge(d, on="date", how="left")
    base = base.merge(stocks, on="date", how="left")
    base = base.merge(ref,    on="date", how="left")

    # AAA first, else GasBuddy, else backfill
    base["gas_price"] = base["aaa"].combine_first(base["gb"])
    base["gas_price"] = base["gas_price"].ffill().bfill()

    for c in ["rbob","wti","surprise","refinery"]:
        if c in base.columns:
            base[c] = base[c].ffill()

    base["crack"] = (base["rbob"] * 42.0) - base["wti"]
    print(f"Debug: Final base shape: {base.shape}, non-null gas_price: {base['gas_price'].notna().sum()}")
    return base

def _make_pulses(df: pd.DataFrame, decay=[1.0, 0.6, 0.36, 0.216]) -> pd.DataFrame:
    out = df.set_index("date").copy()
    idx = out.index
    for name in ["surprise","refinery"]:
        s = out[name].fillna(0.0) if name in out else pd.Series(0.0, index=idx)
        pulse = pd.Series(0.0, index=idx)
        for i,w in enumerate(decay):
            pulse = pulse.add(s.shift(i, fill_value=0.0)*w, fill_value=0.0)
        out[f"{name}_pulse"] = pulse
    return out.reset_index()

def _add_lags(base: pd.DataFrame, max_lag: int = 14) -> pd.DataFrame:
    df = base.copy()
    for lag in range(0, max_lag+1):
        df[f"rbob_l{lag}"]  = df["rbob"].shift(lag)
        df[f"wti_l{lag}"]   = df["wti"].shift(lag)
        df[f"crack_l{lag}"] = df["crack"].shift(lag)
    d = df["rbob"].diff()
    df["rbob_up"]   = np.where(d>0, df["rbob"], 0.0)
    df["rbob_down"] = np.where(d<=0, df["rbob"], 0.0)
    df["gas_lag1"]  = df["gas_price"].shift(1)
    return df

def simulate_forecast(historical: pd.DataFrame, sim_n=2000, max_lag=14, verbose=False):
    print(f"Debug: Starting simulation with {len(historical)} historical records")
    # Build features
    base = _daily_panel(historical)
    if base.empty:
        print("Debug: _daily_panel returned empty DataFrame")
        return {"error":"No usable daily panel"}
    print(f"Debug: _daily_panel returned {len(base)} rows")
    
    base = _make_pulses(base)
    base = _add_lags(base, max_lag=max_lag)

    model_df = base.dropna(subset=["gas_price","gas_lag1"]).copy()
    print(f"Debug: After dropna, model_df has {len(model_df)} rows")
    
    if len(model_df) < 10:
        print("Debug: Insufficient data after dropna")
        return {"error":"Insufficient history for modeling"}
    
    lag_cols = ([f"rbob_l{l}" for l in range(max_lag+1)]
              + [f"wti_l{l}"  for l in range(max_lag+1)]
              + [f"crack_l{l}" for l in range(max_lag+1)])
    feat_cols = [c for c in ["gas_lag1","rbob_up","rbob_down","surprise_pulse","refinery_pulse"]+lag_cols
                 if c in model_df.columns]
    print(f"Debug: Using {len(feat_cols)} feature columns: {feat_cols[:5]}...")

    X = (model_df[feat_cols].fillna(method="ffill").fillna(method="bfill"))
    y = model_df["gas_price"].copy()
    print(f"Debug: X shape: {X.shape}, y shape: {y.shape}")
    
    if len(X) < 10:
        return {"error":"Insufficient history for modeling"}

    cut = int(len(model_df)*0.8)
    Xtr, Xte, ytr, yte = X.iloc[:cut], X.iloc[cut:], y.iloc[:cut], y.iloc[cut:]

    ridge = RidgeCV(alphas=np.logspace(-4,3,21))
    ridge.fit(Xtr, ytr)
    ypred = ridge.predict(Xte) if len(Xte)>0 else np.array([])
    metrics = {
        "MAE": float(mean_absolute_error(yte, ypred)) if len(ypred)>0 else np.nan,
        "RMSE": float(np.sqrt(mean_squared_error(yte, ypred))) if len(ypred)>0 else np.nan,
        "R2": float(r2_score(yte, ypred)) if len(ypred)>0 else np.nan,
    }

    today = model_df["date"].max()
    days_in_month = pd.Period(today, 'M').days_in_month
    month_start = pd.Timestamp(year=today.year, month=today.month, day=1)
    month_end   = pd.Timestamp(year=today.year, month=today.month, day=days_in_month)

    mtd = model_df[(model_df["date"]>=month_start) & (model_df["date"]<=today)][["date","gas_price"]].sort_values("date")
    mtd_avg = float(mtd["gas_price"].mean()) if not mtd.empty else np.nan
    last_actual = mtd["date"].max() if not mtd.empty else (month_start - pd.Timedelta(days=1))

    base_X = X.copy()
    state_idx = base_X.index[model_df["date"]<=last_actual].max()
    state_vec = base_X.loc[state_idx].values
    col_index = {c:i for i,c in enumerate(base_X.columns)}
    const_r = float(model_df.loc[state_idx, "rbob"]) if "rbob" in model_df.columns else 0.0
    const_w = float(model_df.loc[state_idx, "wti"])  if "wti"  in model_df.columns else 0.0
    const_c = float(model_df.loc[state_idx, "crack"])if "crack" in model_df.columns else 0.0
    for l in range(max_lag+1):
        if f"rbob_l{l}" in col_index:  state_vec[col_index[f"rbob_l{l}"]]  = const_r
        if f"wti_l{l}"  in col_index:  state_vec[col_index[f"wti_l{l}"]]   = const_w
        if f"crack_l{l}" in col_index: state_vec[col_index[f"crack_l{l}"]] = const_c
    if "surprise_pulse" in col_index: state_vec[col_index["surprise_pulse"]] = 0.0
    if "refinery_pulse" in col_index: state_vec[col_index["refinery_pulse"]] = 0.0
    if "rbob_up" in col_index:   state_vec[col_index["rbob_up"]] = const_r
    if "rbob_down" in col_index: state_vec[col_index["rbob_down"]] = 0.0
    idx_lag1 = col_index.get("gas_lag1", None)

    resid = (ytr.values - ridge.predict(Xtr)) if len(ytr)>0 else np.array([0.0])

    # build historical daily deltas (use recent window to match regime)
    hist_drb  = model_df["rbob"].diff().dropna().values
    hist_dwt  = model_df["wti"].diff().dropna().values
    hist_dcrk = model_df["crack"].diff().dropna().values
    if hist_drb.size==0: hist_drb = np.array([0.0])
    if hist_dwt.size==0: hist_dwt = np.array([0.0])
    if hist_dcrk.size==0: hist_dcrk = np.array([0.0])

    rb, wt, ck = const_r, const_w, const_c   # start from last actual

    # >>> Weekly "higher than previous Monday" probability
    # Previous Monday (price is the AAA print for that Monday). We use gb/aaa proxy from model_df.
    # We take the last Monday (<= today) as ref_monday and simulate next Monday (target_monday).
    nyc_now = datetime.now(tz=ZoneInfo("America/New_York"))
    wd = nyc_now.weekday()
    this_monday = (nyc_now - timedelta(days=wd)).date()
    prev_monday = this_monday - timedelta(days=7)
    next_monday = this_monday + timedelta(days=7)

    # >>> IMPORTANT: simulate through next Monday even if it's next month
    true_end = max(month_end.date(), next_monday)   # <— ensure weekly chart can compute
    future_dates = pd.date_range(last_actual + pd.Timedelta(days=1), true_end, freq="D")
    H = len(future_dates)
    paths = np.zeros((sim_n, H), dtype=float) if H > 0 else np.zeros((sim_n, 0), dtype=float)
    
    # Reference Monday AAA value (AAA is "as of previous day" at ~3:30am ET)
    try:
        aaa = historical[historical["source"] == "aaa_gas_prices"].copy()
        print(f"Debug: Found {len(aaa)} AAA records in historical data")
        if not aaa.empty:
            print(f"Debug: AAA columns: {aaa.columns.tolist()}")
            print(f"Debug: First few AAA records:")
            print(aaa[["scraped_at", "price"]].head())
        
        aaa["scraped_at"] = pd.to_datetime(aaa["scraped_at"], errors="coerce", utc=True).dt.tz_convert("America/New_York")
        
        # Since as_of_date column doesn't exist yet, calculate it from scraped_at
        aaa["as_of_date"] = (aaa["scraped_at"] - pd.Timedelta(days=1)).dt.date
        
        print(f"Debug: Available AAA dates: {sorted(aaa['as_of_date'].unique())}")
        print(f"Debug: Looking for previous Monday: {prev_monday}")
        print(f"Debug: prev_monday type: {type(prev_monday)}")
        print(f"Debug: as_of_date types: {aaa['as_of_date'].apply(type).unique()}")
        
        # Ensure both are date objects for comparison
        if isinstance(prev_monday, datetime):
            prev_monday = prev_monday.date()
        
        ref_slice = aaa.loc[aaa["as_of_date"] == prev_monday, "price"]
        ref_val = float(ref_slice.mean()) if not ref_slice.empty else np.nan
        
        # Fallback: if no exact match, find the closest available date
        if pd.isna(ref_val):
            print(f"Debug: No exact match for {prev_monday}, looking for closest date")
            available_dates = aaa["as_of_date"].dropna().unique()
            if len(available_dates) > 0:
                # Find the closest date to prev_monday
                date_diffs = [abs((pd.Timestamp(d) - pd.Timestamp(prev_monday)).days) for d in available_dates]
                closest_idx = np.argmin(date_diffs)
                closest_date = available_dates[closest_idx]
                closest_slice = aaa.loc[aaa["as_of_date"] == closest_date, "price"]
                ref_val = float(closest_slice.mean()) if not closest_slice.empty else np.nan
                print(f"Debug: Using closest date {closest_date} (diff: {date_diffs[closest_idx]} days) with value {ref_val}")
        
        print(f"Debug: Previous Monday ({prev_monday}) AAA value: {ref_val}")
        print(f"Debug: Found {len(ref_slice)} AAA records for {prev_monday}")
    except Exception as e:
        print(f"Debug: Error calculating AAA value: {e}")
        ref_val = np.nan

    y0 = float(model_df.loc[model_df["date"]==last_actual, "gas_price"].iloc[0]) if (model_df["date"]==last_actual).any() else float(y.iloc[-1])

    for i in range(sim_n):
        vec = state_vec.copy()
        y_prev = y0
        rb_i, wt_i, ck_i = rb, wt, ck
        for h in range(H):
            # evolve drivers with bootstrapped daily changes
            rb_i += float(np.random.choice(hist_drb))
            wt_i += float(np.random.choice(hist_dwt))
            ck_i += float(np.random.choice(hist_dcrk))

            # push updated driver levels into all lag slots used by ridge
            for l in range(max_lag+1):
                if f"rbob_l{l}" in col_index:  vec[col_index[f"rbob_l{l}"]]  = rb_i
                if f"wti_l{l}"  in col_index:  vec[col_index[f"wti_l{l}"]]   = wt_i
                if f"crack_l{l}" in col_index: vec[col_index[f"crack_l{l}"]] = ck_i

            # keep pulses off for futures days
            if "surprise_pulse" in col_index: vec[col_index["surprise_pulse"]] = 0.0
            if "refinery_pulse" in col_index: vec[col_index["refinery_pulse"]] = 0.0

            # AR part
            if idx_lag1 is not None: vec[idx_lag1] = y_prev

            y_hat = float(ridge.predict(vec.reshape(1,-1))[0])
            eps   = float(np.random.choice(resid)) if resid.size else 0.0
            y_prev = y_hat + eps
            paths[i,h] = y_prev

    # >>> Per-day distribution (median & std) for the fan chart
    daily_median = np.median(paths, axis=0) if H>0 else np.array([])
    daily_std    = np.std(paths, axis=0, ddof=1) if H>0 else np.array([])

    # >>> End-of-month single-day (the 31st or last day) distribution: we model **that day's** price
    eom_vals = paths[:,-1] if H>0 else np.array([])
    eom_mean = float(np.mean(eom_vals)) if eom_vals.size else np.nan
    eom_std  = float(np.std(eom_vals, ddof=1)) if eom_vals.size else np.nan
    ci5, ci95 = (float(np.percentile(eom_vals,5)), float(np.percentile(eom_vals,95))) if eom_vals.size else (np.nan,np.nan)

    # Reference daily value for previous Monday from AAA-first series
    # Note: ref_val is already calculated above using the proper as_of_date method
    # No need to recalculate here

    # Simulate to next Monday index
    if H > 0 and next_monday in future_dates.date:
        idx = list(future_dates.date).index(next_monday)
        next_mon_vals = paths[:, idx]
        prob_higher = float((next_mon_vals > ref_val).mean() * 100.0) if pd.notna(ref_val) else np.nan
        next_mon_pred = float(np.median(next_mon_vals)) if pd.notna(ref_val) else np.nan
    else:
        prob_higher, next_mon_pred = np.nan, np.nan

    # >>> Threshold table around EOM forecast (+/- $0.05 increments centered at eom_mean)
    prob_table = make_threshold_table(eom_vals, eom_mean)

    return {
        "base_df": model_df,
        "future_dates": future_dates,
        "paths": paths,
        "daily_median": daily_median,
        "daily_std": daily_std,
        "mtd": mtd,
        "month_end": month_end,
        "eom_mean": eom_mean,
        "eom_std": eom_std,
        "ci5": ci5, "ci95": ci95,
        "metrics": metrics,
        "weekly_prob_higher": prob_higher,
        "weekly_next_mon_pred": next_mon_pred,
        "weekly_ref_prev_mon": ref_val,
        "prob_table": prob_table
    }

# ============== APP ==============
def main():
    st.markdown('<h1 class="main-header">Gas Price Dashboard</h1>', unsafe_allow_html=True)

    latest_data, previous_data, historical_data = load_data()
    if latest_data is None:
        st.error("Failed to load data.")
        return

    # --------- CONTROLS (omitted for brevity, identical to your original) ---------

    # --------- NEW LAYOUT: 3 charts in top row, 3 tables below, then 2 charts side by side ---------
    st.header("Historical Trends & Analysis")

    # -------- Top row: 3 charts --------
    c1, c2, c3 = st.columns(3)

    # GasBuddy MTD + top-right latest value
    with c1:
        st.subheader("GasBuddy (Month-to-Date)")
        gb = historical_data[historical_data['source']=='gasbuddy_fuel_insights'].copy()
        if not gb.empty:
            gb['scraped_at'] = to_est(gb['scraped_at'])
            today_dt = datetime.now()
            month_start = today_dt.replace(day=1).date()
            gb['date'] = gb['scraped_at'].dt.date
            gb_mtd = gb[(gb['date']>=month_start) & (gb['date']<=today_dt.date())].sort_values('scraped_at')
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=gb_mtd['scraped_at'], y=gb_mtd['price'],
                                     mode='lines+markers', name='GasBuddy',
                                     hovertemplate='Time (EST)=%{x}<br>Price=$%{y:.3f}<extra></extra>'))
            # Add latest value label in top-right
            if len(gb_mtd) > 0:
                last_val = gb_mtd['price'].iloc[-1]
                fig.add_annotation(x=1, y=1, xref="paper", yref="paper",
                                   xanchor="right", yanchor="top", showarrow=False,
                                   text=f"<b>${last_val:.3f}</b>")
            fig.update_layout(xaxis_title="Date & Time (EST)", yaxis_title="Price ($/gal)",
                              height=340, margin=dict(l=40,r=20,t=40,b=40),
                              hovermode='x unified',
                              legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No GasBuddy data available")

    # AAA MTD + top-right latest value
    with c2:
        st.subheader("AAA Daily Gas Prices")
        aaa = historical_data[historical_data['source']=='aaa_gas_prices'].copy()
        if not aaa.empty:
            aaa['scraped_at'] = to_est(aaa['scraped_at'])
            today_dt = datetime.now()
            month_start = today_dt.replace(day=1).date()
            # Since as_of_date column doesn't exist yet, use scraped_at
            aaa['date'] = (aaa['scraped_at'] - timedelta(days=1)).dt.date
            aaa_mtd = aaa[(aaa['date']>=month_start) & (aaa['date']<=today_dt.date())].sort_values('scraped_at')
            fig_aaa = go.Figure()
            fig_aaa.add_trace(go.Scatter(x=aaa_mtd['scraped_at'], y=aaa_mtd['price'],
                                         mode='lines+markers', name='AAA',
                                         hovertemplate='Date (EST)=%{x}<br>AAA=$%{y:.3f}<extra></extra>'))
            # Add latest value label in top-right
            if len(aaa_mtd) > 0:
                last_val = aaa_mtd['price'].iloc[-1]
                fig_aaa.add_annotation(x=1, y=1, xref="paper", yref="paper",
                                       xanchor="right", yanchor="top", showarrow=False,
                                       text=f"<b>${last_val:.3f}</b>")
            fig_aaa.update_layout(xaxis_title="Date (EST)", yaxis_title="Price ($/gal)", height=340,
                                   margin=dict(l=40,r=20,t=40,b=40), hovermode='x unified',
                                   legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1))
            st.plotly_chart(fig_aaa, use_container_width=True)
        else:
            st.info("No AAA data available")

    # Crack Spread + top-right latest value
    with c3:
        st.subheader("Crack Spread (Last 30 Days)")
        rbob = historical_data[historical_data['source']=='marketwatch_rbob_futures'].copy()
        wti  = historical_data[historical_data['source']=='marketwatch_wti_futures'].copy()
        if not rbob.empty and not wti.empty:
            rbob['scraped_at'] = to_est(rbob['scraped_at'])
            wti['scraped_at']  = to_est(wti['scraped_at'])
            rbob_daily = (rbob.set_index('scraped_at').resample('D')['price'].mean().reset_index())
            wti_daily  = (wti.set_index('scraped_at').resample('D')['price'].mean().reset_index())
            rbob_daily['date'] = rbob_daily['scraped_at'].dt.date
            wti_daily['date']  = wti_daily['scraped_at'].dt.date
            futures_df = rbob_daily[['date','price']].merge(
                           wti_daily[['date','price']].rename(columns={'price':'WTI'}),
                           on='date', how='outer').rename(columns={'price':'RBOB'}).sort_values('date')
            crack_df = futures_df.dropna()
            crack_df['Crack_Spread'] = (crack_df['RBOB'] * 42.0) - crack_df['WTI']
            fig_cs = go.Figure()
            fig_cs.add_trace(go.Scatter(x=crack_df['date'], y=crack_df['Crack_Spread'],
                                        mode='lines+markers', name='Crack Spread ($)'))
            fig_cs.add_hline(y=0, line_dash="dash", line_color="gray",
                             annotation_text="Break-even", annotation_position="bottom right")
            # Add latest value label in top-right
            if len(crack_df) > 0:
                last_val = crack_df['Crack_Spread'].iloc[-1]
                fig_cs.add_annotation(x=1, y=1, xref="paper", yref="paper",
                                      xanchor="right", yanchor="top", showarrow=False,
                                      text=f"<b>${last_val:.2f}</b>")
            # >>> auto-fit Y axis
            fig_cs.update_yaxes(autorange=True)
            fig_cs.update_layout(xaxis_title="Date (EST)", yaxis_title="Crack Spread ($)", height=340,
                                 margin=dict(l=40,r=20,t=40,b=40), hovermode='x unified',
                                 legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1))
            st.plotly_chart(fig_cs, use_container_width=True)
        else:
            st.info("No futures data available")

    # -------- Second row: 3 tables under the charts --------
    t1, t2, t3 = st.columns(3)
    with t1:
        averages_table(historical_data[historical_data['source']=='gasbuddy_fuel_insights'], "Average Price")
    with t2:
        averages_table(historical_data[historical_data['source']=='aaa_gas_prices'], "Average Price")
    with t3:
        try:
            if 'crack_df' in locals() and not crack_df.empty:
                tmp = crack_df.copy()
                tmp['scraped_at'] = pd.to_datetime(tmp['date']).dt.tz_localize('America/New_York')
                tmp = tmp.rename(columns={'Crack_Spread':'price'})
                averages_table(tmp[['scraped_at','price']], "Average Value")
            else:
                st.dataframe(pd.DataFrame({'Period':['Daily (EST)','Month-to-Date','Week-to-Date'],
                                           'Average Value':['No data','No data','No data']}),
                             use_container_width=True, hide_index=True)
        except Exception:
            st.dataframe(pd.DataFrame({'Period':['Daily (EST)','Month-to-Date','Week-to-Date'],
                                       'Average Value':['No data','No data','No data']}),
                         use_container_width=True, hide_index=True)

    # -------- NEW: RBOB / WTI last 7 days + summary table --------
    st.subheader("Futures (Last 7 Days)")
    f1, f2 = st.columns(2)
    cutoff = datetime.now(tz=ZoneInfo("America/New_York")) - timedelta(days=7)

    with f1:
        rb = historical_data[historical_data['source']=='marketwatch_rbob_futures'].copy()
        if not rb.empty:
            rb['scraped_at'] = to_est(rb['scraped_at'])
            rb7 = rb[rb['scraped_at']>=cutoff]
            fig_rb = go.Figure()
            fig_rb.add_trace(go.Scatter(x=rb7['scraped_at'], y=rb7['price'],
                                        mode='lines+markers', name='RBOB'))
            fig_rb.update_layout(title="RBOB Futures – 7D", xaxis_title="Time (EST)", yaxis_title="Price",
                                 height=320, margin=dict(l=40,r=20,t=40,b=40), hovermode='x unified')
            st.plotly_chart(fig_rb, use_container_width=True)
        else:
            st.info("No RBOB data")

    with f2:
        wt = historical_data[historical_data['source']=='marketwatch_wti_futures'].copy()
        if not wt.empty:
            wt['scraped_at'] = to_est(wt['scraped_at'])
            wt7 = wt[wt['scraped_at']>=cutoff]
            fig_wt = go.Figure()
            fig_wt.add_trace(go.Scatter(x=wt7['scraped_at'], y=wt7['price'],
                                        mode='lines+markers', name='WTI'))
            fig_wt.update_layout(title="WTI Futures – 7D", xaxis_title="Time (EST)", yaxis_title="Price",
                                 height=320, margin=dict(l=40,r=20,t=40,b=40), hovermode='x unified')
            st.plotly_chart(fig_wt, use_container_width=True)
        else:
            st.info("No WTI data")

    # Summary table below the two futures charts
    def _latest_for(source, value_field="price"):
        df = latest_data[latest_data["source"]==source]
        if df.empty:
            df = historical_data[historical_data["source"]==source]
        if df.empty:
            return np.nan, pd.NaT
        row = df.sort_values("scraped_at").iloc[-1]
        val = row[value_field] if (value_field in row and pd.notna(row[value_field])) else row.get("price", np.nan)
        ts  = row["scraped_at"]
        try:
            ts_est = pd.to_datetime(ts, utc=True).tz_convert("America/New_York")
        except Exception:
            ts_est = pd.to_datetime(ts).tz_localize("UTC").tz_convert("America/New_York")
        return val, ts_est

    rows = []
    for desc, src, fld, is_money in [
        ("GasBuddy", "gasbuddy_fuel_insights", "price", True),
        ("AAA", "aaa_gas_prices", "price", True),
        ("RBOB Future", "marketwatch_rbob_futures", "price", True),
        ("WTI", "marketwatch_wti_futures", "price", True),
        ("Gasoline Stocks Change Surprise", "tradingeconomics_gasoline_stocks", "surprise", False),
        ("Refinery Runs", "tradingeconomics_refinery_runs", "price", False),
    ]:
        val, ts = _latest_for(src, fld)
        if is_money and pd.notna(val):
            lv = f"${float(val):.3f}"
        elif pd.notna(val):
            lv = f"{float(val):.3f}"
        else:
            lv = "—"
        ts_str = ts.strftime("%Y-%m-%d %H:%M %Z") if pd.notna(ts) else "—"
        rows.append({"Description": desc, "Last Value": lv, "Timestamp": ts_str})
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # >>> ADD: Simple Beta 7-Day view (actuals vs forecast)
    st.markdown("### Simple Beta Model (RBOB & WTI as drivers)")
    st.caption("Uses AAA 'as-of-yesterday' for actuals (falls back to GasBuddy), "
               "and a levels-with-lag OLS on RB*42 and WTI with k=7 days.")

    _render_simple_beta_block(historical_data, k=7, lookback_days=120)

    # ============== MODELING ==============
    st.markdown("---")
    st.header("Gas Price Forecasting & Modeling")

    required_sources = ['gasbuddy_fuel_insights','marketwatch_rbob_futures','marketwatch_wti_futures']
    if any(src not in set(historical_data['source']) for src in required_sources) or len(historical_data)<100:
        st.warning("Need GasBuddy, RBOB, and WTI with sufficient history.")
        return

    res = simulate_forecast(historical_data, sim_n=2000, max_lag=14)
    if "error" in res:
        st.error(res["error"])
        st.write("Debug: Historical data sources:", historical_data['source'].unique())
        st.write("Debug: Historical data length:", len(historical_data))
        return

    # >>> NEW KPI CARDS (no debug banners)
    k1,k2,k3,k4 = st.columns(4)
    with k1: st.metric("Records in database", value=f"{len(historical_data):,}")
    with k2:
        gb_last = latest_data[latest_data["source"]=="gasbuddy_fuel_insights"]
        if gb_last.empty:
            gb_last = historical_data[historical_data["source"]=="gasbuddy_fuel_insights"]
        if gb_last.empty:
            val = np.nan
        else:
            val = float(gb_last.sort_values("scraped_at").iloc[-1]["price"])
        st.metric("Current daily retail (GasBuddy, latest scrape)", value=("—" if pd.isna(val) else f"${val:.3f}"))
    with k3:
        p = res["weekly_prob_higher"]
        pred_next_mon = res["weekly_next_mon_pred"]
        ref_prev_mon  = res["weekly_ref_prev_mon"]
        subtitle = f"vs prev Monday {('—' if pd.isna(ref_prev_mon) else f'${ref_prev_mon:.3f}')}"
        st.metric("Prob ↑ next Monday (AAA)", value=("—" if pd.isna(p) else f"{p:.1f}%"), delta=subtitle)
        if not pd.isna(pred_next_mon):
            st.caption(f"Predicted next Monday price: **${pred_next_mon:.3f}**")
    with k4:
        st.metric("EOM daily price (mean)", value=("—" if pd.isna(res['eom_mean']) else f"${res['eom_mean']:.3f}"))
        if not pd.isna(res['eom_std']):
            st.caption(f"Std dev: **${res['eom_std']:.3f}**, 90% CI [{res['ci5']:.3f}, {res['ci95']:.3f}]")

    # >>> 1) Fan chart = Actuals (MTD) + per-day median & σ bands for remaining days
    g1, = st.columns([1])

    with g1:
        if len(res["future_dates"]) and res["paths"].shape[1]:
            p5  = np.percentile(res["paths"], 5, axis=0)
            p25 = np.percentile(res["paths"],25, axis=0)
            p50 = res["daily_median"]
            p75 = np.percentile(res["paths"],75, axis=0)
            p95 = np.percentile(res["paths"],95, axis=0)

            fig_fan = go.Figure()
            if not res["mtd"].empty:
                fig_fan.add_trace(go.Scatter(x=res["mtd"]["date"], y=res["mtd"]["gas_price"],
                                             mode="lines", name="Actual (MTD)"))
            fig_fan.add_trace(go.Scatter(x=res["future_dates"], y=p50, mode="lines", name="Median Forecast"))
            fig_fan.add_trace(go.Scatter(x=np.r_[res["future_dates"], res["future_dates"][::-1]],
                                          y=np.r_[p75, p25[::-1]], fill="toself",
                                          name="50% band", opacity=0.3, line=dict(width=0)))
            fig_fan.add_trace(go.Scatter(x=np.r_[res["future_dates"], res["future_dates"][::-1]],
                                          y=np.r_[p95, p5[::-1]], fill="toself",
                                          name="90% band", opacity=0.2, line=dict(width=0)))
            fig_fan.update_layout(title=f"Month-to-Date Fan Chart → {res['month_end'].date()}",
                                  xaxis_title="Date (EST)", yaxis_title="Price ($/gal)",
                                  height=400, margin=dict(l=40,r=20,t=40,b=40),
                                  hovermode='x unified',
                                  legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1))
            st.plotly_chart(fig_fan, use_container_width=True)
        else:
            st.info("No remaining days to forecast.")




    
    # Footer
    st.markdown("---")
    st.markdown("Dashboard refresh ~5 min • Sources: GasBuddy, AAA, MarketWatch, Trading Economics")

if __name__ == "__main__":
    main()
