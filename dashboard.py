import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import numpy as np
from zoneinfo import ZoneInfo
from typing import Tuple

# === Tunables (model behavior) ===
LOOKBACK_DAYS   = 120
HALF_LIFE_DAYS  = 90
RIDGE_L2        = 5e-4   # slightly less shrinkage
AAA_EWM_SPAN    = 2      # a bit more responsive
SLOPE_CLIP_CPD  = 0.03   # allow ~3¢/day before tanh clips
SLOPE_DECAY_FWD = 0.70   # fade slower to preserve near-term vol
MIN_FIT_ROWS    = 24     # lower hard minimum
from typing import Tuple

# ============== PAGE & CSS ==============
st.set_page_config(page_title="Gas Price Dashboard", page_icon="⛽", layout="wide", initial_sidebar_state="expanded")

css_style = """
.main-header { font-size: 2.5rem; color: #1f77b4; text-align: center; margin-bottom: 2rem; text-shadow: 2px 2px 4px rgba(0,0,0,0.1); }
.metric-card { background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%); padding: 1.2rem; border-radius: 0.8rem; border-left: 4px solid #1f77b4; height: 220px; display: flex; flex-direction: column; justify-content: space-between; box-shadow: 0 4px 6px rgba(0,0,0,0.1); transition: transform 0.2s ease; }
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
.data-table { background: transparent; box-shadow: none; padding: 0; }
.right-table-wrap { display: flex; height: 450px; align-items: center; justify-content: center; }
.right-table-wrap .stDataFrame { width: 100%; max-width: 520px; }
.data-table h4 { display:none; }
.info-strip { background: #f0f7ff; border-left: 4px solid #1f77b4; padding: 12px 14px; border-radius: 8px; margin: 12px 0 4px 0; display: grid; grid-template-columns: 1fr 1fr; gap: 12px; font-size: 0.95rem; }
.info-strip .label { color:#2c3e50; font-weight:600; }
.info-strip .light { color:#57606a; }
.info-strip .muted { color:#6c757d; font-size:0.9rem; }
"""

st.markdown(f"<style>{css_style}</style>", unsafe_allow_html=True)

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
        
        if 'as_of_date' not in historical_data.columns:
            historical_data['as_of_date'] = None
        conn.close()
        return latest_data, previous_data, historical_data
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None, None

def to_est(s):
    out = pd.to_datetime(s, errors='coerce', utc=True)
    return out.dt.tz_convert('America/New_York')

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
    wd = today_est.weekday()
    monday_noon_est = (today_est - timedelta(days=wd)).replace(hour=12, minute=0, second=0, microsecond=0)
    if today_est < monday_noon_est:
        monday_noon_est -= timedelta(days=7)
    wtd = data.loc[data['scraped_at']>=monday_noon_est, 'price'].mean()
    fmt = lambda x, n=3: (f"${x:.{n}f}" if pd.notna(x) else "No data")
    df = pd.DataFrame({'Period':['Daily (EST)','Month-to-Date','Week-to-Date'],
                       f'{label}':[fmt(daily), fmt(mtd), fmt(wtd)]})
    st.dataframe(df, use_container_width=True, hide_index=True)

# ============== NEW: Robust smoothing for futures (model inputs only) ==============
def _hampel_ewma(series: pd.Series, window: int = 7, nsigmas: float = 3.0, ewma_span: int = 3) -> pd.Series:
    """
    Robust outlier repair (MAD/Hampel-style) + gentle EWMA smoothing.
    - Replaces points |x - median| > nsigmas * 1.4826 * MAD with rolling median.
    - Then applies EWMA (span=ewma_span) to the repaired series.
    """
    s = pd.to_numeric(series, errors="coerce").astype(float)
    if s.dropna().empty:
        return s

    med = s.rolling(window=window, center=True, min_periods=max(3, window//2)).median()
    mad = (s - med).abs().rolling(window=window, center=True, min_periods=max(3, window//2)).median()
    # consistent with σ under Gaussian: 1.4826 * MAD
    thresh = nsigmas * 1.4826 * mad
    out = s.copy()
    mask = (s - med).abs() > thresh
    out[mask] = med[mask]

    # EWMA smoothing (keeps responsiveness)
    out = out.ewm(span=ewma_span, adjust=False, min_periods=1).mean()
    return out

def _hampel(x: pd.Series, k: int = 3, n_sig: float = 3.0) -> pd.Series:
    # robust outlier replacement (median ± n_sig*MAD) over a 2k+1 window
    x = pd.Series(x, dtype='float64').copy()
    med = x.rolling(2*k+1, center=True, min_periods=1).median()
    mad = (x - med).abs().rolling(2*k+1, center=True, min_periods=1).median()
    thresh = n_sig * 1.4826 * mad
    out = x.copy()
    mask = (x - med).abs() > thresh
    out[mask] = med[mask]
    return out

def _robust_smooth(series: pd.Series, k: int = 3, n_sig: float = 3.0, ewma_span: int = 5) -> pd.Series:
    return _hampel(series, k=k, n_sig=n_sig).ewm(span=ewma_span, adjust=False).mean()

# ============== SIMPLE DAILY PANEL (AAA first → GB fallback) ==============
def _daily_panel(h: pd.DataFrame) -> pd.DataFrame:
    df = h.copy()
    # Parse both scraped_at and timestamp
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce", utc=True).dt.tz_convert("America/New_York")
    df["timestamp"]  = pd.to_datetime(df.get("timestamp"), errors="coerce", utc=True)
    df["event_date"] = df["timestamp"].dt.tz_convert("America/New_York").dt.normalize()  # used for EIA series
    df = df.dropna(subset=["scraped_at"])
    df["date"] = pd.to_datetime(df["scraped_at"].dt.date)  # calendar day from scrape (used by AAA/GB/RBOB/WTI)

    # GasBuddy daily mean (same-day)
    gb = (df[df["source"]=="gasbuddy_fuel_insights"][["date","price"]]
            .groupby("date", as_index=False)["price"].mean()
            .rename(columns={"price":"gb"}))

    # AAA daily mean (as-of previous day)
    aaa = df[df["source"]=="aaa_gas_prices"][["scraped_at","price"]].copy()
    if not aaa.empty:
        aaa["date"] = (aaa["scraped_at"] - pd.Timedelta(days=1)).dt.normalize()
        aaa = (aaa.groupby("date", as_index=False)["price"].mean()
                 .rename(columns={"price":"aaa"}))
    else:
        aaa = pd.DataFrame(columns=["date","aaa"])

    # RBOB / WTI daily (use scrape-day)
    rbob = (df[df["source"]=="marketwatch_rbob_futures"][["date","price"]]
              .groupby("date", as_index=False)["price"].mean()
              .rename(columns={"price":"rbob"}))
    wti  = (df[df["source"]=="marketwatch_wti_futures"][["date","price"]]
              .groupby("date", as_index=False)["price"].mean()
              .rename(columns={"price":"wti"}))

    # EIA/TradingEconomics weekly series — anchor to event_date (fallback to scrape-day if missing)
    te_stocks = df[df["source"]=="tradingeconomics_gasoline_stocks"][["event_date","date","surprise"]].copy()
    if not te_stocks.empty:
        te_stocks["date"] = te_stocks["event_date"].fillna(te_stocks["date"])
        te_stocks = (te_stocks.dropna(subset=["date","surprise"])
                                .drop_duplicates("date")
                                .rename(columns={"surprise":"stocks_surprise"}))[["date","stocks_surprise"]]
    else:
        te_stocks = pd.DataFrame(columns=["date","stocks_surprise"])

    te_runs = df[df["source"]=="tradingeconomics_refinery_runs"][["event_date","date","price"]].copy()
    if not te_runs.empty:
        te_runs["date"] = te_runs["event_date"].fillna(te_runs["date"])
        te_runs = (te_runs.dropna(subset=["date","price"])
                            .drop_duplicates("date")
                            .rename(columns={"price":"refinery_runs"}))[["date","refinery_runs"]]
    else:
        te_runs = pd.DataFrame(columns=["date","refinery_runs"])

    frames = [g for g in [gb, aaa, rbob, wti] if not g.empty]
    if not frames:
        return pd.DataFrame(columns=["date","gas_price","rbob","wti","stocks_surprise","refinery_runs"])
    
    for frame in frames + [te_stocks, te_runs]:
        if not frame.empty and "date" in frame:
            frame.loc[:, "date"] = frame["date"].dt.tz_localize(None)
    
    start = min(x["date"].min() for x in frames)
    end   = max(x["date"].max() for x in frames)
    base = pd.DataFrame({"date": pd.date_range(start, end, freq="D")})

    for d in (aaa, gb, rbob, wti, te_stocks, te_runs):
        base = base.merge(d, on="date", how="left")

    # AAA first, else GasBuddy, else backfill
    base["gas_price"] = base["aaa"].combine_first(base["gb"]).ffill().bfill()

    # Safe GB gap (fill missing with 0)
    base["GB_gap"] = (base.get("gb") - base["aaa"]).fillna(0.0)

    # Fill and SMOOTH futures (model inputs only)
    for c in ["rbob","wti"]:
        if c in base.columns:
            base[c] = base[c].ffill().bfill()
            sm = _hampel_ewma(base[c], window=7, nsigmas=3.0, ewma_span=3)
            base[c + "_sm"] = sm.ffill().bfill()

    # Forward-fill EIA proxies
    for c in ["stocks_surprise","refinery_runs"]:
        if c in base.columns:
            base[c] = base[c].ffill()

    return base

def _add_pulses(base: pd.DataFrame, decay=[1.0, 0.6, 0.36, 0.216]) -> pd.DataFrame:
    """Build short decaying pulses from weekly EIA series."""
    df = base.sort_values("date").copy().set_index("date")
    s = df.get("stocks_surprise", pd.Series(0.0, index=df.index)).fillna(0.0)
    r = df.get("refinery_runs",  pd.Series(0.0, index=df.index)).fillna(0.0)

    def pulse(x):
        out = pd.Series(0.0, index=x.index)
        for i, w in enumerate(decay):
            out = out.add(x.shift(i, fill_value=0.0) * w, fill_value=0.0)
        return out

    df["stocks_pulse"]   = pulse(s)
    df["refinery_pulse"] = pulse(r)
    return df.reset_index()

def _forecast_future_pulses(base_with_pulses: pd.DataFrame, future_dates: pd.DatetimeIndex, decay=[1.0,0.6,0.36,0.216]) -> pd.DataFrame:
    """Project pulses over the next few days assuming no new EIA prints (they decay to zero)."""
    L = len(decay)
    df = base_with_pulses.sort_values("date").copy()
    s = df.get("stocks_surprise", pd.Series(0.0, index=df.index)).fillna(0.0).values
    r = df.get("refinery_runs",  pd.Series(0.0, index=df.index)).fillna(0.0).values
    s_tail = s[-L:] if len(s) >= L else np.r_[np.zeros(L-len(s)), s]
    r_tail = r[-L:] if len(r) >= L else np.r_[np.zeros(L-len(r)), r]
    w = np.array(decay, dtype=float)

    stocks_future, runs_future = [], []
    for h in range(1, len(future_dates)+1):
        idxs = np.arange(h, L)
        if len(idxs) == 0:
            stocks_future.append(0.0); runs_future.append(0.0); continue
        stocks_future.append( float((w[idxs] * s_tail[-1 - (idxs - h)]).sum()) )
        runs_future.append(   float((w[idxs] * r_tail[-1 - (idxs - h)]).sum()) )

    return pd.DataFrame({
        "date": future_dates,
        "stocks_pulse": np.array(stocks_future),
        "refinery_pulse": np.array(runs_future)
    })


def fit_aaa_nowcast_and_week(
    h: pd.DataFrame,
    k_lag:int=7,
    lookback_days:int=180,
    half_life_days:int=90,
    gate_tol:float=0.03,
    sim_n:int=5000,
    min_rows:int=24,     # use the lower hard minimum you set up top
):
    base = _daily_panel(h).sort_values("date").copy()
    if base.empty:
        return {"error":"No history"}

    # true AAA for charting; Y is training target (AAA -> GB fallback)
    base["AAA"] = base.get("aaa")
    base["Y"]   = base["AAA"].combine_first(base.get("gas_price"))

    # smoothed futures (model inputs only)
    base["rb_sm"]  = _robust_smooth(base["rbob"])
    base["wti_sm"] = _robust_smooth(base["wti"])

    # slope source is AAA->GB so gaps don't kill slope
    slope_src        = base["AAA"].combine_first(base["gas_price"])
    base["AAA_l1"]   = base["AAA"].fillna(slope_src).shift(1)
    base["AAA_ewm"]  = slope_src.ewm(span=AAA_EWM_SPAN, adjust=False).mean()
    raw_slope        = (base["AAA_ewm"] - base["AAA_ewm"].shift(7)) / 7.0
    base["AAA_slope7"]= np.tanh(raw_slope / SLOPE_CLIP_CPD) * SLOPE_CLIP_CPD

    # 7-day lag futures & crack
    base["rb42_l7"]  = (base["rb_sm"]*42.0).shift(k_lag)
    base["wti_l7"]   = base["wti_sm"].shift(k_lag)
    base["crack_l7"] = (base["rb_sm"]*42.0 - base["wti_sm"]).shift(k_lag)
    base["d_crack7"] = base["crack_l7"] - base["crack_l7"].shift(7)

    # GB gap (safe)
    base["GB_gap"] = (base.get("gb", pd.Series(index=base.index)) - base["AAA"]).fillna(0.0)

    # weekly pulses
    base = _add_pulses(base)

    # ------- build training window
    cutoff = base["date"].max() - pd.Timedelta(days=lookback_days)
    # IMPORTANT: drop on Y (not AAA) so we keep AAA-missing days
    fit = base[(base["date"]>=cutoff)].dropna(
        subset=["Y","AAA_l1","AAA_slope7","rb42_l7","wti_l7"]
    ).copy()

    # if still thin, allow missing crack features (set to 0)
    if "crack_l7" in fit and fit["crack_l7"].isna().any():
        fit["crack_l7"] = fit["crack_l7"].fillna(0.0)
    if "d_crack7" in fit and fit["d_crack7"].isna().any():
        fit["d_crack7"] = fit["d_crack7"].fillna(0.0)

    # If not enough rows, skip to fallback later (don't return an error)
    has_enough = len(fit) >= min_rows

    # -------------- MODEL BRANCH --------------
    if has_enough:
        ages = (fit["date"].max() - fit["date"]).dt.days.astype(float)
        lam  = np.log(2.0) / max(1, half_life_days)
        w    = np.exp(-lam * ages.values)

        def _wls_ridge(X, y, w, l2=RIDGE_L2):
            X = np.asarray(X, float); y = np.asarray(y, float).reshape(-1)
            W = np.sqrt(np.asarray(w, float).reshape(-1))
            Xw = X * W[:, None]; yw = y * W
            I  = np.eye(Xw.shape[1]); I[0,0] = 0.0
            return np.linalg.solve(Xw.T @ Xw + l2*I, Xw.T @ yw)

        Xb = np.column_stack([
            np.ones(len(fit), float),
            fit["AAA_l1"], fit["AAA_slope7"],
            fit["rb42_l7"], fit["wti_l7"],
            fit["crack_l7"], fit["d_crack7"],
            fit["GB_gap"],
        ]).astype(float)
        yb = fit["Y"].values

        X_aug = np.column_stack([
            Xb,
            fit.get("stocks_pulse",   pd.Series(np.zeros(len(fit)))).values.astype(float),
            fit.get("refinery_pulse", pd.Series(np.zeros(len(fit)))).values.astype(float),
        ])

        b_base = np.asarray(_wls_ridge(Xb,   yb, w)).reshape(-1)
        b_aug  = np.asarray(_wls_ridge(X_aug,yb, w)).reshape(-1)

        cut      = int(len(fit)*0.8)
        mae_base = float(np.mean(np.abs(yb[cut:] - (Xb[cut:]   @ b_base))))
        mae_aug  = float(np.mean(np.abs(yb[cut:] - (X_aug[cut:] @ b_aug))))
        use_pulses = (mae_aug <= mae_base*(1.0 - gate_tol))
        betas      = b_aug if use_pulses else b_base

        yhat_in = (X_aug @ betas) if use_pulses else (Xb @ betas)
        resid   = (yb - yhat_in[:len(yb)]).astype(float)
        if resid.size == 0:
            resid = base["Y"].diff().dropna().tail(60).values
            if resid.size == 0: resid = np.array([0.0], float)

        # forecast horizon: start day after last true AAA → ensures a 9/12 point
        last_aaa_date = base.loc[base["AAA"].notna(), "date"].max()
        if pd.isna(last_aaa_date): last_aaa_date = base["date"].max()
        start = last_aaa_date + pd.Timedelta(days=1)
        eom   = pd.Timestamp(year=start.year, month=start.month,
                             day=pd.Period(start,'M').days_in_month)
        horizon_days = max(7, (eom - start).days + 1)
        fdates = pd.date_range(start, periods=horizon_days, freq="D")

        # seed slope with last observed (fade more slowly for higher vol)
        seed = base["AAA"].dropna() if base["AAA"].notna().any() else base["Y"].dropna()
        slope0 = (seed.iloc[-1] - seed.iloc[-7]) / 7.0 if len(seed) >= 7 else 0.0

        out = []
        gb_gap_once = float(base.loc[base["date"]==last_aaa_date, "GB_gap"].fillna(0.0).tail(1).iloc[0]) \
                      if "GB_gap" in base.columns and len(base) else 0.0

        for h, d in enumerate(fdates, start=1):
            ref = d - pd.Timedelta(days=k_lag)
            rb_ref  = base.loc[base["date"]==ref,"rb_sm"]
            wti_ref = base.loc[base["date"]==ref,"wti_sm"]
            rb_ref  = float(rb_ref.iloc[0])  if len(rb_ref)  else float(base["rb_sm"].dropna().iloc[-1])
            wti_ref = float(wti_ref.iloc[0]) if len(wti_ref) else float(base["wti_sm"].dropna().iloc[-1])

            row = [
                1.0,
                float(seed.iloc[-1] if len(seed) else base["Y"].dropna().iloc[-1]),
                float(slope0 * (SLOPE_DECAY_FWD ** (h-1))),
                rb_ref*42.0, wti_ref,
                float((base["rb_sm"].iloc[-1]*42.0 - base["wti_sm"].iloc[-1]) if len(base) else 0.0),
                0.0,   # d_crack7 for the step → 0
                gb_gap_once if h==1 else 0.0,
            ]
            if use_pulses:
                row += [
                    float(base.get("stocks_pulse",   pd.Series([0.0])).iloc[-1]),
                    float(base.get("refinery_pulse", pd.Series([0.0])).iloc[-1]),
                ]
            yhat = float(np.dot(betas, np.asarray(row, float)))
            out.append({"date": d, "AAA": yhat})
            seed = pd.concat([seed, pd.Series([yhat])], ignore_index=True)

        preds_series = pd.Series({r["date"]: r["AAA"] for r in out})

    # -------------- FALLBACK BRANCH --------------
    else:
        # slope-decay extrapolation using latest Y (ensures a value for 9/12)
        resid = base["Y"].diff().dropna().tail(60).values
        if resid.size == 0: resid = np.array([0.0], float)

        last_aaa_date = base.loc[base["AAA"].notna(), "date"].max()
        if pd.isna(last_aaa_date): last_aaa_date = base["date"].max()
        start = last_aaa_date + pd.Timedelta(days=1)
        eom   = pd.Timestamp(year=start.year, month=start.month,
                             day=pd.Period(start,'M').days_in_month)
        horizon_days = max(7, (eom - start).days + 1)
        fdates = pd.date_range(start, periods=horizon_days, freq="D")

        seed = base["AAA"].dropna()
        if seed.empty: seed = base["Y"].dropna()
        last = float(seed.iloc[-1]) if len(seed) else 0.0
        slope0 = (seed.iloc[-1] - seed.iloc[-7]) / 7.0 if len(seed) >= 7 else 0.0

        preds_series = pd.Series([last + slope0*(SLOPE_DECAY_FWD**(h-1))
                                  for h in range(1, len(fdates)+1)], index=fdates)
        mae_base = mae_aug = np.nan

    # common outputs
    next_7_dates = fdates[:7]
    preds_7      = preds_series.reindex(next_7_dates).values.astype(float)

    def _next_sun(d):
        while d.weekday()!=6: d += pd.Timedelta(days=1)
        return d
    next_sunday = _next_sun(base["date"].max()+pd.Timedelta(days=1))
    next_sunday_point = float(preds_series.loc[next_sunday]) if next_sunday in preds_series.index else float(preds_7[-1])

    eom_point   = float(preds_series.loc[eom]) if eom in preds_series.index else np.nan
    eom_samples = (eom_point + np.random.choice(resid, size=sim_n, replace=True)) if np.isfinite(eom_point) else np.array([])

    return {
        "base": base,
        "future_dates": next_7_dates,
        "preds": preds_7,
        "resid": resid,
        "next_sunday": next_sunday,
        "next_sunday_point": next_sunday_point,
        "used_pulses": bool(has_enough),
        "val_mae_base": (mae_base if has_enough else np.nan),
        "val_mae_aug":  (mae_aug  if has_enough else np.nan),
        "eom_point": eom_point,
        "eom_samples": eom_samples,
    }

def _prev_aaa_sunday_default(h: pd.DataFrame) -> Tuple[float, datetime]:
    aaa = h[h["source"]=="aaa_gas_prices"].copy()
    if aaa.empty:
        return np.nan, None
    aaa["scraped_at"] = to_est(aaa["scraped_at"]) 
    aaa["as_of_date"] = (aaa["scraped_at"] - pd.Timedelta(days=1)).dt.date
    sundays = aaa[aaa["as_of_date"].apply(lambda d: pd.Timestamp(d).weekday()==6)]
    if sundays.empty:
        return np.nan, None
    last_sun = max(sundays["as_of_date"])
    val = float(sundays.loc[sundays["as_of_date"]==last_sun,"price"].mean())
    return val, pd.Timestamp(last_sun)

def _next_sunday(date_from: pd.Timestamp) -> pd.Timestamp:
    d = date_from + pd.Timedelta(days=1)
    while d.weekday() != 6:  # 6=Sunday
        d += pd.Timedelta(days=1)
    return d

def round_to_05(x):
    return np.round(x*20)/20.0

def _make_threshold_table(eom_samples: np.ndarray, center: float) -> pd.DataFrame:
    if eom_samples.size == 0 or not np.isfinite(center):
        return pd.DataFrame(columns=["Threshold ($)", "Probability (%)"])
    c = np.round(center*20)/20.0
    thresholds = np.array([c-0.10, c-0.05, c, c+0.05, c+0.10, c+0.15])
    rows = [{"Threshold ($)": f"${t:.2f}",
             "Probability (%)": f"{(eom_samples > t).mean()*100:.1f}%"} for t in thresholds]
    return pd.DataFrame(rows)


# ============== APP ==============

def _prev_aaa_sunday_default(h: pd.DataFrame) -> Tuple[float, datetime]:
    aaa = h[h["source"]=="aaa_gas_prices"].copy()
    if aaa.empty:
        return np.nan, None
    aaa["scraped_at"] = to_est(aaa["scraped_at"]) 
    aaa["as_of_date"] = (aaa["scraped_at"] - pd.Timedelta(days=1)).dt.date
    sundays = aaa[aaa["as_of_date"].apply(lambda d: pd.Timestamp(d).weekday()==6)]
    if sundays.empty:
        return np.nan, None
    last_sun = max(sundays["as_of_date"])
    val = float(sundays.loc[sundays["as_of_date"]==last_sun,"price"].mean())
    return val, pd.Timestamp(last_sun)

def _next_sunday(date_from: pd.Timestamp) -> pd.Timestamp:
    d = date_from + pd.Timedelta(days=1)
    while d.weekday() != 6:  # 6=Sunday
        d += pd.Timedelta(days=1)
    return d

def round_to_05(x):
    return np.round(x*20)/20.0  # steps of 0.05 → ends with .x0 or .x5

def _make_threshold_table(eom_samples: np.ndarray, center: float) -> pd.DataFrame:
    if eom_samples.size == 0 or not np.isfinite(center):
        return pd.DataFrame(columns=["Threshold ($)", "Probability (%)"])
    c = np.round(center*20)/20.0
    thresholds = np.array([c-0.10, c-0.05, c, c+0.05, c+0.10, c+0.15])
    rows = [{"Threshold ($)": f"${t:.2f}",
             "Probability (%)": f"{(eom_samples > t).mean()*100:.1f}%"} for t in thresholds]
    return pd.DataFrame(rows)

def _render_simple_beta_block(h: pd.DataFrame, k: int = 7, lookback_days: int = 120):
    # Legacy block removed per AAA-only forecasting spec.
    return

# ============== APP ==============
def main():
    st.markdown('<h1 class="main-header">Gas Price Dashboard</h1>', unsafe_allow_html=True)

    latest_data, previous_data, historical_data = load_data()
    if latest_data is None:
        st.error("Failed to load data.")
        return

    # -------- Historical Trends & Analysis (top area stays as-is) --------
    st.header("Historical Trends & Analysis")

    c1, c2, c3 = st.columns(3)

    # GasBuddy MTD
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
            if len(gb_mtd) > 0:
                fig.add_annotation(x=1, y=1, xref="paper", yref="paper",
                                   xanchor="right", yanchor="top", showarrow=False,
                                   text=f"<b>${gb_mtd['price'].iloc[-1]:.3f}</b>")
            fig.update_layout(xaxis_title="Date & Time (EST)", yaxis_title="Price ($/gal)", height=340,
                              margin=dict(l=40,r=20,t=40,b=40), hovermode='x unified',
                              legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No GasBuddy data available")

    # AAA (Last 30 Days)
    with c2:
        st.subheader("AAA Daily Gas Prices (Last 30 Days)")
        aaa = historical_data[historical_data['source']=='aaa_gas_prices'].copy()
        if not aaa.empty:
            aaa['scraped_at'] = to_est(aaa['scraped_at'])
            # AAA is "as-of yesterday": align to as_of_date
            aaa['as_of_date'] = (aaa['scraped_at'] - pd.Timedelta(days=1)).dt.date
            end_date = datetime.now(tz=ZoneInfo("America/New_York")).date()
            start_date = end_date - timedelta(days=30)
            aaa_30 = (aaa[(aaa['as_of_date']>=start_date) & (aaa['as_of_date']<=end_date)]
                        .sort_values('scraped_at'))
            fig_aaa = go.Figure()
            fig_aaa.add_trace(go.Scatter(
                x=aaa_30['as_of_date'], y=aaa_30['price'],
                                         mode='lines+markers', name='AAA',
                hovertemplate='As-of=%{x}<br>AAA=$%{y:.3f}<extra></extra>'
            ))
            if len(aaa_30):
                fig_aaa.add_annotation(x=1, y=1, xref="paper", yref="paper",
                                       xanchor="right", yanchor="top", showarrow=False,
                                       text=f"<b>${aaa_30['price'].iloc[-1]:.3f}</b>")
            fig_aaa.update_layout(xaxis_title="As-of Date (ET)", yaxis_title="Price ($/gal)",
                                  height=340, margin=dict(l=40,r=20,t=40,b=40),
                                  hovermode='x unified',
                                   legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1))
            st.plotly_chart(fig_aaa, use_container_width=True)
        else:
            st.info("No AAA data available")

    # Crack Spread
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
            if len(crack_df) > 0:
                fig_cs.add_annotation(x=1, y=1, xref="paper", yref="paper",
                                      xanchor="right", yanchor="top", showarrow=False,
                                      text=f"<b>${crack_df['Crack_Spread'].iloc[-1]:.2f}</b>")
            fig_cs.update_yaxes(autorange=True)
            fig_cs.update_layout(xaxis_title="Date (EST)", yaxis_title="Crack Spread ($)", height=340,
                                 margin=dict(l=40,r=20,t=40,b=40), hovermode='x unified',
                                 legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1))
            st.plotly_chart(fig_cs, use_container_width=True)
        else:
            st.info("No futures data available")

    # -------- Tables under the charts --------
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

    # -------- Futures last 7 days + quick status table (kept) --------
    st.subheader("Futures (Last 30 Days)")
    f1, f2 = st.columns(2)
    cutoff30 = datetime.now(tz=ZoneInfo("America/New_York")) - timedelta(days=30)

    with f1:
        rb = historical_data[historical_data['source']=='marketwatch_rbob_futures'].copy()
        if not rb.empty:
            rb['scraped_at'] = to_est(rb['scraped_at'])
            rb30 = (rb[rb['scraped_at'] >= cutoff30]
                      .set_index('scraped_at')
                      .resample('D')['price'].mean()
                      .reset_index())
            fig_rb = go.Figure()
            fig_rb.add_trace(go.Scatter(x=rb30['scraped_at'], y=rb30['price'],
                                        mode='lines+markers', name='RBOB (D-avg)',
                                        connectgaps=True))
            fig_rb.update_layout(title="RBOB Futures – Last 30 Days",
                                 xaxis_title="Date (ET)", yaxis_title="Price ($/gal)",
                                 height=320, margin=dict(l=40,r=20,t=40,b=40), hovermode='x unified')
            st.plotly_chart(fig_rb, use_container_width=True)
        else:
            st.info("No RBOB data")

    with f2:
        wt = historical_data[historical_data['source']=='marketwatch_wti_futures'].copy()
        if not wt.empty:
            wt['scraped_at'] = to_est(wt['scraped_at'])
            wt30 = (wt[wt['scraped_at'] >= cutoff30]
                      .set_index('scraped_at')
                      .resample('D')['price'].mean()
                      .reset_index())
            fig_wt = go.Figure()
            fig_wt.add_trace(go.Scatter(x=wt30['scraped_at'], y=wt30['price'],
                                        mode='lines+markers', name='WTI (D-avg)',
                                        connectgaps=True))
            fig_wt.update_layout(title="WTI Futures – Last 30 Days",
                                 xaxis_title="Date (ET)", yaxis_title="Price ($/bbl)",
                                 height=320, margin=dict(l=40,r=20,t=40,b=40), hovermode='x unified')
            st.plotly_chart(fig_wt, use_container_width=True)
        else:
            st.info("No WTI data")

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


    # ============== Gas Price Forecasting & Modeling ==============
    st.markdown("---")
    st.header("Gas Price Forecasting & Modeling")

    # KPI cards (records + current retail + NEW Sunday probability + NEW next Sunday point forecast)
    k1,k2,k3,k4 = st.columns(4)
    with k1:
        st.metric("Records in database", value=f"{len(historical_data):,}")

    with k2:
        gb_last = latest_data[latest_data["source"]=="gasbuddy_fuel_insights"]
        if gb_last.empty:
            gb_last = historical_data[historical_data["source"]=="gasbuddy_fuel_insights"]
        val = float(gb_last.sort_values("scraped_at").iloc[-1]["price"]) if not gb_last.empty else np.nan
        st.metric("Current daily retail (GasBuddy, latest scrape)", value=("—" if pd.isna(val) else f"${val:.3f}"))

    # Fit new AAA nowcast model
    res = fit_aaa_nowcast_and_week(historical_data, k_lag=7, lookback_days=180, half_life_days=90, gate_tol=0.03)
    if "error" in res:
        st.warning(res["error"])
        return

    next_sun = res["next_sunday"].date()
    next_sun_pred = res["next_sunday_point"]

    # Residuals for uncertainty (for probability calc)
    resid = res["resid"]

    # Prev AAA Sunday default (as-of Sunday; scraped Monday)
    default_prev_sun_val, default_prev_sun_date = _prev_aaa_sunday_default(historical_data)

    # --- New two-component visual (input + live probability)
    st.subheader("Sunday-over-Sunday Probability")
    left, right = st.columns([1,2])

    with left:
        init_val = float(default_prev_sun_val) if np.isfinite(default_prev_sun_val) else float(res["base"]["AAA"].iloc[-1])
        prev_sun_val = st.number_input(
            "Previous AAA Sunday value ($/gal)",
            min_value=0.000, max_value=10.000, value=round(init_val,3), step=0.001, format="%.3f",
            help="Default is AAA 'as-of Sunday' (scraped on Monday). You can override."
        )

    with right:
        # simulate probability that next Sunday's AAA (pred) > entered previous Sunday
        sim_n = 10000
        samples = next_sun_pred + np.random.choice(resid, size=sim_n, replace=True)
        prob_pct = float((samples > prev_sun_val).mean()*100.0)
        fig_prob = go.Figure()
        fig_prob.add_trace(go.Indicator(
            mode="number+gauge+delta",
            value=prob_pct,
            number={"suffix":"%"},
            delta={"reference":50, "increasing":{"color":"#2e7d32"}, "decreasing":{"color":"#c62828"}},
            gauge={"axis":{"range":[0,100]}, "bar":{"thickness":0.35}},
            title={"text":f"Prob next Sunday > ${prev_sun_val:.3f}<br><span style='font-size:0.9em;'>Next Sunday forecast: ${next_sun_pred:.3f}</span>"}
        ))
        fig_prob.update_layout(height=200, margin=dict(l=10,r=10,t=20,b=10))
        st.plotly_chart(fig_prob, use_container_width=True)

    # KPI 3: replace with new probability vs prev Sunday
    with k3:
        subtitle = f"vs prev Sunday ${prev_sun_val:.3f}"
        st.metric("Prob ↑ next Sunday (AAA)", value=f"{prob_pct:.1f}%", delta=subtitle)

    # KPI 4: replace with point forecast for next Sunday
    with k4:
        st.metric("Predicted next Sunday (AAA)", value=f"${next_sun_pred:.3f}")

    # === AAA: Last 14 vs Next 7 (Forecast) ===
    st.subheader("AAA: Last 14 vs Next 7 (Forecast)")

    base_sorted = res["base"].sort_values("date")
    hist14 = (base_sorted.dropna(subset=["aaa"])
              .tail(14)[["date","aaa"]]
              .rename(columns={"aaa":"AAA"}))
    hist14["date"] = pd.to_datetime(hist14["date"]).dt.tz_localize(None)

    fdates = pd.to_datetime(res["future_dates"]).tz_localize(None)
    preds  = res["preds"].astype(float)

    fig7 = go.Figure()
    fig7.add_trace(go.Scatter(
        x=hist14["date"], y=hist14["AAA"],
        mode="lines+markers", name="AAA (last 14)",
        hovertemplate="Date=%{x|%Y-%m-%d}<br>AAA=$%{y:.3f}<extra></extra>"
    ))
    fig7.add_trace(go.Scatter(
        x=fdates, y=preds,
        mode="lines+markers+text", name="Forecast (next 7)",
        text=[f"${v:.3f}" for v in preds], textposition="top center",
        hovertemplate="Date=%{x|%Y-%m-%d}<br>Forecast=$%{y:.3f}<extra></extra>"
    ))

    # Line of best fit over last 14 actuals (OLS)
    if len(hist14) >= 3:
        xnum = hist14["date"].map(pd.Timestamp.toordinal).values.astype(float)
        m, b = np.polyfit(xnum, hist14["AAA"].values.astype(float), 1)
        xfit = pd.date_range(hist14["date"].min(), hist14["date"].max(), freq="D")
        yfit = m * xfit.map(pd.Timestamp.toordinal).values.astype(float) + b
        fig7.add_trace(go.Scatter(
            x=xfit, y=yfit, name="Trend (14d OLS)",
            mode="lines", line=dict(dash="dot")
        ))

    # vline at last true AAA
    x_last = hist14["date"].max()
    fig7.add_vline(x=x_last, line_dash="dash", line_color="gray")
    fig7.add_annotation(x=x_last, xref="x", y=1.02, yref="paper",
                        text="Last actual (AAA)", showarrow=False, xanchor="left")

    fig7.update_layout(
        xaxis_title="Date (ET)", yaxis_title="Price ($/gal)",
        height=360, margin=dict(l=40, r=20, t=40, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified"
    )
    st.plotly_chart(fig7, use_container_width=True)

    # === AAA Month-End Thresholds (from AAA model) ===
    st.markdown("##### Month-End Thresholds (AAA model)")
    if isinstance(res.get("eom_samples"), np.ndarray) and res["eom_samples"].size:
        center = float(res.get("eom_point", np.nan))
        prob_df = _make_threshold_table(res["eom_samples"], center)
        st.caption(f"Mean EOM AAA: ${np.mean(res['eom_samples']):.3f} • 90% CI [{np.percentile(res['eom_samples'],5):.3f}, {np.percentile(res['eom_samples'],95):.3f}]")
        st.dataframe(prob_df, use_container_width=True, hide_index=True)
    else:
        st.info("EOM is out of the 7+ day window or insufficient uncertainty samples.")

    # === AAA Daily Move Sigma Table (full history) ===
    st.markdown("##### AAA Daily Move Distribution (σ buckets)")
    aaa_hist = historical_data[historical_data["source"]=="aaa_gas_prices"].copy()
    if aaa_hist.empty:
        st.info("No AAA history available for σ table.")
    else:
        aaa_hist["scraped_at"] = to_est(aaa_hist["scraped_at"]) 
        aaa_hist["as_of_date"] = (aaa_hist["scraped_at"] - pd.Timedelta(days=1)).dt.date
        daily = (aaa_hist.groupby("as_of_date", as_index=False)["price"].mean()
                          .sort_values("as_of_date"))
        daily["diff"] = daily["price"].diff()
        sigma = float(daily["diff"].std(skipna=True))
        last_val = float(daily["price"].iloc[-1]) if len(daily) else np.nan

        rows = []
        for ksig in [3, 2, 1, 0]:
            thresh = (ksig * sigma) if np.isfinite(sigma) else np.nan
            abs_move = abs(thresh) if np.isfinite(thresh) else np.nan
            occ = (int((daily["diff"].abs() >= (ksig*sigma)).sum())
                   if ksig > 0 and np.isfinite(sigma)
                   else int(daily["diff"].dropna().shape[0]))
            rows.append({
                "Std Dev (kσ)": f"{ksig}σ",
                "Abs Move ($)": f"{abs_move:.3f}" if np.isfinite(abs_move) else "—",
                "+ Impact ($)": f"{(last_val + thresh):.3f}" if np.isfinite(thresh) and np.isfinite(last_val) else "—",
                "- Impact ($)": f"{(last_val - thresh):.3f}" if np.isfinite(thresh) and np.isfinite(last_val) else "—",
                "Occurrences ≥ kσ": occ
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    
    # Footer
    st.markdown("---")
    st.markdown("Dashboard refresh ~5 min • Sources: GasBuddy, AAA, MarketWatch, Trading Economics")

if __name__ == "__main__":
    main()