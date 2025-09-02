import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import numpy as np
from zoneinfo import ZoneInfo

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
    for c in ["rbob","wti","stocks_surprise","refinery_runs"]:
        if c in base.columns:
            base[c] = base[c].ffill()

    return base

def _add_pulses(base: pd.DataFrame, decay=[1.0, 0.6, 0.36, 0.216]) -> pd.DataFrame:
    """Build short decaying pulses from weekly EIA series."""
    df = base.sort_values("date").copy().set_index("date")
    s = df.get("stocks_surprise", pd.Series(0.0, index=df.index)).fillna(0.0)
    r = df.get("refinery_runs",  pd.Series(0.0, index=df.index)).fillna(0.0)

    # finite impulse response: sum_{i=0..L-1} w_i * x_{t-i}
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
    last_date = df["date"].max()

    # tails of the underlying weekly inputs
    s = df.get("stocks_surprise", pd.Series(0.0, index=df.index)).fillna(0.0).values
    r = df.get("refinery_runs",  pd.Series(0.0, index=df.index)).fillna(0.0).values
    s_tail = s[-L:] if len(s) >= L else np.r_[np.zeros(L-len(s)), s]
    r_tail = r[-L:] if len(r) >= L else np.r_[np.zeros(L-len(r)), r]

    # weights w0..w_{L-1}
    w = np.array(decay, dtype=float)

    stocks_future = []
    runs_future   = []
    for h in range(1, len(future_dates)+1):
        # pulse_{T+h} = sum_{i=h..L-1} w_i * x_{T-(i-h)}
        idxs = np.arange(h, L)
        if len(idxs) == 0:
            stocks_future.append(0.0)
            runs_future.append(0.0)
            continue
        stocks_future.append( float((w[idxs] * s_tail[-1 - (idxs - h)]).sum()) )
        runs_future.append(   float((w[idxs] * r_tail[-1 - (idxs - h)]).sum()) )

    future_df = pd.DataFrame({
        "date": future_dates,
        "stocks_pulse": np.array(stocks_future),
        "refinery_pulse": np.array(runs_future)
    })
    return future_df

# ============== SIMPLE BETA with OPTIONAL EIA PULSES (k=7) ==============
def _simple_beta_fit_predict_with_eia(h: pd.DataFrame, k: int = 7, lookback_days: int = 120, improve_tol: float = 0.01):
    base = _daily_panel(h)
    if base.empty or base[["rbob","wti","gas_price"]].dropna().empty:
        return {"error":"Not enough data for simple beta."}

    base = base.sort_values("date").copy()
    base["rb42_lag"] = (base["rbob"] * 42.0).shift(k)
    base["wti_lag"]  = base["wti"].shift(k)

    # add pulses
    base_p = _add_pulses(base)

    fit = base_p.dropna(subset=["gas_price","rb42_lag","wti_lag"]).copy()
    if len(fit) < 30:
        return {"error":"Insufficient rows after lagging."}

    cutoff = base_p["date"].max() - pd.Timedelta(days=lookback_days)
    use = fit[fit["date"] >= cutoff]
    if len(use) < 25:
        use = fit

    # split 80/20
    cut = int(len(use)*0.8)
    train = use.iloc[:cut]
    valid = use.iloc[cut:] if len(use) > cut else use.iloc[-1:]

    # baseline fit
    Xb = np.column_stack([np.ones(len(train)), train["rb42_lag"].values, train["wti_lag"].values])
    yb = train["gas_price"].values
    ab,b1b,b2b = np.linalg.lstsq(Xb, yb, rcond=None)[0]
    # baseline val MAE
    Xb_v = np.column_stack([np.ones(len(valid)), valid["rb42_lag"].values, valid["wti_lag"].values])
    yhat_b = Xb_v @ np.array([ab,b1b,b2b])
    mae_b = float(np.mean(np.abs(valid["gas_price"].values - yhat_b)))

    # augmented with pulses
    Xa = np.column_stack([np.ones(len(train)), train["rb42_lag"].values, train["wti_lag"].values,
                          train["stocks_pulse"].values, train["refinery_pulse"].values])
    aa,b1a,b2a,g1a,g2a = np.linalg.lstsq(Xa, yb, rcond=None)[0]
    Xa_v = np.column_stack([np.ones(len(valid)), valid["rb42_lag"].values, valid["wti_lag"].values,
                            valid["stocks_pulse"].values, valid["refinery_pulse"].values])
    yhat_a = Xa_v @ np.array([aa,b1a,b2a,g1a,g2a])
    mae_a = float(np.mean(np.abs(valid["gas_price"].values - yhat_a)))

    use_pulses = (mae_a < mae_b * (1 - improve_tol))

    # choose params
    if use_pulses:
        params = {"alpha":aa, "b1":b1a, "b2":b2a, "g1":g1a, "g2":g2a}
        resid_train = train["gas_price"].values - (Xa @ np.array([aa,b1a,b2a,g1a,g2a]))
    else:
        params = {"alpha":ab, "b1":b1b, "b2":b2b, "g1":0.0, "g2":0.0}
        resid_train = train["gas_price"].values - (Xb @ np.array([ab,b1b,b2b]))

    # Next 7 days predictions -----------------------------------
    start_date   = base_p["date"].max() + pd.Timedelta(days=1)
    future_dates = pd.date_range(start_date, periods=7, freq="D")

    preds = []
    pred_base = []
    stocks_contrib = []
    runs_contrib   = []

    # precompute future pulses
    future_pulses = _forecast_future_pulses(base_p, future_dates)
    base_pulsed = pd.concat([base_p, future_pulses], ignore_index=True)

    for d in future_dates:
        ref = d - pd.Timedelta(days=k)
        row = base_pulsed.loc[base_pulsed["date"] == ref, ["rbob", "wti"]]
        if row.empty:
            # Get the last available values, with fallback to 0 if no data
            rb_data = base_pulsed["rbob"].dropna()
            wt_data = base_pulsed["wti"].dropna()
            rb = float(rb_data.iloc[-1]) if len(rb_data) > 0 else 0.0
            wt = float(wt_data.iloc[-1]) if len(wt_data) > 0 else 0.0
        else:
            rb = float(row["rbob"].iloc[0]); wt = float(row["wti"].iloc[0])

        base_part = params["alpha"] + params["b1"]*(rb*42.0) + params["b2"]*wt
        # pulses at day d
        sp = 0.0
        rp = 0.0
        if "stocks_pulse" in base_pulsed:
            stocks_data = base_pulsed.loc[base_pulsed["date"]==d, "stocks_pulse"].values
            if len(stocks_data) > 0:
                sp = float(stocks_data[0])
        if "refinery_pulse" in base_pulsed:
            runs_data = base_pulsed.loc[base_pulsed["date"]==d, "refinery_pulse"].values
            if len(runs_data) > 0:
                rp = float(runs_data[0])

        pulse_part = params["g1"]*sp + params["g2"]*rp if use_pulses else 0.0
        preds.append(base_part + pulse_part)
        pred_base.append(base_part)
        stocks_contrib.append(params["g1"]*sp)
        runs_contrib.append(params["g2"]*rp)

    preds = np.array(preds)
    pred_base = np.array(pred_base)
    stocks_contrib = np.array(stocks_contrib)
    runs_contrib   = np.array(runs_contrib)

    # EOM threshold table via residual bootstrap -----------------
    resid = resid_train if resid_train.size else np.array([0.0])
    today = base_p["date"].max()
    month_end = pd.Timestamp(year=today.year, month=today.month, day=pd.Period(today, 'M').days_in_month)
    ref_eom = month_end - pd.Timedelta(days=k)
    row_eom = base_pulsed.loc[base_pulsed["date"] == ref_eom, ["rbob","wti"]]
    if row_eom.empty:
        # Get the last available values, with fallback to 0 if no data
        rb_data = base_pulsed["rbob"].dropna()
        wt_data = base_pulsed["wti"].dropna()
        rb_eom = float(rb_data.iloc[-1]) if len(rb_data) > 0 else 0.0
        wt_eom = float(wt_data.iloc[-1]) if len(wt_data) > 0 else 0.0
    else:
        rb_eom = float(row_eom["rbob"].iloc[0]); wt_eom = float(row_eom["wti"].iloc[0])

    base_eom = params["alpha"] + params["b1"]*(rb_eom*42.0) + params["b2"]*wt_eom
    # pulses at month end (decayed)
    sp_eom = 0.0
    rp_eom = 0.0
    if "stocks_pulse" in base_pulsed:
        stocks_eom_data = base_pulsed.loc[base_pulsed["date"]==month_end, "stocks_pulse"].values
        if len(stocks_eom_data) > 0:
            sp_eom = float(stocks_eom_data[0])
    if "refinery_pulse" in base_pulsed:
        runs_eom_data = base_pulsed.loc[base_pulsed["date"]==month_end, "refinery_pulse"].values
        if len(runs_eom_data) > 0:
            rp_eom = float(runs_eom_data[0])
    eom_point = base_eom + (params["g1"]*sp_eom + params["g2"]*rp_eom if use_pulses else 0.0)
    sim_n = 5000
    eom_samples = eom_point + np.random.choice(resid, size=sim_n, replace=True)

    # latest event dates/values for display ----------------------
    # Use original historical h with event_date
    df = h.copy()
    df["timestamp"]  = pd.to_datetime(df.get("timestamp"), errors="coerce", utc=True)
    df["event_date"] = df["timestamp"].dt.tz_convert("America/New_York").dt.normalize()

    stocks_rows = df[df["source"]=="tradingeconomics_gasoline_stocks"].dropna(subset=["event_date"])
    runs_rows   = df[df["source"]=="tradingeconomics_refinery_runs"].dropna(subset=["event_date"])
    last_stock_date = pd.to_datetime(stocks_rows["event_date"].max()) if not stocks_rows.empty else pd.NaT
    last_runs_date  = pd.to_datetime(runs_rows["event_date"].max())   if not runs_rows.empty else pd.NaT
    last_stock_surprise = float(stocks_rows.loc[stocks_rows["event_date"]==last_stock_date, "surprise"].mean()) if pd.notna(last_stock_date) else np.nan
    last_runs_value     = float(runs_rows.loc[runs_rows["event_date"]==last_runs_date, "price"].mean()) if pd.notna(last_runs_date) else np.nan

    return {
        "base_df": base_p,
        "future_dates": future_dates,
        "preds": preds,
        "preds_base": pred_base,
        "params": params,
        "use_pulses": bool(use_pulses),
        "mae_base": mae_b,
        "mae_aug": mae_a,
        "stocks_contrib": stocks_contrib,
        "runs_contrib": runs_contrib,
        "eom_samples": eom_samples,
        "last_stock_date": last_stock_date,
        "last_runs_date": last_runs_date,
        "last_stock_surprise": last_stock_surprise,
        "last_runs_value": last_runs_value
    }

def _prev_aaa_sunday_default(h: pd.DataFrame) -> tuple[float, datetime]:
    aaa = h[h["source"]=="aaa_gas_prices"].copy()
    if aaa.empty:
        return np.nan, None
    aaa["scraped_at"] = pd.to_datetime(aaa["scraped_at"], errors="coerce", utc=True).dt.tz_convert("America/New_York")
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
    res = _simple_beta_fit_predict_with_eia(h, k=k, lookback_days=lookback_days)
    if "error" in res:
        st.info(res["error"])
        return

    # ---------- INFO STRIP (requested) ----------
    lsd = res["last_stock_date"]
    lrd = res["last_runs_date"]
    lsd_txt = "—" if pd.isna(lsd) else pd.Timestamp(lsd).date().isoformat()
    lrd_txt = "—" if pd.isna(lrd)  else pd.Timestamp(lrd).date().isoformat()

    # impacts (in cents)
    s_imp_tom = 100*res["stocks_contrib"][0] if len(res["stocks_contrib"]) else 0.0
    r_imp_tom = 100*res["runs_contrib"][0]   if len(res["runs_contrib"])   else 0.0
    s_imp_avg = 100*float(np.mean(res["stocks_contrib"])) if len(res["stocks_contrib"]) else 0.0
    r_imp_avg = 100*float(np.mean(res["runs_contrib"]))   if len(res["runs_contrib"])   else 0.0

    used = res["use_pulses"]
    use_txt = "using EIA pulses (stocks surprise & refinery runs value)" if used else "not used (no validation gain)"

    # Create the info strip HTML
    info_html = f"""
<div class="info-strip">
  <div>
    <div class="label">EIA Gasoline Stocks</div>
    <div>Date of last release: <b>{lsd_txt}</b></div>
    <div class="light">Feature used: <b>surprise</b> (actual − consensus)</div>
    <div class="muted">Impact on forecast: {('+' if s_imp_tom>=0 else '')}{s_imp_tom:.1f}¢ tomorrow; {('+' if s_imp_avg>=0 else '')}{s_imp_avg:.1f}¢ avg next 7 days</div>
  </div>
  <div>
    <div class="label">Refinery Runs</div>
    <div>Date of last release: <b>{lrd_txt}</b></div>
    <div class="light">Feature used: <b>value</b> (level)</div>
    <div class="muted">Impact on forecast: {('+' if r_imp_tom>=0 else '')}{r_imp_tom:.1f}¢ tomorrow; {('+' if r_imp_avg>=0 else '')}{r_imp_avg:.1f}¢ avg next 7 days</div>
  </div>
</div>
<p class="muted">Model is {use_txt}.  Stocks use the weekly <i>surprise</i>; runs use the weekly <i>value</i>. Weekly effects decay quickly.</p>
"""
    st.markdown(info_html, unsafe_allow_html=True)

    # ---------- TITLE ----------
    st.markdown("### Simple Beta Model (RBOB & WTI as drivers, optional EIA pulses)")

    # ---------- CHART ----------
    last7 = res["base_df"].tail(7)[["date","gas_price"]]
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=last7["date"], y=last7["gas_price"],
        mode="lines+markers", name="Actual (AAA→GasBuddy fallback)",
        hovertemplate="Date=%{x|%Y-%m-%d}<br>Retail=$%{y:.3f}<extra></extra>"
    ))
    fig.add_trace(go.Scatter(
        x=res["future_dates"], y=res["preds"], mode="lines+markers+text",
        name="Forecast (k=7)", text=[f"<b>${v:.3f}</b>" for v in res["preds"]],
        textposition="top center",
        hovertemplate="Date=%{x|%Y-%m-%d}<br>Forecast=$%{y:.3f}<extra></extra>"
    ))
    fig.add_vline(x=last7["date"].max(), line_dash="dot", line_color="gray", opacity=0.5)
    fig.update_layout(
        xaxis_title="Date (ET)", yaxis_title="Retail price ($/gal)",
        height=380, margin=dict(l=40, r=20, t=40, b=40),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig, use_container_width=True)

    # ---------- 7-row forecast table ----------
    pred_df = pd.DataFrame({
        "Date": [d.date() for d in res["future_dates"]],
        "Predicted Value": [f"${v:.3f}" for v in res["preds"]],
        "EIA Stocks Impact (¢)": [f"{100*s:.1f}" for s in res["stocks_contrib"]],
        "Refinery Runs Impact (¢)": [f"{100*r:.1f}" for r in res["runs_contrib"]],
    })
    st.dataframe(pred_df, use_container_width=True, hide_index=True)

    # ---------- Month-end thresholds ----------
    prob_df = _make_threshold_table(res["eom_samples"], float(np.mean(res["eom_samples"])))
    st.markdown("##### Month-End Thresholds (from residual bootstrap)")
    st.caption(f"Mean EOM daily price: ${np.mean(res['eom_samples']):.3f} • 90% CI [{np.percentile(res['eom_samples'],5):.3f}, {np.percentile(res['eom_samples'],95):.3f}]")
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

    # AAA MTD
    with c2:
        st.subheader("AAA Daily Gas Prices")
        aaa = historical_data[historical_data['source']=='aaa_gas_prices'].copy()
        if not aaa.empty:
            aaa['scraped_at'] = to_est(aaa['scraped_at'])
            today_dt = datetime.now()
            month_start = today_dt.replace(day=1).date()
            aaa['date'] = (aaa['scraped_at'] - timedelta(days=1)).dt.date
            aaa_mtd = aaa[(aaa['date']>=month_start) & (aaa['date']<=today_dt.date())].sort_values('scraped_at')
            fig_aaa = go.Figure()
            fig_aaa.add_trace(go.Scatter(x=aaa_mtd['scraped_at'], y=aaa_mtd['price'],
                                         mode='lines+markers', name='AAA',
                                         hovertemplate='Date (EST)=%{x}<br>AAA=$%{y:.3f}<extra></extra>'))
            if len(aaa_mtd) > 0:
                fig_aaa.add_annotation(x=1, y=1, xref="paper", yref="paper",
                                       xanchor="right", yanchor="top", showarrow=False,
                                       text=f"<b>${aaa_mtd['price'].iloc[-1]:.3f}</b>")
            fig_aaa.update_layout(xaxis_title="Date (EST)", yaxis_title="Price ($/gal)", height=340,
                                   margin=dict(l=40,r=20,t=40,b=40), hovermode='x unified',
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

    # Fit simple beta with EIA and compute next Sunday forecast
    res = _simple_beta_fit_predict_with_eia(historical_data, k=7, lookback_days=120)
    if "error" in res:
        st.warning("Not enough data to fit the simple model yet.")
        return

    # Find next Sunday and its point forecast
    next_sun = _next_sunday(res["base_df"]["date"].max())
    pred_map = {d: p for d, p in zip(res["future_dates"], res["preds"])}
    if next_sun in pred_map:
        next_sun_pred = float(pred_map[next_sun])
    else:
        # fallback compute using last known RB/WTI at lag 7
        params = res["params"]
        ref = next_sun - pd.Timedelta(days=7)
        row = res["base_df"].loc[res["base_df"]["date"]==ref, ["rbob","wti"]]
        if row.empty:
            # Get the last available values, with fallback to 0 if no data
            rb_data = res["base_df"]["rbob"].dropna()
            wt_data = res["base_df"]["wti"].dropna()
            rb = float(rb_data.iloc[-1]) if len(rb_data) > 0 else 0.0
            wt = float(wt_data.iloc[-1]) if len(wt_data) > 0 else 0.0
        else:
            rb = float(row["rbob"].iloc[0]); wt = float(row["wti"].iloc[0])
        next_sun_pred = float(params["alpha"] + params["b1"]*(rb*42.0) + params["b2"]*wt)

    # Residuals for uncertainty (for probability calc)
    resid = res["eom_samples"] - np.mean(res["eom_samples"])  # Use the residuals from the model

    # Prev AAA Sunday default (as-of Sunday; scraped Monday)
    default_prev_sun_val, default_prev_sun_date = _prev_aaa_sunday_default(historical_data)

    # --- New two-component visual (input + live probability)
    st.subheader("Sunday-over-Sunday Probability")
    left, right = st.columns([1,2])

    with left:
        init_val = float(default_prev_sun_val) if np.isfinite(default_prev_sun_val) else float(res["base_df"]["gas_price"].iloc[-1])
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

    # (Fan chart and all advanced-model code removed)
    
    # Footer
    st.markdown("---")
    st.markdown("Dashboard refresh ~5 min • Sources: GasBuddy, AAA, MarketWatch, Trading Economics")

if __name__ == "__main__":
    main()