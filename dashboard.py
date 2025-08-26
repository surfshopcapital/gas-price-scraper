import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, date
import os

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
    # 📈 NEW CHARTS (Bottom Area)
    # ===========================
    st.markdown("---")
    st.header("📊 Historical Trends & Analysis")

    # Helper: safe datetime conversion with timezone handling
    def _to_datetime(series):
        """Convert to datetime with proper timezone handling"""
        out = pd.to_datetime(series, errors='coerce', utc=False)
        return out

    # ---------- 1) Month-to-date line graph: GasBuddy avg per day + AAA daily dots ----------
    st.subheader("1. Month-to-Date Gas Prices: GasBuddy vs AAA")
    try:
        gb = historical_data[historical_data['source'] == 'gasbuddy_fuel_insights'].copy()
        aaa = historical_data[historical_data['source'] == 'aaa_gas_prices'].copy()

        if not gb.empty and not aaa.empty:
            gb['scraped_at'] = _to_datetime(gb['scraped_at'])
            aaa['scraped_at'] = _to_datetime(aaa['scraped_at'])

            # Current month filter (based on local system date)
            today_dt = datetime.now()
            month_start = today_dt.replace(day=1).date()
            gb['date'] = gb['scraped_at'].dt.date
            aaa['date'] = aaa['scraped_at'].dt.date

            gb_mtd = gb[(gb['date'] >= month_start) & (gb['date'] <= today_dt.date())].copy()
            aaa_mtd = aaa[(aaa['date'] >= month_start) & (aaa['date'] <= today_dt.date())].copy()

            if not gb_mtd.empty:
                # Average GasBuddy by day for smoother MTD trend
                gb_daily = (gb_mtd
                            .groupby('date', as_index=False)['price']
                            .mean()
                            .rename(columns={'price': 'gb_avg_price'}))

                # AAA: one point per day (last observation of the day if multiple)
                aaa_daily = (aaa_mtd
                             .sort_values('scraped_at')
                             .groupby('date', as_index=False)
                             .tail(1)[['date', 'price']])

                fig_mtd = go.Figure()
                
                # GasBuddy line with smaller markers
                fig_mtd.add_trace(go.Scatter(
                    x=gb_daily['date'], y=gb_daily['gb_avg_price'],
                    mode='lines+markers',
                    name='GasBuddy (Daily Average)',
                    line=dict(color='#1f77b4', width=2),
                    marker=dict(size=6, color='#1f77b4'),
                    hovertemplate='Date=%{x}<br>Avg Price=$%{y:.3f}<extra></extra>'
                ))
                
                # AAA dots (larger, different color)
                if not aaa_daily.empty:
                    fig_mtd.add_trace(go.Scatter(
                        x=aaa_daily['date'], y=aaa_daily['price'],
                        mode='markers',
                        marker=dict(size=12, symbol='circle-open', color='#ff7f0e', line=dict(width=2)),
                        name='AAA (Daily)',
                        hovertemplate='Date=%{x}<br>AAA Price=$%{y:.3f}<extra></extra>'
                    ))

                fig_mtd.update_layout(
                    title="Month-to-Date: GasBuddy Daily Average vs. AAA Daily Prices",
                    xaxis_title="Date",
                    yaxis_title="Price ($/gal)",
                    height=450,
                    margin=dict(l=40, r=20, t=60, b=40),
                    hovermode='x unified',
                    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
                )
                
                st.plotly_chart(fig_mtd, use_container_width=True)
                
                # Add explanatory text
                st.info("🔄 **GasBuddy**: Live ticking average (updated every 10 min) shown as daily averages. **AAA**: Daily national average (updated once daily at 3:30 AM EST) shown as individual data points.")
            else:
                st.warning("No GasBuddy data available for current month")
        else:
            st.warning("Missing data sources for MTD comparison")
            
    except Exception as e:
        st.error(f"Error rendering MTD GasBuddy vs AAA chart: {e}")
        st.exception(e)

    # ---------- 2) Last month RBOB & WTI futures with dual y-axes ----------
    st.subheader("2. RBOB & WTI Futures (Last 30 Days)")
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
                               .rename(columns={'price': 'RBOB'}))
            wti_daily = (wti.set_index('scraped_at')
                              .resample('D')['price']
                              .mean()
                              .reset_index()
                              .rename(columns={'price': 'WTI'}))

            # Merge on date for shared x-axis
            rbob_daily['date'] = rbob_daily['scraped_at'].dt.date
            wti_daily['date'] = wti_daily['scraped_at'].dt.date
            futures_df = pd.merge(rbob_daily[['date','RBOB']], wti_daily[['date','WTI']], on='date', how='outer').sort_values('date')

            if not futures_df.empty:
                fig_dual = make_subplots(specs=[[{"secondary_y": True}]])
                
                # RBOB on left y-axis (primary)
                fig_dual.add_trace(
                    go.Scatter(
                        x=futures_df['date'], y=futures_df['RBOB'], 
                        name="RBOB ($/gal)", 
                        mode='lines+markers',
                        line=dict(color='#2ca02c', width=2),
                        marker=dict(size=6, color='#2ca02c')
                    ),
                    secondary_y=False
                )
                
                # WTI on right y-axis (secondary)
                fig_dual.add_trace(
                    go.Scatter(
                        x=futures_df['date'], y=futures_df['WTI'], 
                        name="WTI ($/bbl)", 
                        mode='lines+markers',
                        line=dict(color='#d62728', width=2),
                        marker=dict(size=6, color='#d62728')
                    ),
                    secondary_y=True
                )
                
                fig_dual.update_layout(
                    title="Futures Prices: RBOB vs. WTI (Last 30 Days)",
                    height=450,
                    margin=dict(l=40, r=40, t=60, b=40),
                    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
                    hovermode='x unified'
                )
                
                fig_dual.update_xaxes(title_text="Date")
                fig_dual.update_yaxes(title_text="RBOB ($/gal)", secondary_y=False, color='#2ca02c')
                fig_dual.update_yaxes(title_text="WTI ($/bbl)", secondary_y=True, color='#d62728')
                
                st.plotly_chart(fig_dual, use_container_width=True)
                
                # Add explanatory text
                st.info("⛽ **RBOB**: Gasoline futures in $/gallon. **WTI**: Crude oil futures in $/barrel. Both updated hourly during market hours (Sun 6pm EST - Fri 8pm EST).")
            else:
                st.warning("No futures data available for charting")
        else:
            st.warning("Missing futures data sources")
            
    except Exception as e:
        st.error(f"Error rendering RBOB/WTI dual-axis chart: {e}")
        st.exception(e)

    # ---------- 3) Crack Spread chart (last month) ----------
    st.subheader("3. Crack Spread Analysis (Last 30 Days)")
    try:
        # Use futures_df from previous step if available, otherwise compute again
        if 'futures_df' not in locals():
            rbob = historical_data[historical_data['source'] == 'marketwatch_rbob_futures'].copy()
            wti = historical_data[historical_data['source'] == 'marketwatch_wti_futures'].copy()
            
            if not rbob.empty and not wti.empty:
                rbob['scraped_at'] = _to_datetime(rbob['scraped_at'])
                wti['scraped_at'] = _to_datetime(wti['scraped_at'])
                
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
                
                rbob_daily['date'] = rbob_daily['scraped_at'].dt.date
                wti_daily['date'] = wti_daily['scraped_at'].dt.date
                futures_df = pd.merge(rbob_daily[['date','RBOB']], wti_daily[['date','WTI']], on='date', how='outer').sort_values('date')

        if 'futures_df' in locals() and not futures_df.empty:
            crack_df = futures_df.copy()
            crack_df['WTI_gal'] = crack_df['WTI'] / 42.0  # Convert barrel to gallon
            crack_df['Crack_Spread'] = crack_df['RBOB'] - crack_df['WTI_gal']
            
            # Remove any infinite or NaN values
            crack_df = crack_df.dropna()

            if not crack_df.empty:
                fig_crack = go.Figure()
                
                # Main crack spread line
                fig_crack.add_trace(go.Scatter(
                    x=crack_df['date'], y=crack_df['Crack_Spread'],
                    mode='lines+markers',
                    name='Crack Spread ($/gal)',
                    line=dict(color='#9467bd', width=3),
                    marker=dict(size=8, color='#9467bd'),
                    hovertemplate='Date=%{x}<br>Crack Spread=$%{y:.3f}/gal<extra></extra>'
                ))
                
                # Add horizontal line at zero for reference
                fig_crack.add_hline(y=0, line_dash="dash", line_color="gray", 
                                  annotation_text="Break-even", annotation_position="bottom right")
                
                fig_crack.update_layout(
                    title="Crack Spread: RBOB - (WTI/42) — Last 30 Days",
                    xaxis_title="Date",
                    yaxis_title="Crack Spread ($/gallon)",
                    height=450,
                    margin=dict(l=40, r=20, t=60, b=40),
                    hovermode='x unified',
                    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
                )
                
                st.plotly_chart(fig_crack, use_container_width=True)
                
                # Add explanatory text and statistics
                col1, col2 = st.columns(2)
                with col1:
                    avg_crack = crack_df['Crack_Spread'].mean()
                    st.metric("Average Crack Spread", f"${avg_crack:.3f}/gal")
                
                with col2:
                    current_crack = crack_df['Crack_Spread'].iloc[-1] if len(crack_df) > 0 else 0
                    st.metric("Current Crack Spread", f"${current_crack:.3f}/gal")
                
                st.info("🏭 **Crack Spread** = RBOB gasoline price - (WTI crude price ÷ 42). This represents the theoretical refining margin. Positive values indicate profitable refining conditions.")
            else:
                st.warning("No valid crack spread data available")
        else:
            st.warning("Missing futures data for crack spread calculation")
            
    except Exception as e:
        st.error(f"Error rendering Crack Spread chart: {e}")
        st.exception(e)

    # Footer
    st.markdown("---")
    st.markdown("Dashboard updated every 5 minutes | Data from multiple sources including GasBuddy, AAA, MarketWatch, and Trading Economics")

if __name__ == "__main__":
    main()
